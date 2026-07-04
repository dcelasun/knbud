package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dcelasun/knbud/internal/model"
	"github.com/dcelasun/knbud/internal/planner"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type Executor struct {
	Client      kubernetes.Interface
	Dynamic     dynamic.Interface
	Parallelism int
	Timeout     time.Duration
	Poll        time.Duration
	Output      io.Writer
	Now         func() time.Time
}

func (e *Executor) Run(ctx context.Context, plan *planner.Plan, operationID string) error {
	if e.Parallelism < 1 {
		return fmt.Errorf("parallelism must be at least 1")
	}
	for _, phase := range plan.Before {
		if err := e.runGitOpsPhase(ctx, planner.Down, phase, operationID); err != nil {
			return fmt.Errorf("GitOps phase %s failed: %w", phase.Name, err)
		}
	}
	for index, wave := range plan.Waves {
		fmt.Fprintf(e.Output, "wave %d/%d\n", index+1, len(plan.Waves))
		if err := e.runWave(ctx, plan.Direction, wave, operationID); err != nil {
			return fmt.Errorf("wave %d failed: %w", index+1, err)
		}
	}
	for _, phase := range plan.After {
		if err := e.runGitOpsPhase(ctx, planner.Up, phase, operationID); err != nil {
			return fmt.Errorf("GitOps phase %s failed: %w", phase.Name, err)
		}
	}
	return nil
}

func (e *Executor) runGitOpsPhase(ctx context.Context, direction planner.Direction, phase planner.GitOpsPhase, operationID string) error {
	fmt.Fprintf(e.Output, "GitOps %s\n", phase.Name)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, e.Parallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for _, action := range phase.Actions {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			if err := e.runGitOpsAction(ctx, direction, action, operationID); err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("%s: %w", action.Resource.Ref.ID(), err))
				mu.Unlock()
				cancel()
			}
		})
	}
	wg.Wait()
	return errors.Join(failures...)
}

func (e *Executor) runGitOpsAction(ctx context.Context, direction planner.Direction, action planner.GitOpsAction, operationID string) error {
	ctx, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()
	if e.Dynamic == nil {
		return fmt.Errorf("dynamic Kubernetes client is not configured")
	}
	if direction == planner.Down {
		if err := e.saveGitOpsState(ctx, action.Resource, operationID); err != nil {
			return err
		}
	}
	if err := e.setGitOpsSuspended(ctx, action); err != nil {
		return err
	}
	if direction == planner.Up {
		if err := e.clearGitOpsState(ctx, action.Resource.Ref); err != nil {
			return err
		}
	}
	fmt.Fprintf(e.Output, "  %s complete\n", action.Resource.Ref.ID())
	return nil
}

func (e *Executor) saveGitOpsState(ctx context.Context, resource *model.GitOpsResource, operationID string) error {
	current, err := e.getGitOpsResource(ctx, resource.Ref)
	if err != nil {
		return err
	}
	if current.Annotations[model.GitOpsStateAnnotation] != "" {
		return nil
	}
	state := planner.NewGitOpsState(current, operationID, e.Now())
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode GitOps state: %w", err)
	}
	return e.patchGitOps(ctx, resource.Ref, map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{model.GitOpsStateAnnotation: string(raw)}},
	})
}

func (e *Executor) getGitOpsResource(ctx context.Context, ref model.GitOpsRef) (*model.GitOpsResource, error) {
	item, err := e.Dynamic.Resource(gitOpsGVR(ref)).Namespace(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get GitOps resource: %w", err)
	}
	suspended := false
	if ref.Provider == model.ProviderFlux {
		suspended, _, _ = unstructured.NestedBool(item.Object, "spec", "suspend")
	} else {
		suspended = item.GetAnnotations()["argocd.argoproj.io/skip-reconcile"] == "true"
	}
	return &model.GitOpsResource{Ref: ref, Annotations: item.GetAnnotations(), Suspended: suspended}, nil
}

func (e *Executor) setGitOpsSuspended(ctx context.Context, action planner.GitOpsAction) error {
	ref := action.Resource.Ref
	var patch map[string]any
	if ref.Provider == model.ProviderFlux {
		patch = map[string]any{"spec": map[string]any{"suspend": action.TargetSuspended}}
	} else {
		var value any = "true"
		if !action.TargetSuspended {
			value = nil
			if action.State != nil && action.State.OriginalSkipReconcile != nil {
				value = *action.State.OriginalSkipReconcile
			}
		}
		patch = map[string]any{"metadata": map[string]any{"annotations": map[string]any{"argocd.argoproj.io/skip-reconcile": value}}}
	}
	if err := e.patchGitOps(ctx, ref, patch); err != nil {
		return fmt.Errorf("set suspended=%t: %w", action.TargetSuspended, err)
	}
	return nil
}

func (e *Executor) clearGitOpsState(ctx context.Context, ref model.GitOpsRef) error {
	return e.patchGitOps(ctx, ref, map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{model.GitOpsStateAnnotation: nil}},
	})
}

func (e *Executor) patchGitOps(ctx context.Context, ref model.GitOpsRef, value map[string]any) error {
	patch, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode GitOps patch: %w", err)
	}
	if _, err := e.Dynamic.Resource(gitOpsGVR(ref)).Namespace(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patch GitOps resource: %w", err)
	}
	return nil
}

func gitOpsGVR(ref model.GitOpsRef) schema.GroupVersionResource {
	groupVersion, _ := schema.ParseGroupVersion(ref.APIVersion)
	resource := "applications"
	if ref.Kind == model.KindKustomization {
		resource = "kustomizations"
	} else if ref.Kind == model.KindHelmRelease {
		resource = "helmreleases"
	}
	return groupVersion.WithResource(resource)
}

func (e *Executor) runWave(ctx context.Context, direction planner.Direction, actions []planner.Action, operationID string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, e.Parallelism)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for _, action := range actions {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			if err := e.runAction(ctx, direction, action, operationID); err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("%s: %w", action.Workload.Ref.ID(), err))
				mu.Unlock()
				cancel()
			}
		})
	}
	wg.Wait()
	return errors.Join(failures...)
}

func (e *Executor) runAction(ctx context.Context, direction planner.Direction, action planner.Action, operationID string) error {
	ctx, cancel := context.WithTimeout(ctx, e.Timeout)
	defer cancel()
	if direction == planner.Down {
		if err := e.saveState(ctx, action.Workload, operationID); err != nil {
			return err
		}
	}
	if action.Workload.Ref.Kind == model.KindCronJob {
		if err := e.setCronJob(ctx, action.Workload.Ref, *action.TargetSuspended); err != nil {
			return err
		}
		if direction == planner.Down {
			if err := e.waitForCronJobs(ctx, action.Workload); err != nil {
				return err
			}
		}
	} else {
		if err := e.setReplicas(ctx, action.Workload.Ref, *action.TargetReplicas); err != nil {
			return err
		}
		if err := e.waitForReplicas(ctx, action.Workload.Ref, *action.TargetReplicas); err != nil {
			return err
		}
	}
	if direction == planner.Up {
		if err := e.clearState(ctx, action.Workload.Ref); err != nil {
			return err
		}
	}
	fmt.Fprintf(e.Output, "  %s complete\n", action.Workload.Ref.ID())
	return nil
}

func (e *Executor) saveState(ctx context.Context, workload *model.Workload, operationID string) error {
	current, err := e.currentWorkload(ctx, workload.Ref)
	if err != nil {
		return err
	}
	if current.Annotations[model.StateAnnotation] != "" {
		return nil
	}
	state := planner.NewState(current, operationID, e.Now())
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{model.StateAnnotation: string(raw)}}})
	if err != nil {
		return fmt.Errorf("encode annotation patch: %w", err)
	}
	return e.patch(ctx, workload.Ref, patch)
}

func (e *Executor) currentWorkload(ctx context.Context, ref model.Ref) (*model.Workload, error) {
	switch ref.Kind {
	case model.KindDeployment:
		item, err := e.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get current deployment: %w", err)
		}
		replicas := lo.FromPtrOr(item.Spec.Replicas, int32(1))
		return &model.Workload{Ref: ref, Replicas: replicas, Annotations: item.Annotations}, nil
	case model.KindStatefulSet:
		item, err := e.Client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get current statefulset: %w", err)
		}
		replicas := lo.FromPtrOr(item.Spec.Replicas, int32(1))
		return &model.Workload{Ref: ref, Replicas: replicas, Annotations: item.Annotations}, nil
	case model.KindCronJob:
		item, err := e.Client.BatchV1().CronJobs(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get current cronjob: %w", err)
		}
		suspended := lo.FromPtr(item.Spec.Suspend)
		return &model.Workload{Ref: ref, Suspended: suspended, Annotations: item.Annotations}, nil
	default:
		return nil, fmt.Errorf("unsupported kind %s", ref.Kind)
	}
}

func (e *Executor) clearState(ctx context.Context, ref model.Ref) error {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]any{model.StateAnnotation: nil}}})
	if err != nil {
		return fmt.Errorf("encode annotation patch: %w", err)
	}
	return e.patch(ctx, ref, patch)
}

func (e *Executor) patch(ctx context.Context, ref model.Ref, patch []byte) error {
	var err error
	switch ref.Kind {
	case model.KindDeployment:
		_, err = e.Client.AppsV1().Deployments(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	case model.KindStatefulSet:
		_, err = e.Client.AppsV1().StatefulSets(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	case model.KindCronJob:
		_, err = e.Client.BatchV1().CronJobs(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	default:
		return fmt.Errorf("unsupported kind %s", ref.Kind)
	}
	if err != nil {
		return fmt.Errorf("patch state: %w", err)
	}
	return nil
}

func (e *Executor) setReplicas(ctx context.Context, ref model.Ref, replicas int32) error {
	patch := fmt.Appendf(nil, `{"spec":{"replicas":%d}}`, replicas)
	var err error
	switch ref.Kind {
	case model.KindDeployment:
		_, err = e.Client.AppsV1().Deployments(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{}, "scale")
	case model.KindStatefulSet:
		_, err = e.Client.AppsV1().StatefulSets(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{}, "scale")
	default:
		return fmt.Errorf("cannot scale kind %s", ref.Kind)
	}
	if err != nil {
		return fmt.Errorf("scale to %d: %w", replicas, err)
	}
	return nil
}

func (e *Executor) setCronJob(ctx context.Context, ref model.Ref, suspended bool) error {
	patch := fmt.Appendf(nil, `{"spec":{"suspend":%t}}`, suspended)
	if _, err := e.Client.BatchV1().CronJobs(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("set suspended=%t: %w", suspended, err)
	}
	return nil
}

func (e *Executor) waitForReplicas(ctx context.Context, ref model.Ref, target int32) error {
	ticker := time.NewTicker(e.Poll)
	defer ticker.Stop()
	for {
		ready, err := e.replicasReady(ctx, ref, target)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for replicas=%d: %w", target, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (e *Executor) replicasReady(ctx context.Context, ref model.Ref, target int32) (bool, error) {
	switch ref.Kind {
	case model.KindDeployment:
		item, err := e.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get deployment: %w", err)
		}
		if target == 0 {
			return item.Status.Replicas == 0, nil
		}
		return item.Status.ObservedGeneration >= item.Generation && item.Status.ReadyReplicas >= target && item.Status.UpdatedReplicas >= target && item.Status.AvailableReplicas >= target, nil
	case model.KindStatefulSet:
		item, err := e.Client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get statefulset: %w", err)
		}
		if target == 0 {
			return item.Status.Replicas == 0, nil
		}
		return item.Status.ObservedGeneration >= item.Generation && item.Status.ReadyReplicas >= target && item.Status.UpdatedReplicas >= target, nil
	default:
		return false, fmt.Errorf("unsupported scalable kind %s", ref.Kind)
	}
}

func (e *Executor) waitForCronJobs(ctx context.Context, workload *model.Workload) error {
	ticker := time.NewTicker(e.Poll)
	defer ticker.Stop()
	for {
		jobs, err := e.Client.BatchV1().Jobs(workload.Ref.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("list active jobs: %w", err)
		}
		active := false
		for _, job := range jobs.Items {
			if job.Status.Active == 0 {
				continue
			}
			for _, owner := range job.OwnerReferences {
				if owner.Kind == model.KindCronJob && string(owner.UID) == workload.UID {
					active = true
				}
			}
		}
		if !active {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for active jobs: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
