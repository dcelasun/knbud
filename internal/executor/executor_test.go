package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dcelasun/knbud/internal/model"
	"github.com/dcelasun/knbud/internal/planner"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDownSavesState(t *testing.T) {
	replicas := int32(2)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "app"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{Replicas: 0},
	}
	client := fake.NewClientset(deployment)
	workload := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "test", Name: "app"}, Replicas: 2, Annotations: map[string]string{}}
	zero := int32(0)
	plan := &planner.Plan{Direction: planner.Down, Waves: [][]planner.Action{{{Workload: workload, TargetReplicas: &zero}}}}
	runner := newTestExecutor(client)
	if err := runner.Run(context.Background(), plan, "operation"); err != nil {
		t.Fatal(err)
	}
	updated, err := client.AppsV1().Deployments("test").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var state planner.State
	if err := json.Unmarshal([]byte(updated.Annotations[model.StateAnnotation]), &state); err != nil {
		t.Fatal(err)
	}
	if state.OriginalReplicas == nil || *state.OriginalReplicas != 2 {
		t.Fatalf("unexpected state: %#v", state)
	}
}

func TestUpClearsStateAfterReady(t *testing.T) {
	replicas := int32(0)
	original := int32(2)
	stateRaw, _ := json.Marshal(planner.State{OperationID: "operation", OriginalReplicas: &original})
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "app", Annotations: map[string]string{model.StateAnnotation: string(stateRaw)}, Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 2, ReadyReplicas: 2, UpdatedReplicas: 2, AvailableReplicas: 2},
	}
	client := fake.NewClientset(deployment)
	workload := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "test", Name: "app"}, Annotations: deployment.Annotations}
	plan := &planner.Plan{Direction: planner.Up, Waves: [][]planner.Action{{{Workload: workload, TargetReplicas: &original}}}}
	runner := newTestExecutor(client)
	if err := runner.Run(context.Background(), plan, "operation"); err != nil {
		t.Fatal(err)
	}
	updated, err := client.AppsV1().Deployments("test").Get(context.Background(), "app", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Annotations[model.StateAnnotation] != "" {
		t.Fatalf("state annotation was not removed: %#v", updated.Annotations)
	}
}

func TestDownSuspendsCronJobAndSavesState(t *testing.T) {
	suspended := false
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "job", UID: types.UID("job-uid")},
		Spec:       batchv1.CronJobSpec{Suspend: &suspended},
	}
	client := fake.NewClientset(cronJob)
	workload := &model.Workload{
		Ref:         model.Ref{Kind: model.KindCronJob, Namespace: "test", Name: "job"},
		Annotations: map[string]string{}, UID: "job-uid",
	}
	target := true
	plan := &planner.Plan{Direction: planner.Down, Waves: [][]planner.Action{{{Workload: workload, TargetSuspended: &target}}}}
	runner := newTestExecutor(client)
	if err := runner.Run(context.Background(), plan, "operation"); err != nil {
		t.Fatal(err)
	}
	updated, err := client.BatchV1().CronJobs("test").Get(context.Background(), "job", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Spec.Suspend == nil || !*updated.Spec.Suspend {
		t.Fatal("cronjob was not suspended")
	}
	var state planner.State
	if err := json.Unmarshal([]byte(updated.Annotations[model.StateAnnotation]), &state); err != nil {
		t.Fatal(err)
	}
	if state.OriginalSuspended == nil || *state.OriginalSuspended {
		t.Fatalf("unexpected state: %#v", state)
	}
}

func TestDownSuspendsGitOpsResourceAndSavesState(t *testing.T) {
	item := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": model.KindHelmRelease,
		"metadata": map[string]any{"namespace": "test", "name": "store"},
		"spec":     map[string]any{"suspend": false},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), item)
	resource := &model.GitOpsResource{
		Ref:         model.GitOpsRef{Provider: model.ProviderFlux, APIVersion: item.GetAPIVersion(), Kind: item.GetKind(), Namespace: item.GetNamespace(), Name: item.GetName()},
		Annotations: map[string]string{},
	}
	plan := &planner.Plan{Direction: planner.Down, Before: []planner.GitOpsPhase{{
		Name: model.KindHelmRelease, Actions: []planner.GitOpsAction{{Resource: resource, TargetSuspended: true}},
	}}}
	runner := newTestExecutor(fake.NewClientset())
	runner.Dynamic = dynamicClient
	if err := runner.Run(context.Background(), plan, "operation"); err != nil {
		t.Fatal(err)
	}
	updated, err := dynamicClient.Resource(gitOpsGVR(resource.Ref)).Namespace("test").Get(context.Background(), "store", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	suspended, _, _ := unstructured.NestedBool(updated.Object, "spec", "suspend")
	if !suspended || updated.GetAnnotations()[model.GitOpsStateAnnotation] == "" {
		t.Fatalf("GitOps state was not persisted and suspended: %#v", updated.Object)
	}
}

func TestUpRestoresArgoAnnotationExactly(t *testing.T) {
	originalSuspended := false
	originalValue := "false"
	state := planner.GitOpsState{OriginalSuspended: &originalSuspended, OriginalSkipReconcile: &originalValue}
	raw, _ := json.Marshal(state)
	item := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": model.KindApplication,
		"metadata": map[string]any{
			"namespace": "argocd", "name": "store",
			"annotations": map[string]any{
				"argocd.argoproj.io/skip-reconcile": "true",
				model.GitOpsStateAnnotation:         string(raw),
			},
		},
	}}
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), item)
	resource := &model.GitOpsResource{
		Ref:         model.GitOpsRef{Provider: model.ProviderArgoCD, APIVersion: item.GetAPIVersion(), Kind: item.GetKind(), Namespace: item.GetNamespace(), Name: item.GetName()},
		Annotations: item.GetAnnotations(), Suspended: true,
	}
	plan := &planner.Plan{Direction: planner.Up, After: []planner.GitOpsPhase{{
		Name: model.KindApplication, Actions: []planner.GitOpsAction{{Resource: resource, TargetSuspended: false, State: &state}},
	}}}
	runner := newTestExecutor(fake.NewClientset())
	runner.Dynamic = dynamicClient
	if err := runner.Run(context.Background(), plan, "operation"); err != nil {
		t.Fatal(err)
	}
	updated, err := dynamicClient.Resource(gitOpsGVR(resource.Ref)).Namespace("argocd").Get(context.Background(), "store", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	annotations := updated.GetAnnotations()
	if annotations["argocd.argoproj.io/skip-reconcile"] != "false" || annotations[model.GitOpsStateAnnotation] != "" {
		t.Fatalf("Argo CD annotations were not restored: %#v", annotations)
	}
}

func newTestExecutor(client *fake.Clientset) *Executor {
	return &Executor{Client: client, Parallelism: 2, Timeout: time.Second, Poll: time.Millisecond, Output: &bytes.Buffer{}, Now: func() time.Time { return time.Unix(0, 0) }}
}
