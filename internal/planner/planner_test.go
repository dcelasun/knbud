package planner

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/discovery"
	"github.com/dcelasun/knbud/internal/model"
)

func workload(name string, direct bool) *model.Workload {
	return &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "test", Name: name}, Replicas: 1, DirectNFS: direct, Annotations: map[string]string{}}
}

func TestBuildDownIncludesConsumersAndOrdersWaves(t *testing.T) {
	store := workload("store", true)
	api := workload("api", false)
	result := &discovery.Result{
		Inventory: &model.Inventory{Workloads: map[string]*model.Workload{store.Ref.ID(): store, api.Ref.ID(): api}, Edges: []model.Edge{{Consumer: api.Ref, Provider: store.Ref}}},
		Included:  map[string]bool{store.Ref.ID(): true}, HPAs: map[string]string{},
	}
	plan, err := Build(result, Down)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Waves) != 2 || plan.Waves[0][0].Workload.Ref.Name != "api" || plan.Waves[1][0].Workload.Ref.Name != "store" {
		t.Fatalf("unexpected plan: %#v", plan.Waves)
	}
}

func TestBuildUpUsesSavedStateAndReversesWaves(t *testing.T) {
	store := workload("store", true)
	api := workload("api", false)
	for _, item := range []*model.Workload{store, api} {
		replicas := int32(2)
		raw, _ := json.Marshal(State{OperationID: "one", OriginalReplicas: &replicas})
		item.Annotations[model.StateAnnotation] = string(raw)
	}
	result := &discovery.Result{
		Inventory: &model.Inventory{Workloads: map[string]*model.Workload{store.Ref.ID(): store, api.Ref.ID(): api}, Edges: []model.Edge{{Consumer: api.Ref, Provider: store.Ref}}},
		Included:  map[string]bool{}, HPAs: map[string]string{},
	}
	plan, err := Build(result, Up)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Waves[0][0].Workload.Ref.Name != "store" || plan.Waves[1][0].Workload.Ref.Name != "api" {
		t.Fatalf("unexpected up plan: %#v", plan.Waves)
	}
}

func TestBuildRejectsHPA(t *testing.T) {
	store := workload("store", true)
	result := &discovery.Result{Inventory: &model.Inventory{Workloads: map[string]*model.Workload{store.Ref.ID(): store}}, Included: map[string]bool{store.Ref.ID(): true}, HPAs: map[string]string{store.Ref.ID(): "test/store"}}
	if _, err := Build(result, Down); err == nil {
		t.Fatal("expected HPA error")
	}
}

func TestBuildUpRestoresExcludedWorkload(t *testing.T) {
	app := workload("app", false)
	original := int32(3)
	raw, _ := json.Marshal(State{OperationID: "one", OriginalReplicas: &original})
	app.Annotations[model.StateAnnotation] = string(raw)
	result := &discovery.Result{
		Inventory: &model.Inventory{Workloads: map[string]*model.Workload{app.Ref.ID(): app}},
		Excluded:  map[string]bool{app.Ref.ID(): true}, HPAs: map[string]string{},
	}
	plan, err := Build(result, Up)
	if err != nil {
		t.Fatal(err)
	}
	if got := *plan.Waves[0][0].TargetReplicas; got != 3 {
		t.Fatalf("expected 3 replicas, got %d", got)
	}
}

func TestBuildDownWarnsOnOperatorManagedWorkload(t *testing.T) {
	store := workload("store", true)
	store.ManagedBy = &model.ControllerRef{APIVersion: "example.io/v1", Kind: "DatabaseCluster", Name: "database"}
	result := &discovery.Result{
		Inventory: &model.Inventory{Workloads: map[string]*model.Workload{store.Ref.ID(): store}},
		Included:  map[string]bool{store.Ref.ID(): true}, HPAs: map[string]string{},
	}
	plan, err := Build(result, Down)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Warnings) != 1 || !strings.Contains(plan.Warnings[0], "managed by DatabaseCluster/database") {
		t.Fatalf("expected operator warning, got %#v", plan.Warnings)
	}
}

func TestBuildDownAddsFluxAutoPhases(t *testing.T) {
	store := workload("store", true)
	kustomization := &model.GitOpsResource{Ref: model.GitOpsRef{
		Provider: model.ProviderFlux, APIVersion: "kustomize.toolkit.fluxcd.io/v1",
		Kind: model.KindKustomization, Namespace: "gitops-system", Name: "store",
	}, Annotations: map[string]string{}}
	helmRelease := &model.GitOpsResource{Ref: model.GitOpsRef{
		Provider: model.ProviderFlux, APIVersion: "helm.toolkit.fluxcd.io/v2",
		Kind: model.KindHelmRelease, Namespace: "test", Name: "store",
	}, Annotations: map[string]string{}}
	result := &discovery.Result{
		Inventory: &model.Inventory{
			Workloads:       map[string]*model.Workload{store.Ref.ID(): store},
			GitOpsResources: map[string]*model.GitOpsResource{kustomization.Ref.ID(): kustomization, helmRelease.Ref.ID(): helmRelease},
			GitOpsOwnership: []model.GitOpsOwnership{{Workload: store.Ref, Owner: helmRelease.Ref}, {Workload: store.Ref, Owner: kustomization.Ref}},
		},
		Included: map[string]bool{store.Ref.ID(): true}, HPAs: map[string]string{},
		GitOps: config.GitOps{Flux: config.GitOpsProvider{Enabled: true, Mode: "auto"}},
	}
	plan, err := Build(result, Down)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Before) != 2 || plan.Before[0].Name != model.KindKustomization || plan.Before[1].Name != model.KindHelmRelease {
		t.Fatalf("unexpected GitOps phases: %#v", plan.Before)
	}
}

func TestBuildUpCanRestoreOnlyGitOpsState(t *testing.T) {
	originalSuspended := false
	state, _ := json.Marshal(GitOpsState{OriginalSuspended: &originalSuspended})
	resource := &model.GitOpsResource{
		Ref:         model.GitOpsRef{Provider: model.ProviderFlux, APIVersion: "helm.toolkit.fluxcd.io/v2", Kind: model.KindHelmRelease, Namespace: "test", Name: "store"},
		Annotations: map[string]string{model.GitOpsStateAnnotation: string(state)}, Suspended: true,
	}
	result := &discovery.Result{Inventory: &model.Inventory{
		Workloads: map[string]*model.Workload{}, GitOpsResources: map[string]*model.GitOpsResource{resource.Ref.ID(): resource},
	}, HPAs: map[string]string{}}
	plan, err := Build(result, Up)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Waves) != 0 || len(plan.After) != 1 || plan.After[0].Actions[0].TargetSuspended {
		t.Fatalf("unexpected GitOps-only plan: %#v", plan)
	}
}
