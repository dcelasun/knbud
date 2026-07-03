package main

import (
	"bufio"
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/discovery"
	"github.com/dcelasun/knbud/internal/kube"
	"github.com/dcelasun/knbud/internal/model"
	"github.com/dcelasun/knbud/internal/planner"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func nfsDeployment(namespace, name string) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: appsv1.DeploymentSpec{Replicas: lo.ToPtr(int32(1)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "nfs", Path: "/d"}}}},
		}}},
	}
}

func namedGroup(namespace, name string) config.Group {
	return config.Group{Resources: []config.ResourceSelector{{Kind: model.KindDeployment, Namespace: namespace, Name: name}}}
}

func TestResolveCyclesDropsInferredEdge(t *testing.T) {
	snapshot := &kube.Snapshot{Deployments: []appsv1.Deployment{nfsDeployment("app", "a"), nfsDeployment("app", "b")}}
	cfg := &config.Config{
		Version: 1, StorageClasses: []string{"nfs"},
		Groups:       map[string]config.Group{"a": namedGroup("app", "a"), "b": namedGroup("app", "b")},
		Dependencies: []config.Dependency{{Consumer: "a", Provider: "b"}, {Consumer: "b", Provider: "a"}},
	}
	var output bytes.Buffer
	if err := resolveCycles(nil, &output, snapshot, cfg, map[string]bool{}); err != nil {
		t.Fatalf("resolveCycles should self-heal an inferred cycle: %v", err)
	}
	if len(cfg.Dependencies) != 1 {
		t.Fatalf("expected one dependency to be dropped, got %#v", cfg.Dependencies)
	}
}

func TestBuildPlanFormatsCustomCycle(t *testing.T) {
	a := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "a"}, Replicas: 1, DirectNFS: true, Annotations: map[string]string{}}
	b := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "b"}, Replicas: 1, DirectNFS: true, Annotations: map[string]string{}}
	result := &discovery.Result{
		Inventory: &model.Inventory{
			Workloads: map[string]*model.Workload{a.Ref.ID(): a, b.Ref.ID(): b},
			Edges:     []model.Edge{{Consumer: a.Ref, Provider: b.Ref}, {Consumer: b.Ref, Provider: a.Ref}},
		},
		Included: map[string]bool{a.Ref.ID(): true, b.Ref.ID(): true},
		HPAs:     map[string]string{},
		Groups:   map[string][]model.Ref{"a": {a.Ref}, "b": {b.Ref}},
	}
	cfg := &config.Config{Version: 1, StorageClasses: []string{"nfs"}, CustomDependencies: []config.Dependency{{Consumer: "b", Provider: "a"}}}
	_, err := buildPlan(result, cfg, planner.Down)
	if err == nil || !strings.Contains(err.Error(), "custom dependency creates a dependency cycle") {
		t.Fatalf("expected formatted custom cycle error, got %v", err)
	}
	if !strings.Contains(err.Error(), "(custom dependency)") {
		t.Fatalf("expected the custom dependency to be tagged: %v", err)
	}
}

func TestResolveCyclesRejectsCustomCycle(t *testing.T) {
	snapshot := &kube.Snapshot{Deployments: []appsv1.Deployment{nfsDeployment("app", "a"), nfsDeployment("app", "b")}}
	cfg := &config.Config{
		Version: 1, StorageClasses: []string{"nfs"},
		Groups:             map[string]config.Group{"a": namedGroup("app", "a"), "b": namedGroup("app", "b")},
		Dependencies:       []config.Dependency{{Consumer: "a", Provider: "b"}},
		CustomDependencies: []config.Dependency{{Consumer: "b", Provider: "a"}},
	}
	var output bytes.Buffer
	err := resolveCycles(nil, &output, snapshot, cfg, map[string]bool{})
	if err == nil || !strings.Contains(err.Error(), "custom dependency") {
		t.Fatalf("expected a custom dependency cycle error, got %v", err)
	}
}

func TestHelp(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	if err := command.Run(context.Background(), []string{"knbud", "--help"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "discover") || !strings.Contains(output.String(), "config") || !strings.Contains(output.String(), "plan") {
		t.Fatalf("unexpected help output: %s", output.String())
	}
}

func TestParseRefs(t *testing.T) {
	refs, err := parseRefs([]string{"deployment/app/frontend", "sts/data/store"})
	if err != nil {
		t.Fatal(err)
	}
	if refs[0].Kind != model.KindDeployment || refs[1].Kind != model.KindStatefulSet {
		t.Fatalf("unexpected refs: %#v", refs)
	}
	if _, err := parseRefs([]string{"invalid"}); err == nil {
		t.Fatal("expected invalid ref error")
	}
}

func TestDiscoverRejectsMissingExplicitConfigBeforeClusterAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "--config", path})
	if err == nil || !strings.Contains(err.Error(), "open config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiscoverRejectsConflictingSuggestionFlags(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "--accept-suggestions", "--ignore-suggestions"})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDiscoverRejectsDryRunSuggestionDecisions(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "--dry-run", "--accept-suggestions"})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseSelection(t *testing.T) {
	selected, err := parseSelection("1, 3", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || !selected[0] || !selected[2] {
		t.Fatalf("unexpected selection: %#v", selected)
	}
	selected, err = parseSelection("all", 2)
	if err != nil || len(selected) != 2 {
		t.Fatalf("unexpected all selection: %#v, %v", selected, err)
	}
	if _, err := parseSelection("2x", 3); err == nil {
		t.Fatal("expected invalid selection error")
	}
}

func TestSelectCandidates(t *testing.T) {
	candidates := []model.DependencyCandidate{
		{Consumer: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "one"}, Provider: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "provider"}},
		{Consumer: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "two"}, Provider: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "provider"}},
	}
	var output bytes.Buffer
	accepted, err := selectCandidates(bufio.NewReader(strings.NewReader("2\n")), &output, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 || !accepted[1] {
		t.Fatalf("unexpected decision: %#v", accepted)
	}
}

func TestSelectWorkloadsRequiresSpecificSearch(t *testing.T) {
	workloads := map[string]*model.Workload{}
	for _, name := range []string{"consumer", "provider"} {
		workload := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: name}}
		workloads[workload.Ref.ID()] = workload
	}
	var output bytes.Buffer
	refs, err := selectWorkloads(bufio.NewReader(strings.NewReader("\nmissing\nconsumer\n1\n")), &output, "consumer", workloads)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].Name != "consumer" {
		t.Fatalf("unexpected refs: %#v", refs)
	}
	if !strings.Contains(output.String(), "Enter part of its name") || !strings.Contains(output.String(), "No workloads match") {
		t.Fatalf("missing search guidance: %s", output.String())
	}
	if strings.Contains(output.String(), "Deployment/app/provider") {
		t.Fatalf("blank search exposed the full workload list: %s", output.String())
	}
}

func TestConfigureInteractively(t *testing.T) {
	consumer := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "consumer"}}
	provider := &model.Workload{Ref: model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "provider"}}
	included := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "ops", Name: "operator"}}
	flux := &model.GitOpsResource{Ref: model.GitOpsRef{Provider: model.ProviderFlux, Kind: model.KindKustomization, Namespace: "flux-system", Name: "apps"}}
	result := &discovery.Result{Inventory: &model.Inventory{
		Workloads: map[string]*model.Workload{
			consumer.Ref.ID(): consumer,
			provider.Ref.ID(): provider,
			included.Ref.ID(): included,
		},
		GitOpsResources: map[string]*model.GitOpsResource{flux.Ref.ID(): flux},
	}}
	input := strings.Join([]string{"", "y", "consumer", "1", "provider", "1", "n", "y", "operator", "1", "n", ""}, "\n")
	cfg := &config.Config{Version: 1, StorageClasses: []string{"nfs"}}
	var output bytes.Buffer
	if err := configureInteractively(bufio.NewReader(strings.NewReader(input)), &output, cfg, result); err != nil {
		t.Fatal(err)
	}
	if !cfg.GitOps.Flux.Enabled || len(cfg.CustomDependencies) != 1 || len(cfg.Include) != 1 {
		t.Fatalf("unexpected interactive config: %#v", cfg)
	}
}

func TestRejectsInvalidOutputBeforeLoadingConfig(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "--output", "xml"})
	if err == nil || !strings.Contains(err.Error(), "unsupported output format") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRejectsUnexpectedArguments(t *testing.T) {
	var output bytes.Buffer
	command := newCommand(strings.NewReader(""), &output, &output)
	err := command.Run(context.Background(), []string{"knbud", "discover", "extra"})
	if err == nil || !strings.Contains(err.Error(), "unexpected arguments") {
		t.Fatalf("unexpected error: %v", err)
	}
}
