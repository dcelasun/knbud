package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dcelasun/knbud/internal/model"
)

func TestDecodeRejectsUnknownFields(t *testing.T) {
	_, err := Decode(strings.NewReader("version: 1\nstorageClasses: [nfs]\nunknown: true\n"))
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestDecodeRejectsMissingDependencyGroup(t *testing.T) {
	input := `
version: 1
storageClasses: [nfs]
groups:
  app:
    resources:
      - kind: Deployment
        namespace: app
        name: app
dependencies:
  - consumer: app
    provider: missing
`
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "provider group") {
		t.Fatalf("expected missing provider error, got %v", err)
	}
}

func TestResolveLabelSelector(t *testing.T) {
	selector := ResourceSelector{Kind: "Deployment", Namespace: "app", MatchLabels: map[string]string{"app": "one"}}
	workloads := map[string]*model.Workload{
		"one": {Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "one"}, Labels: map[string]string{"app": "one"}},
		"two": {Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "two"}, Labels: map[string]string{"app": "two"}},
	}
	refs := Resolve([]ResourceSelector{selector}, workloads)
	if len(refs) != 1 || refs[0].Name != "one" {
		t.Fatalf("unexpected refs: %#v", refs)
	}
}

func TestDecodeValidatesGitOpsModes(t *testing.T) {
	input := `
version: 1
storageClasses: [nfs]
gitOps:
  argoCD:
    enabled: true
    mode: auto
`
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "only explicit") {
		t.Fatalf("expected Argo CD mode error, got %v", err)
	}
}

func TestDecodeAcceptsExplicitFluxResources(t *testing.T) {
	input := `
version: 1
storageClasses: [nfs]
gitOps:
  flux:
    enabled: true
    mode: explicit
    resources:
      - kind: Kustomization
        namespace: gitops-system
        name: application
`
	if _, err := Decode(strings.NewReader(input)); err != nil {
		t.Fatal(err)
	}
}

func TestCreateRefusesToOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knbud.yaml")
	if err := os.WriteFile(path, []byte("existing\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := Create(path, &Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err == nil || !strings.Contains(err.Error(), "file exists") {
		t.Fatalf("expected file exists error, got %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "existing\n" {
		t.Fatalf("existing file was modified: %q", raw)
	}
}

func TestCreateWritesMinimalConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knbud.yaml")
	enabled := true
	cfg := &Config{
		Version: 1, StorageClasses: []string{"nfs"},
		Inference: Inference{ServiceReferences: &enabled},
	}
	if err := Create(path, cfg); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := "version: 1\nstorageClasses:\n    - nfs\ninference:\n    serviceReferences: true\n"
	if string(raw) != expected {
		t.Fatalf("unexpected generated config:\n%s", raw)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptAndIgnoreDependencyCandidates(t *testing.T) {
	cfg := &Config{Version: 1, StorageClasses: []string{"nfs"}}
	candidate := model.DependencyCandidate{
		Consumer: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "frontend"},
		Provider: model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "store"},
	}
	cfg.Accept(candidate)
	cfg.Accept(candidate)
	if len(cfg.Groups) != 2 || len(cfg.Dependencies) != 1 {
		t.Fatalf("unexpected accepted config: %#v", cfg)
	}
	cfg.Ignore(candidate)
	cfg.Ignore(candidate)
	if len(cfg.Inference.Ignore) != 1 {
		t.Fatalf("unexpected ignored config: %#v", cfg.Inference.Ignore)
	}
}

func TestAcceptDependencyCreatesReusableMultiResourceGroups(t *testing.T) {
	cfg := &Config{Version: 1, StorageClasses: []string{"nfs"}}
	consumers := []model.Ref{
		{Kind: model.KindDeployment, Namespace: "app", Name: "web"},
		{Kind: model.KindDeployment, Namespace: "app", Name: "worker"},
	}
	providers := []model.Ref{
		{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"},
		{Kind: model.KindDeployment, Namespace: "data", Name: "cache"},
	}
	cfg.AcceptDependency(consumers, providers)
	cfg.AcceptDependency([]model.Ref{consumers[1], consumers[0]}, []model.Ref{providers[1], providers[0]})
	if len(cfg.Groups) != 2 || len(cfg.Dependencies) != 1 {
		t.Fatalf("unexpected groups or dependencies: %#v", cfg)
	}
	cfg.IncludeResources([]model.Ref{consumers[0], consumers[0]})
	if len(cfg.Include) != 1 {
		t.Fatalf("include was not deduplicated: %#v", cfg.Include)
	}
}

func TestAcceptCustomDependencyUsesCustomSections(t *testing.T) {
	cfg := &Config{Version: 1, StorageClasses: []string{"nfs"}}
	consumer := model.Ref{Kind: model.KindDeployment, Namespace: "web", Name: "frontend"}
	provider := model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"}
	cfg.AcceptCustomDependency([]model.Ref{consumer}, []model.Ref{provider})
	if len(cfg.Groups) != 0 || len(cfg.Dependencies) != 0 {
		t.Fatalf("custom dependency changed discovered sections: %#v", cfg)
	}
	if len(cfg.CustomGroups) != 2 || len(cfg.CustomDependencies) != 1 {
		t.Fatalf("custom dependency was not recorded: %#v", cfg)
	}
}

func TestResetDiscoveredPreservesCustomConfiguration(t *testing.T) {
	cfg := &Config{
		Groups: map[string]Group{
			"frontend": {Resources: []ResourceSelector{{Kind: model.KindDeployment, Namespace: "web", Name: "frontend"}}},
			"database": {Resources: []ResourceSelector{{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"}}},
		},
		Dependencies:       []Dependency{{Consumer: "frontend", Provider: "database"}},
		CustomDependencies: []Dependency{{Consumer: "frontend", Provider: "database"}},
	}
	cfg.ResetDiscovered()
	if len(cfg.Groups) != 0 || len(cfg.Dependencies) != 0 {
		t.Fatalf("discovered sections were not reset: %#v", cfg)
	}
	if len(cfg.CustomGroups) != 2 || len(cfg.CustomDependencies) != 1 {
		t.Fatalf("custom configuration was not preserved: %#v", cfg)
	}
}

func TestAcceptDependencyUsesWorkloadNames(t *testing.T) {
	cfg := &Config{Version: 1, StorageClasses: []string{"nfs"}}
	cfg.Accept(model.DependencyCandidate{
		Consumer: model.Ref{Kind: model.KindDeployment, Namespace: "web", Name: "frontend"},
		Provider: model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"},
	})
	if _, ok := cfg.Groups["frontend"]; !ok {
		t.Fatalf("group should be named after the workload: %#v", cfg.Groups)
	}
	if _, ok := cfg.Groups["database"]; !ok {
		t.Fatalf("group should be named after the workload: %#v", cfg.Groups)
	}
	if len(cfg.Dependencies) != 1 || cfg.Dependencies[0].Consumer != "frontend" || cfg.Dependencies[0].Provider != "database" {
		t.Fatalf("unexpected dependency: %#v", cfg.Dependencies)
	}
}

func TestUniqueGroupNameResolvesCollisions(t *testing.T) {
	cfg := &Config{Version: 1, StorageClasses: []string{"nfs"}}
	cfg.Accept(model.DependencyCandidate{
		Consumer: model.Ref{Kind: model.KindDeployment, Namespace: "a", Name: "worker"},
		Provider: model.Ref{Kind: model.KindDeployment, Namespace: "a", Name: "sink"},
	})
	cfg.Accept(model.DependencyCandidate{
		Consumer: model.Ref{Kind: model.KindCronJob, Namespace: "b", Name: "worker"},
		Provider: model.Ref{Kind: model.KindDeployment, Namespace: "a", Name: "sink"},
	})
	if _, ok := cfg.Groups["worker"]; !ok {
		t.Fatalf("first worker should keep the plain name: %#v", cfg.Groups)
	}
	if _, ok := cfg.Groups["b-worker"]; !ok {
		t.Fatalf("colliding worker should be namespace-qualified: %#v", cfg.Groups)
	}
}

func TestDecodeAcceptsCustomGroupsAndDependencies(t *testing.T) {
	input := `
version: 1
storageClasses: [nfs]
groups:
  database:
    resources:
      - kind: StatefulSet
        namespace: data
        name: database
customGroups:
  web:
    selectors:
      - kind: Deployment
        namespace: web
        matchLabels:
          app.kubernetes.io/part-of: web
customDependencies:
  - consumer: web
    provider: database
`
	cfg, err := Decode(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.CustomGroups) != 1 || len(cfg.CustomDependencies) != 1 {
		t.Fatalf("custom keys not decoded: %#v", cfg)
	}
	if _, ok := cfg.EffectiveGroups()["web"]; !ok {
		t.Fatalf("custom group not merged: %#v", cfg.EffectiveGroups())
	}
	if len(cfg.EffectiveDependencies()) != 1 {
		t.Fatalf("custom dependency not merged: %#v", cfg.EffectiveDependencies())
	}
}

func TestDecodeRejectsUnknownCustomDependencyGroup(t *testing.T) {
	input := `
version: 1
storageClasses: [nfs]
customGroups:
  web:
    resources:
      - kind: Deployment
        namespace: web
        name: frontend
customDependencies:
  - consumer: web
    provider: missing
`
	_, err := Decode(strings.NewReader(input))
	if err == nil || !strings.Contains(err.Error(), "custom dependency provider group") {
		t.Fatalf("expected missing custom provider error, got %v", err)
	}
}

func TestAcceptDependencyAvoidsCustomGroupNameCollision(t *testing.T) {
	cfg := &Config{
		Version: 1, StorageClasses: []string{"nfs"},
		CustomGroups: map[string]Group{"database": {Resources: []ResourceSelector{{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"}}}},
	}
	cfg.Accept(model.DependencyCandidate{
		Consumer: model.Ref{Kind: model.KindDeployment, Namespace: "data", Name: "database-ui"},
		Provider: model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"},
	})
	if _, ok := cfg.Groups["database"]; ok {
		t.Fatalf("machine group must not collide with custom group name: %#v", cfg.Groups)
	}
	if _, ok := cfg.Groups["data-database"]; !ok {
		t.Fatalf("expected namespace-qualified machine group, got %#v", cfg.Groups)
	}
}

func TestWritePreservesCustomKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knbud.yaml")
	cfg := &Config{
		Version: 1, StorageClasses: []string{"nfs"},
		CustomGroups: map[string]Group{
			"web":      {Resources: []ResourceSelector{{Kind: model.KindDeployment, Namespace: "web", Name: "frontend"}}},
			"database": {Resources: []ResourceSelector{{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"}}},
		},
		CustomDependencies: []Dependency{{Consumer: "web", Provider: "database"}},
	}
	if err := Write(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.CustomGroups) != 2 || len(loaded.CustomDependencies) != 1 || loaded.CustomDependencies[0].Provider != "database" {
		t.Fatalf("custom keys not preserved through write/load: %#v", loaded)
	}
}

func TestWriteReplacesConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "knbud.yaml")
	if err := os.WriteFile(path, []byte("old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Write(path, &Config{Version: 1, StorageClasses: []string{"nfs"}}); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.StorageClasses) != 1 || cfg.StorageClasses[0] != "nfs" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
}
