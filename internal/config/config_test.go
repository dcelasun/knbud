package config

import (
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
