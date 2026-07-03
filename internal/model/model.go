package model

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	KindDeployment        = "Deployment"
	KindStatefulSet       = "StatefulSet"
	KindCronJob           = "CronJob"
	StateAnnotation       = "knbud.io/state"
	GitOpsStateAnnotation = "knbud.io/gitops-state"
	ProviderFlux          = "flux"
	ProviderArgoCD        = "argocd"
	KindKustomization     = "Kustomization"
	KindHelmRelease       = "HelmRelease"
	KindApplication       = "Application"
)

type Ref struct {
	Kind      string `json:"kind" yaml:"kind"`
	Namespace string `json:"namespace" yaml:"namespace"`
	Name      string `json:"name" yaml:"name"`
}

func (r Ref) ID() string { return fmt.Sprintf("%s/%s/%s", r.Kind, r.Namespace, r.Name) }

func (r Ref) String() string { return r.ID() }

func ParseKind(kind string) (string, error) {
	switch strings.ToLower(kind) {
	case "deployment", "deployments":
		return KindDeployment, nil
	case "statefulset", "statefulsets", "sts":
		return KindStatefulSet, nil
	case "cronjob", "cronjobs":
		return KindCronJob, nil
	default:
		return "", fmt.Errorf("unsupported kind %q", kind)
	}
}

type Workload struct {
	Ref         Ref               `json:"ref"`
	Labels      map[string]string `json:"labels,omitempty"`
	PodLabels   map[string]string `json:"-"`
	Annotations map[string]string `json:"annotations,omitempty"`
	PodSpec     *corev1.PodSpec   `json:"-"`
	Replicas    int32             `json:"replicas,omitempty"`
	Suspended   bool              `json:"suspended,omitempty"`
	DirectNFS   bool              `json:"directNFS"`
	ManagedBy   *ControllerRef    `json:"managedBy,omitempty"`
	UID         string            `json:"-"`
}

// ControllerRef identifies a non-workload controller (typically an operator
// custom resource such as Prometheus or Alertmanager) that owns a workload and
// may revert manual scaling.
type ControllerRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

func (c ControllerRef) String() string { return c.Kind + "/" + c.Name }

type GitOpsRef struct {
	Provider   string `json:"provider" yaml:"provider"`
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
	Namespace  string `json:"namespace" yaml:"namespace"`
	Name       string `json:"name" yaml:"name"`
}

func (r GitOpsRef) ID() string {
	return fmt.Sprintf("%s/%s/%s/%s", r.Provider, r.Kind, r.Namespace, r.Name)
}

type GitOpsResource struct {
	Ref         GitOpsRef         `json:"ref"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Suspended   bool              `json:"suspended"`
}

type GitOpsOwnership struct {
	Workload Ref       `json:"workload"`
	Owner    GitOpsRef `json:"owner"`
}

type EvidenceType string

const (
	EvidenceExplicit EvidenceType = "explicit"
	EvidenceService  EvidenceType = "service-reference"
)

type Edge struct {
	Consumer Ref          `json:"consumer"`
	Provider Ref          `json:"provider"`
	Type     EvidenceType `json:"type"`
	Evidence string       `json:"evidence"`
}

func (e Edge) ID() string { return e.Consumer.ID() + "->" + e.Provider.ID() }

type Suggestion struct {
	Consumer Ref    `json:"consumer"`
	Targets  []Ref  `json:"targets,omitempty"`
	Evidence string `json:"evidence"`
	Reason   string `json:"reason"`
}

type DependencyCandidate struct {
	Consumer Ref      `json:"consumer"`
	Provider Ref      `json:"provider"`
	Evidence []string `json:"evidence"`
	Reason   string   `json:"reason"`
}

func DependencyCandidates(suggestions []Suggestion) []DependencyCandidate {
	byEdge := make(map[string]DependencyCandidate)
	for _, suggestion := range suggestions {
		for _, target := range suggestion.Targets {
			id := suggestion.Consumer.ID() + "->" + target.ID()
			candidate := byEdge[id]
			candidate.Consumer = suggestion.Consumer
			candidate.Provider = target
			candidate.Reason = suggestion.Reason
			if !slices.Contains(candidate.Evidence, suggestion.Evidence) {
				candidate.Evidence = append(candidate.Evidence, suggestion.Evidence)
				sort.Strings(candidate.Evidence)
			}
			byEdge[id] = candidate
		}
	}
	candidates := make([]DependencyCandidate, 0, len(byEdge))
	for _, candidate := range byEdge {
		candidates = append(candidates, candidate)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].Consumer.ID() + candidates[i].Provider.ID()
		right := candidates[j].Consumer.ID() + candidates[j].Provider.ID()
		return left < right
	})
	return candidates
}

func Diagnostics(suggestions []Suggestion) []Suggestion {
	var diagnostics []Suggestion
	for _, suggestion := range suggestions {
		if len(suggestion.Targets) == 0 {
			diagnostics = append(diagnostics, suggestion)
		}
	}
	return diagnostics
}

type Inventory struct {
	Workloads       map[string]*Workload
	Edges           []Edge
	Suggestions     []Suggestion
	GitOpsResources map[string]*GitOpsResource
	GitOpsOwnership []GitOpsOwnership
}

func SortedWorkloads(workloads map[string]*Workload) []*Workload {
	result := make([]*Workload, 0, len(workloads))
	for _, workload := range workloads {
		result = append(result, workload)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Ref.ID() < result[j].Ref.ID() })
	return result
}
