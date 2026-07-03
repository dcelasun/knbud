package planner

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/discovery"
	"github.com/dcelasun/knbud/internal/graph"
	"github.com/dcelasun/knbud/internal/model"
	"github.com/samber/lo"
)

type Direction string

const (
	Down Direction = "down"
	Up   Direction = "up"
)

type State struct {
	OperationID       string `json:"operationID"`
	OriginalReplicas  *int32 `json:"originalReplicas,omitempty"`
	OriginalSuspended *bool  `json:"originalSuspended,omitempty"`
	SavedAt           string `json:"savedAt"`
}

type Action struct {
	Workload          *model.Workload `json:"workload"`
	TargetReplicas    *int32          `json:"targetReplicas,omitempty"`
	TargetSuspended   *bool           `json:"targetSuspended,omitempty"`
	State             *State          `json:"state,omitempty"`
	DependencyReasons []model.Edge    `json:"dependencyReasons,omitempty"`
}

type GitOpsState struct {
	OperationID           string  `json:"operationID"`
	OriginalSuspended     *bool   `json:"originalSuspended"`
	OriginalSkipReconcile *string `json:"originalSkipReconcile,omitempty"`
	SavedAt               string  `json:"savedAt"`
}

type GitOpsAction struct {
	Resource        *model.GitOpsResource `json:"resource"`
	TargetSuspended bool                  `json:"targetSuspended"`
	State           *GitOpsState          `json:"state,omitempty"`
}

type GitOpsPhase struct {
	Name    string         `json:"name"`
	Actions []GitOpsAction `json:"actions"`
}

type Plan struct {
	Direction   Direction          `json:"direction"`
	Before      []GitOpsPhase      `json:"before,omitempty"`
	Waves       [][]Action         `json:"waves"`
	After       []GitOpsPhase      `json:"after,omitempty"`
	Edges       []model.Edge       `json:"edges"`
	Suggestions []model.Suggestion `json:"suggestions,omitempty"`
	Warnings    []string           `json:"warnings,omitempty"`
}

func Build(result *discovery.Result, direction Direction) (*Plan, error) {
	if direction != Down && direction != Up {
		return nil, fmt.Errorf("invalid direction %q", direction)
	}
	selected := make(map[string]bool)
	states := make(map[string]*State)
	if direction == Down {
		if len(result.UnresolvedGroups) > 0 {
			return nil, fmt.Errorf("configuration groups resolve to no workloads: %v", result.UnresolvedGroups)
		}
		selected = graph.ConsumerClosureExcept(result.Included, result.Inventory.Edges, result.Excluded)
	} else {
		for id, workload := range result.Inventory.Workloads {
			raw := workload.Annotations[model.StateAnnotation]
			if raw == "" {
				continue
			}
			var state State
			if err := json.Unmarshal([]byte(raw), &state); err != nil {
				return nil, fmt.Errorf("parse state for %s: %w", id, err)
			}
			selected[id] = true
			states[id] = &state
		}
	}
	if direction == Down && len(selected) == 0 {
		return nil, fmt.Errorf("no NFS-backed or explicitly included workloads found")
	}
	for id := range selected {
		if hpa, ok := result.HPAs[id]; ok {
			return nil, fmt.Errorf("%s is controlled by HorizontalPodAutoscaler %s", id, hpa)
		}
	}
	var waves [][]string
	if len(selected) > 0 {
		var err error
		waves, err = graph.Waves(selected, result.Inventory.Edges)
		if err != nil {
			return nil, err
		}
		if direction == Up {
			slices.Reverse(waves)
		}
	}

	plan := &Plan{Direction: direction, Edges: relevantEdges(result.Inventory.Edges, selected), Suggestions: relevantSuggestions(result.Inventory.Suggestions, selected)}
	if direction == Down {
		plan.Warnings = activeJobWarnings(result, selected)
	}
	for _, ids := range waves {
		wave := make([]Action, 0, len(ids))
		for _, id := range ids {
			workload := result.Inventory.Workloads[id]
			action := Action{Workload: workload, State: states[id], DependencyReasons: incomingEdges(result.Inventory.Edges, id, selected)}
			if direction == Down {
				if workload.Ref.Kind == model.KindCronJob {
					value := true
					action.TargetSuspended = &value
				} else {
					value := int32(0)
					action.TargetReplicas = &value
				}
			} else if workload.Ref.Kind == model.KindCronJob {
				if action.State.OriginalSuspended == nil {
					return nil, fmt.Errorf("saved state for %s has no original suspended value", id)
				}
				action.TargetSuspended = action.State.OriginalSuspended
			} else {
				if action.State.OriginalReplicas == nil {
					return nil, fmt.Errorf("saved state for %s has no original replica count", id)
				}
				action.TargetReplicas = action.State.OriginalReplicas
			}
			wave = append(wave, action)
		}
		plan.Waves = append(plan.Waves, wave)
	}
	before, after, warnings, err := buildGitOps(result, selected, direction)
	if err != nil {
		return nil, err
	}
	plan.Before = before
	plan.After = after
	plan.Warnings = append(plan.Warnings, warnings...)
	sort.Strings(plan.Warnings)
	if direction == Up && len(plan.Waves) == 0 && len(plan.After) == 0 {
		return nil, fmt.Errorf("no workloads or GitOps resources have saved knbud state")
	}
	return plan, nil
}

func buildGitOps(result *discovery.Result, selected map[string]bool, direction Direction) ([]GitOpsPhase, []GitOpsPhase, []string, error) {
	resources := result.Inventory.GitOpsResources
	chosen := make(map[string]*model.GitOpsResource)
	var warnings []string
	if direction == Down {
		detected := make(map[string]map[string]bool)
		for _, ownership := range result.Inventory.GitOpsOwnership {
			if !selected[ownership.Workload.ID()] {
				continue
			}
			if detected[ownership.Owner.Provider] == nil {
				detected[ownership.Owner.Provider] = make(map[string]bool)
			}
			detected[ownership.Owner.Provider][ownership.Owner.ID()] = true
		}
		providers := []struct {
			name   string
			policy config.GitOpsProvider
		}{{model.ProviderFlux, result.GitOps.Flux}, {model.ProviderArgoCD, result.GitOps.ArgoCD}}
		for _, provider := range providers {
			if !provider.policy.Enabled {
				if len(detected[provider.name]) > 0 {
					warnings = append(warnings, fmt.Sprintf("%d selected workload GitOps owner(s) from %s will not be suspended because the provider is disabled", len(detected[provider.name]), provider.name))
				}
				continue
			}
			if provider.policy.Mode == "auto" {
				for id := range detected[provider.name] {
					chosen[id] = resources[id]
				}
				continue
			}
			for _, configured := range provider.policy.Resources {
				id := gitOpsConfigID(provider.name, configured)
				resource := resources[id]
				if resource == nil {
					return nil, nil, nil, fmt.Errorf("configured GitOps resource %s was not found", id)
				}
				chosen[id] = resource
			}
		}
	} else {
		for id, resource := range resources {
			raw := resource.Annotations[model.GitOpsStateAnnotation]
			if raw != "" {
				chosen[id] = resource
			}
		}
	}

	byKind := make(map[string][]GitOpsAction)
	for _, resource := range chosen {
		action := GitOpsAction{Resource: resource, TargetSuspended: direction == Down}
		if direction == Up {
			var state GitOpsState
			if err := json.Unmarshal([]byte(resource.Annotations[model.GitOpsStateAnnotation]), &state); err != nil {
				return nil, nil, nil, fmt.Errorf("parse GitOps state for %s: %w", resource.Ref.ID(), err)
			}
			if state.OriginalSuspended == nil {
				return nil, nil, nil, fmt.Errorf("saved GitOps state for %s has no original suspended value", resource.Ref.ID())
			}
			action.State = &state
			action.TargetSuspended = *state.OriginalSuspended
		}
		byKind[resource.Ref.Kind] = append(byKind[resource.Ref.Kind], action)
	}
	for kind := range byKind {
		sort.Slice(byKind[kind], func(i, j int) bool {
			return byKind[kind][i].Resource.Ref.ID() < byKind[kind][j].Resource.Ref.ID()
		})
	}
	if direction == Down {
		return phases(byKind, []string{model.KindKustomization, model.KindHelmRelease, model.KindApplication}), nil, warnings, nil
	}
	return nil, phases(byKind, []string{model.KindHelmRelease, model.KindKustomization, model.KindApplication}), warnings, nil
}

func phases(actions map[string][]GitOpsAction, order []string) []GitOpsPhase {
	var result []GitOpsPhase
	for _, kind := range order {
		if len(actions[kind]) > 0 {
			result = append(result, GitOpsPhase{Name: kind, Actions: actions[kind]})
		}
	}
	return result
}

func gitOpsConfigID(provider string, resource config.GitOpsResource) string {
	return model.GitOpsRef{Provider: provider, Kind: resource.Kind, Namespace: resource.Namespace, Name: resource.Name}.ID()
}

func activeJobWarnings(result *discovery.Result, selected map[string]bool) []string {
	var warnings []string
	for _, job := range result.Jobs {
		if job.Status.Active == 0 {
			continue
		}
		for _, owner := range job.OwnerReferences {
			if owner.Kind != model.KindCronJob {
				continue
			}
			ref := model.Ref{Kind: model.KindCronJob, Namespace: job.Namespace, Name: owner.Name}
			if selected[ref.ID()] {
				warnings = append(warnings, fmt.Sprintf("active Job %s/%s must finish before %s can complete", job.Namespace, job.Name, ref.ID()))
			}
		}
	}
	sort.Strings(warnings)
	return warnings
}

func NewState(workload *model.Workload, operationID string, now time.Time) *State {
	state := &State{OperationID: operationID, SavedAt: now.UTC().Format(time.RFC3339)}
	if workload.Ref.Kind == model.KindCronJob {
		state.OriginalSuspended = lo.ToPtr(workload.Suspended)
	} else {
		state.OriginalReplicas = lo.ToPtr(workload.Replicas)
	}
	return state
}

func NewGitOpsState(resource *model.GitOpsResource, operationID string, now time.Time) *GitOpsState {
	state := &GitOpsState{
		OperationID: operationID, OriginalSuspended: lo.ToPtr(resource.Suspended),
		SavedAt: now.UTC().Format(time.RFC3339),
	}
	if resource.Ref.Provider == model.ProviderArgoCD {
		if value, ok := resource.Annotations["argocd.argoproj.io/skip-reconcile"]; ok {
			state.OriginalSkipReconcile = lo.ToPtr(value)
		}
	}
	return state
}

func relevantEdges(edges []model.Edge, selected map[string]bool) []model.Edge {
	var result []model.Edge
	for _, edge := range edges {
		if selected[edge.Consumer.ID()] && selected[edge.Provider.ID()] {
			result = append(result, edge)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID() < result[j].ID() })
	return result
}

func incomingEdges(edges []model.Edge, id string, selected map[string]bool) []model.Edge {
	var result []model.Edge
	for _, edge := range edges {
		if selected[edge.Consumer.ID()] && selected[edge.Provider.ID()] && (edge.Consumer.ID() == id || edge.Provider.ID() == id) {
			result = append(result, edge)
		}
	}
	return result
}

func relevantSuggestions(suggestions []model.Suggestion, selected map[string]bool) []model.Suggestion {
	var result []model.Suggestion
	for _, suggestion := range suggestions {
		keep := selected[suggestion.Consumer.ID()]
		for _, target := range suggestion.Targets {
			keep = keep || selected[target.ID()]
		}
		if keep {
			result = append(result, suggestion)
		}
	}
	return result
}
