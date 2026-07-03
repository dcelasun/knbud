package output

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/dcelasun/knbud/internal/discovery"
	"github.com/dcelasun/knbud/internal/model"
	"github.com/dcelasun/knbud/internal/planner"
)

func Discovery(writer io.Writer, result *discovery.Result, format string) error {
	if format == "json" {
		return writeJSON(writer, struct {
			Workloads       []*model.Workload       `json:"workloads"`
			Edges           []model.Edge            `json:"edges"`
			Suggestions     []model.Suggestion      `json:"suggestions,omitempty"`
			Groups          map[string][]model.Ref  `json:"groups"`
			GitOpsResources []*model.GitOpsResource `json:"gitOpsResources,omitempty"`
			GitOpsOwnership []model.GitOpsOwnership `json:"gitOpsOwnership,omitempty"`
		}{
			model.SortedWorkloads(result.Inventory.Workloads), result.Inventory.Edges,
			result.Inventory.Suggestions, result.Groups, sortedGitOpsResources(result.Inventory.GitOpsResources),
			result.Inventory.GitOpsOwnership,
		})
	}
	if format != "human" {
		return fmt.Errorf("unsupported output format %q", format)
	}
	fmt.Fprintln(writer, "Direct NFS users:")
	count := 0
	for _, workload := range model.SortedWorkloads(result.Inventory.Workloads) {
		if workload.DirectNFS {
			fmt.Fprintf(writer, "  %s\n", workload.Ref.ID())
			count++
		}
	}
	if count == 0 {
		fmt.Fprintln(writer, "  none")
	}
	printEdges(writer, result.Inventory.Edges)
	printSuggestions(writer, result.Inventory.Suggestions)
	printGitOpsOwnership(writer, result.Inventory.GitOpsOwnership)
	return nil
}

func RenderPlan(writer io.Writer, plan *planner.Plan, format string) error {
	if format == "json" {
		return writeJSON(writer, plan)
	}
	if format != "human" {
		return fmt.Errorf("unsupported output format %q", format)
	}
	count := 0
	for _, wave := range plan.Waves {
		count += len(wave)
	}
	fmt.Fprintf(writer, "Plan: %s %d workloads in %d waves\n", plan.Direction, count, len(plan.Waves))
	printGitOpsPhases(writer, "Before workload changes", plan.Before)
	for index, wave := range plan.Waves {
		fmt.Fprintf(writer, "\nWave %d:\n", index+1)
		for _, action := range wave {
			target := ""
			if action.TargetReplicas != nil {
				target = fmt.Sprintf("replicas=%d", *action.TargetReplicas)
			} else {
				target = fmt.Sprintf("suspended=%t", *action.TargetSuspended)
			}
			origin := "direct dependency closure"
			if action.Workload.DirectNFS {
				origin = "direct NFS user"
			}
			fmt.Fprintf(writer, "  %-55s %-18s %s\n", action.Workload.Ref.ID(), target, origin)
		}
	}
	printGitOpsPhases(writer, "After workload changes", plan.After)
	if len(plan.Warnings) > 0 {
		fmt.Fprintln(writer, "\nWarnings:")
		for _, warning := range plan.Warnings {
			fmt.Fprintf(writer, "  %s\n", warning)
		}
	}
	printEdges(writer, plan.Edges)
	printSuggestions(writer, plan.Suggestions)
	return nil
}

func printGitOpsPhases(writer io.Writer, title string, phases []planner.GitOpsPhase) {
	if len(phases) == 0 {
		return
	}
	fmt.Fprintf(writer, "\n%s:\n", title)
	for _, phase := range phases {
		for _, action := range phase.Actions {
			fmt.Fprintf(writer, "  %-55s suspended=%t\n", action.Resource.Ref.ID(), action.TargetSuspended)
		}
	}
}

func printGitOpsOwnership(writer io.Writer, ownership []model.GitOpsOwnership) {
	fmt.Fprintln(writer, "\nGitOps ownership:")
	if len(ownership) == 0 {
		fmt.Fprintln(writer, "  none")
		return
	}
	for _, item := range ownership {
		fmt.Fprintf(writer, "  %s -> %s\n", item.Workload.ID(), item.Owner.ID())
	}
}

func sortedGitOpsResources(resources map[string]*model.GitOpsResource) []*model.GitOpsResource {
	result := make([]*model.GitOpsResource, 0, len(resources))
	for _, resource := range resources {
		result = append(result, resource)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Ref.ID() < result[j].Ref.ID() })
	return result
}

func printEdges(writer io.Writer, edges []model.Edge) {
	fmt.Fprintln(writer, "\nActive dependencies:")
	if len(edges) == 0 {
		fmt.Fprintln(writer, "  none")
		return
	}
	for _, edge := range edges {
		fmt.Fprintf(writer, "  %s -> %s [%s: %s]\n", edge.Consumer.ID(), edge.Provider.ID(), edge.Type, edge.Evidence)
	}
}

func printSuggestions(writer io.Writer, suggestions []model.Suggestion) {
	fmt.Fprintln(writer, "\nSuggestions:")
	if len(suggestions) == 0 {
		fmt.Fprintln(writer, "  none")
		return
	}
	sort.Slice(suggestions, func(i, j int) bool {
		left := suggestions[i].Consumer.ID() + suggestions[i].Evidence + suggestions[i].Reason
		right := suggestions[j].Consumer.ID() + suggestions[j].Evidence + suggestions[j].Reason
		return left < right
	})
	for _, suggestion := range suggestions {
		fmt.Fprintf(writer, "  %s: %s (%s)", suggestion.Consumer.ID(), suggestion.Evidence, suggestion.Reason)
		if len(suggestion.Targets) > 0 {
			fmt.Fprint(writer, " targets=")
			for index, target := range suggestion.Targets {
				if index > 0 {
					fmt.Fprint(writer, ",")
				}
				fmt.Fprint(writer, target.ID())
			}
		}
		fmt.Fprintln(writer)
	}
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
