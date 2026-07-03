package graph

import (
	"sort"
	"strings"

	"github.com/dcelasun/knbud/internal/model"
)

// CycleError reports a dependency cycle and the edges that form it, so callers
// can render it with their own context (for example tagging custom dependencies).
type CycleError struct {
	Cycle []model.Edge
}

func (e *CycleError) Error() string {
	parts := make([]string, 0, len(e.Cycle))
	for _, edge := range e.Cycle {
		parts = append(parts, edge.Consumer.ID()+" -> "+edge.Provider.ID())
	}
	return "dependency cycle: " + strings.Join(parts, ", ")
}

// FindCycle returns the edges forming one dependency cycle among the selected
// workloads in consumer -> provider order, or nil when the graph is acyclic.
func FindCycle(selected map[string]bool, edges []model.Edge) []model.Edge {
	adjacency := make(map[string][]model.Edge)
	for _, edge := range edges {
		consumer, provider := edge.Consumer.ID(), edge.Provider.ID()
		if !selected[consumer] || !selected[provider] || consumer == provider {
			continue
		}
		adjacency[consumer] = append(adjacency[consumer], edge)
	}
	for id := range adjacency {
		sort.Slice(adjacency[id], func(i, j int) bool { return adjacency[id][i].Provider.ID() < adjacency[id][j].Provider.ID() })
	}
	nodes := make([]string, 0, len(selected))
	for id := range selected {
		nodes = append(nodes, id)
	}
	sort.Strings(nodes)

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int)
	var path, cycle []model.Edge
	var visit func(string) bool
	visit = func(node string) bool {
		color[node] = gray
		for _, edge := range adjacency[node] {
			next := edge.Provider.ID()
			if color[next] == gray {
				cycle = append(cycle, edge)
				for i := len(path) - 1; i >= 0; i-- {
					cycle = append(cycle, path[i])
					if path[i].Consumer.ID() == next {
						break
					}
				}
				for i, j := 0, len(cycle)-1; i < j; i, j = i+1, j-1 {
					cycle[i], cycle[j] = cycle[j], cycle[i]
				}
				return true
			}
			if color[next] == white {
				path = append(path, edge)
				if visit(next) {
					return true
				}
				path = path[:len(path)-1]
			}
		}
		color[node] = black
		return false
	}
	for _, id := range nodes {
		if color[id] == white && visit(id) {
			return cycle
		}
	}
	return nil
}

func ConsumerClosure(seeds map[string]bool, edges []model.Edge) map[string]bool {
	return ConsumerClosureExcept(seeds, edges, nil)
}

func ConsumerClosureExcept(seeds map[string]bool, edges []model.Edge, excluded map[string]bool) map[string]bool {
	selected := make(map[string]bool, len(seeds))
	for id := range seeds {
		selected[id] = true
	}
	changed := true
	for changed {
		changed = false
		for _, edge := range edges {
			if excluded[edge.Consumer.ID()] || excluded[edge.Provider.ID()] {
				continue
			}
			if selected[edge.Provider.ID()] && !selected[edge.Consumer.ID()] {
				selected[edge.Consumer.ID()] = true
				changed = true
			}
		}
	}
	return selected
}

func Waves(selected map[string]bool, edges []model.Edge) ([][]string, error) {
	indegree := make(map[string]int, len(selected))
	outgoing := make(map[string][]string, len(selected))
	for id := range selected {
		indegree[id] = 0
	}
	for _, edge := range edges {
		consumer := edge.Consumer.ID()
		provider := edge.Provider.ID()
		if !selected[consumer] || !selected[provider] || consumer == provider {
			continue
		}
		outgoing[consumer] = append(outgoing[consumer], provider)
		indegree[provider]++
	}

	remaining := len(selected)
	var waves [][]string
	for remaining > 0 {
		var wave []string
		for id, degree := range indegree {
			if degree == 0 {
				wave = append(wave, id)
			}
		}
		if len(wave) == 0 {
			return nil, &CycleError{Cycle: FindCycle(selected, edges)}
		}
		sort.Strings(wave)
		waves = append(waves, wave)
		for _, id := range wave {
			for _, target := range outgoing[id] {
				indegree[target]--
			}
			delete(indegree, id)
			remaining--
		}
	}
	return waves, nil
}
