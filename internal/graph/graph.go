package graph

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dcelasun/knbud/internal/model"
)

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
			var cycle []string
			for id := range indegree {
				cycle = append(cycle, id)
			}
			sort.Strings(cycle)
			return nil, fmt.Errorf("dependency cycle among: %s", strings.Join(cycle, ", "))
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
