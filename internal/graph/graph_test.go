package graph

import (
	"strings"
	"testing"

	"github.com/dcelasun/knbud/internal/model"
)

func ref(name string) model.Ref {
	return model.Ref{Kind: model.KindDeployment, Namespace: "test", Name: name}
}

func TestConsumerClosureAndWaves(t *testing.T) {
	edges := []model.Edge{
		{Consumer: ref("frontend"), Provider: ref("api")},
		{Consumer: ref("api"), Provider: ref("store")},
	}
	selected := ConsumerClosure(map[string]bool{ref("store").ID(): true}, edges)
	if len(selected) != 3 {
		t.Fatalf("expected 3 selected nodes, got %d", len(selected))
	}
	waves, err := Waves(selected, edges)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join([]string{waves[0][0], waves[1][0], waves[2][0]}, ","); got != strings.Join([]string{ref("frontend").ID(), ref("api").ID(), ref("store").ID()}, ",") {
		t.Fatalf("unexpected order: %s", got)
	}
}

func TestWavesRejectsCycle(t *testing.T) {
	edges := []model.Edge{{Consumer: ref("one"), Provider: ref("two")}, {Consumer: ref("two"), Provider: ref("one")}}
	selected := map[string]bool{ref("one").ID(): true, ref("two").ID(): true}
	if _, err := Waves(selected, edges); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestFindCycle(t *testing.T) {
	edges := []model.Edge{
		{Consumer: ref("a"), Provider: ref("b")},
		{Consumer: ref("b"), Provider: ref("c")},
		{Consumer: ref("c"), Provider: ref("a")},
	}
	selected := map[string]bool{ref("a").ID(): true, ref("b").ID(): true, ref("c").ID(): true}
	cycle := FindCycle(selected, edges)
	if len(cycle) != 3 {
		t.Fatalf("expected a 3-edge cycle, got %#v", cycle)
	}
	seen := map[string]bool{}
	for _, edge := range cycle {
		seen[edge.ID()] = true
	}
	for _, want := range []string{ref("a").ID() + "->" + ref("b").ID(), ref("b").ID() + "->" + ref("c").ID(), ref("c").ID() + "->" + ref("a").ID()} {
		if !seen[want] {
			t.Fatalf("cycle missing edge %s: %#v", want, cycle)
		}
	}
}

func TestFindCycleAcyclic(t *testing.T) {
	edges := []model.Edge{{Consumer: ref("a"), Provider: ref("b")}, {Consumer: ref("b"), Provider: ref("c")}}
	selected := map[string]bool{ref("a").ID(): true, ref("b").ID(): true, ref("c").ID(): true}
	if cycle := FindCycle(selected, edges); cycle != nil {
		t.Fatalf("expected no cycle, got %#v", cycle)
	}
}
