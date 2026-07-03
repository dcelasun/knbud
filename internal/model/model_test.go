package model

import "testing"

func TestDependencyCandidatesAggregateEvidence(t *testing.T) {
	consumer := Ref{Kind: KindDeployment, Namespace: "app", Name: "consumer"}
	provider := Ref{Kind: KindDeployment, Namespace: "app", Name: "provider"}
	candidates := DependencyCandidates([]Suggestion{
		{Consumer: consumer, Targets: []Ref{provider}, Evidence: "one.example", Reason: "ingress"},
		{Consumer: consumer, Targets: []Ref{provider}, Evidence: "two.example", Reason: "ingress"},
	})
	if len(candidates) != 1 || len(candidates[0].Evidence) != 2 {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}
}
