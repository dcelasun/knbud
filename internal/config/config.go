package config

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/dcelasun/knbud/internal/model"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type Config struct {
	Version        int                `yaml:"version"`
	StorageClasses []string           `yaml:"storageClasses"`
	Groups         map[string]Group   `yaml:"groups"`
	Dependencies   []Dependency       `yaml:"dependencies"`
	Include        []ResourceSelector `yaml:"include"`
	Exclude        []ResourceSelector `yaml:"exclude"`
	Inference      Inference          `yaml:"inference"`
	GitOps         GitOps             `yaml:"gitOps"`
}

type GitOps struct {
	Flux   GitOpsProvider `yaml:"flux"`
	ArgoCD GitOpsProvider `yaml:"argoCD"`
}

type GitOpsProvider struct {
	Enabled   bool             `yaml:"enabled"`
	Mode      string           `yaml:"mode"`
	Resources []GitOpsResource `yaml:"resources"`
}

type GitOpsResource struct {
	Kind      string `yaml:"kind"`
	Namespace string `yaml:"namespace"`
	Name      string `yaml:"name"`
}

type Inference struct {
	ServiceReferences *bool               `yaml:"serviceReferences"`
	Ignore            []IgnoredDependency `yaml:"ignore"`
}

type IgnoredDependency struct {
	Consumer ResourceSelector `yaml:"consumer"`
	Provider ResourceSelector `yaml:"provider"`
}

func (i Inference) ServiceReferencesEnabled() bool {
	return i.ServiceReferences == nil || *i.ServiceReferences
}

type Group struct {
	Resources []ResourceSelector `yaml:"resources"`
	Selectors []ResourceSelector `yaml:"selectors"`
}

type Dependency struct {
	Consumer string `yaml:"consumer"`
	Provider string `yaml:"provider"`
}

type ResourceSelector struct {
	Kind          string            `yaml:"kind"`
	Namespace     string            `yaml:"namespace"`
	Name          string            `yaml:"name"`
	MatchLabels   map[string]string `yaml:"matchLabels"`
	LabelSelector string            `yaml:"labelSelector"`
}

func Load(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()
	return Decode(file)
}

func Decode(reader io.Reader) (*Config, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Version != 1 {
		return nil, fmt.Errorf("unsupported config version %d", cfg.Version)
	}
	if len(cfg.StorageClasses) == 0 {
		return nil, fmt.Errorf("storageClasses must not be empty")
	}
	for name, group := range cfg.Groups {
		if name == "" || len(group.Resources)+len(group.Selectors) == 0 {
			return nil, fmt.Errorf("group %q must contain resources or selectors", name)
		}
		for _, selector := range append(group.Resources, group.Selectors...) {
			if _, err := selector.Selector(); err != nil {
				return nil, fmt.Errorf("group %q: %w", name, err)
			}
		}
	}
	for _, dependency := range cfg.Dependencies {
		if _, ok := cfg.Groups[dependency.Consumer]; !ok {
			return nil, fmt.Errorf("dependency consumer group %q does not exist", dependency.Consumer)
		}
		if _, ok := cfg.Groups[dependency.Provider]; !ok {
			return nil, fmt.Errorf("dependency provider group %q does not exist", dependency.Provider)
		}
	}
	for _, selector := range append(cfg.Include, cfg.Exclude...) {
		if _, err := selector.Selector(); err != nil {
			return nil, err
		}
	}
	for _, ignored := range cfg.Inference.Ignore {
		if _, err := ignored.Consumer.Selector(); err != nil {
			return nil, fmt.Errorf("inference ignore consumer: %w", err)
		}
		if _, err := ignored.Provider.Selector(); err != nil {
			return nil, fmt.Errorf("inference ignore provider: %w", err)
		}
	}
	if err := validateGitOpsProvider(model.ProviderFlux, cfg.GitOps.Flux); err != nil {
		return nil, err
	}
	if err := validateGitOpsProvider(model.ProviderArgoCD, cfg.GitOps.ArgoCD); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validateGitOpsProvider(provider string, value GitOpsProvider) error {
	field := provider
	if provider == model.ProviderArgoCD {
		field = "argoCD"
	}
	if !value.Enabled {
		if value.Mode != "" || len(value.Resources) > 0 {
			return fmt.Errorf("gitOps.%s must be enabled when mode or resources are configured", field)
		}
		return nil
	}
	if value.Mode != "auto" && value.Mode != "explicit" {
		return fmt.Errorf("gitOps.%s mode must be auto or explicit", field)
	}
	if provider == model.ProviderArgoCD && value.Mode != "explicit" {
		return fmt.Errorf("gitOps.argoCD supports only explicit mode")
	}
	if value.Mode == "explicit" && len(value.Resources) == 0 {
		return fmt.Errorf("gitOps.%s explicit mode requires resources", field)
	}
	for _, resource := range value.Resources {
		if resource.Namespace == "" || resource.Name == "" {
			return fmt.Errorf("gitOps.%s resource namespace and name must not be empty", field)
		}
		valid := provider == model.ProviderFlux && (resource.Kind == model.KindKustomization || resource.Kind == model.KindHelmRelease) ||
			provider == model.ProviderArgoCD && resource.Kind == model.KindApplication
		if !valid {
			return fmt.Errorf("unsupported gitOps.%s resource kind %q", field, resource.Kind)
		}
	}
	return nil
}

func (r ResourceSelector) Selector() (labels.Selector, error) {
	if r.Kind == "" {
		return nil, fmt.Errorf("resource selector kind must not be empty")
	}
	if _, err := model.ParseKind(r.Kind); err != nil {
		return nil, err
	}
	if r.Name != "" && (len(r.MatchLabels) > 0 || r.LabelSelector != "") {
		return nil, fmt.Errorf("selector for %s cannot combine name and labels", r.Name)
	}
	selector := labels.Everything()
	if len(r.MatchLabels) > 0 {
		selector = labels.SelectorFromSet(r.MatchLabels)
	}
	if r.LabelSelector != "" {
		parsed, err := metav1.ParseToLabelSelector(r.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid labelSelector %q: %w", r.LabelSelector, err)
		}
		selector, err = metav1.LabelSelectorAsSelector(parsed)
		if err != nil {
			return nil, fmt.Errorf("invalid labelSelector %q: %w", r.LabelSelector, err)
		}
	}
	return selector, nil
}

func (r ResourceSelector) Matches(workload *model.Workload) bool {
	kind, err := model.ParseKind(r.Kind)
	if err != nil || workload.Ref.Kind != kind {
		return false
	}
	if r.Namespace != "" && workload.Ref.Namespace != r.Namespace {
		return false
	}
	if r.Name != "" {
		return workload.Ref.Name == r.Name
	}
	selector, err := r.Selector()
	return err == nil && selector.Matches(labels.Set(workload.Labels))
}

func Resolve(selectors []ResourceSelector, workloads map[string]*model.Workload) []model.Ref {
	seen := make(map[string]model.Ref)
	for _, workload := range workloads {
		for _, selector := range selectors {
			if selector.Matches(workload) {
				seen[workload.Ref.ID()] = workload.Ref
			}
		}
	}
	refs := make([]model.Ref, 0, len(seen))
	for _, ref := range seen {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID() < refs[j].ID() })
	return refs
}
