package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dcelasun/knbud/internal/model"
	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type Config struct {
	Version            int                `yaml:"version"`
	StorageClasses     []string           `yaml:"storageClasses"`
	Groups             map[string]Group   `yaml:"groups,omitempty"`
	Dependencies       []Dependency       `yaml:"dependencies,omitempty"`
	CustomGroups       map[string]Group   `yaml:"customGroups,omitempty"`
	CustomDependencies []Dependency       `yaml:"customDependencies,omitempty"`
	Include            []ResourceSelector `yaml:"include,omitempty"`
	Exclude            []ResourceSelector `yaml:"exclude,omitempty"`
	Inference          Inference          `yaml:"inference,omitempty"`
	GitOps             GitOps             `yaml:"gitOps,omitempty"`
}

// EffectiveGroups merges the hand-authored customGroups over the discovered
// groups. discover manages the latter; custom entries win on a name collision.
func (c *Config) EffectiveGroups() map[string]Group {
	merged := make(map[string]Group, len(c.Groups)+len(c.CustomGroups))
	for name, group := range c.Groups {
		merged[name] = group
	}
	for name, group := range c.CustomGroups {
		merged[name] = group
	}
	return merged
}

// EffectiveDependencies concatenates discovered and hand-authored dependencies.
func (c *Config) EffectiveDependencies() []Dependency {
	return append(append([]Dependency{}, c.Dependencies...), c.CustomDependencies...)
}

func (c *Config) nameTaken(name string) bool {
	if _, ok := c.Groups[name]; ok {
		return true
	}
	_, ok := c.CustomGroups[name]
	return ok
}

type GitOps struct {
	Flux   GitOpsProvider `yaml:"flux,omitempty"`
	ArgoCD GitOpsProvider `yaml:"argoCD,omitempty"`
}

type GitOpsProvider struct {
	Enabled   bool             `yaml:"enabled,omitempty"`
	Mode      string           `yaml:"mode,omitempty"`
	Resources []GitOpsResource `yaml:"resources,omitempty"`
}

type GitOpsResource struct {
	Kind      string `yaml:"kind"`
	Namespace string `yaml:"namespace"`
	Name      string `yaml:"name"`
}

type Inference struct {
	ServiceReferences *bool               `yaml:"serviceReferences,omitempty"`
	Ignore            []IgnoredDependency `yaml:"ignore,omitempty"`
}

type IgnoredDependency struct {
	Consumer ResourceSelector `yaml:"consumer"`
	Provider ResourceSelector `yaml:"provider"`
}

func (i Inference) ServiceReferencesEnabled() bool {
	return i.ServiceReferences == nil || *i.ServiceReferences
}

type Group struct {
	Resources []ResourceSelector `yaml:"resources,omitempty"`
	Selectors []ResourceSelector `yaml:"selectors,omitempty"`
}

type Dependency struct {
	Consumer string `yaml:"consumer"`
	Provider string `yaml:"provider"`
}

type ResourceSelector struct {
	Kind          string            `yaml:"kind"`
	Namespace     string            `yaml:"namespace,omitempty"`
	Name          string            `yaml:"name,omitempty"`
	MatchLabels   map[string]string `yaml:"matchLabels,omitempty"`
	LabelSelector string            `yaml:"labelSelector,omitempty"`
}

func Load(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()
	return Decode(file)
}

func Create(path string, cfg *Config) error {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create config: %w", err)
	}
	if _, err := file.Write(raw); err != nil {
		file.Close()
		return fmt.Errorf("write config: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close config: %w", err)
	}
	return nil
}

func Write(path string, cfg *Config) error {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".knbud-*.yaml")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return fmt.Errorf("set temporary config permissions: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		temporary.Close()
		return fmt.Errorf("write temporary config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary config: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

func (c *Config) Accept(candidate model.DependencyCandidate) {
	c.AcceptDependency([]model.Ref{candidate.Consumer}, []model.Ref{candidate.Provider})
}

func (c *Config) AcceptDependency(consumers, providers []model.Ref) {
	consumer := c.ensureGroup(consumers)
	provider := c.ensureGroup(providers)
	c.Dependencies = addDependency(c.Dependencies, consumer, provider)
}

func (c *Config) AcceptCustomDependency(consumers, providers []model.Ref) {
	consumer := c.ensureCustomGroup(consumers)
	provider := c.ensureCustomGroup(providers)
	c.CustomDependencies = addDependency(c.CustomDependencies, consumer, provider)
}

func (c *Config) ResetDiscovered() {
	for _, dependency := range c.CustomDependencies {
		for _, name := range []string{dependency.Consumer, dependency.Provider} {
			if _, exists := c.CustomGroups[name]; exists {
				continue
			}
			if group, exists := c.Groups[name]; exists {
				if c.CustomGroups == nil {
					c.CustomGroups = make(map[string]Group)
				}
				c.CustomGroups[name] = group
			}
		}
	}
	c.Groups = nil
	c.Dependencies = nil
}

func addDependency(dependencies []Dependency, consumer, provider string) []Dependency {
	for _, dependency := range dependencies {
		if dependency.Consumer == consumer && dependency.Provider == provider {
			return dependencies
		}
	}
	dependencies = append(dependencies, Dependency{Consumer: consumer, Provider: provider})
	sort.Slice(dependencies, func(i, j int) bool {
		left := dependencies[i].Consumer + "->" + dependencies[i].Provider
		right := dependencies[j].Consumer + "->" + dependencies[j].Provider
		return left < right
	})
	return dependencies
}

func (c *Config) IncludeResources(refs []model.Ref) {
	for _, ref := range refs {
		selector := selectorFor(ref)
		found := false
		for _, existing := range c.Include {
			if sameSelector(existing, selector) {
				found = true
				break
			}
		}
		if !found {
			c.Include = append(c.Include, selector)
		}
	}
	sort.Slice(c.Include, func(i, j int) bool { return selectorID(c.Include[i]) < selectorID(c.Include[j]) })
}

func (c *Config) Ignore(candidate model.DependencyCandidate) {
	rule := IgnoredDependency{Consumer: selectorFor(candidate.Consumer), Provider: selectorFor(candidate.Provider)}
	for _, existing := range c.Inference.Ignore {
		if sameSelector(existing.Consumer, rule.Consumer) && sameSelector(existing.Provider, rule.Provider) {
			return
		}
	}
	c.Inference.Ignore = append(c.Inference.Ignore, rule)
	sort.Slice(c.Inference.Ignore, func(i, j int) bool {
		left := selectorID(c.Inference.Ignore[i].Consumer) + "->" + selectorID(c.Inference.Ignore[i].Provider)
		right := selectorID(c.Inference.Ignore[j].Consumer) + "->" + selectorID(c.Inference.Ignore[j].Provider)
		return left < right
	})
}

func (c *Config) ensureGroup(refs []model.Ref) string {
	refs = append([]model.Ref{}, refs...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID() < refs[j].ID() })
	selectors := make([]ResourceSelector, 0, len(refs))
	for _, ref := range refs {
		selectors = append(selectors, selectorFor(ref))
	}
	for name, group := range c.Groups {
		if sameResources(group, selectors) {
			return name
		}
	}
	if c.Groups == nil {
		c.Groups = make(map[string]Group)
	}
	name := c.uniqueGroupName(refs, selectors)
	c.Groups[name] = Group{Resources: selectors}
	return name
}

func (c *Config) ensureCustomGroup(refs []model.Ref) string {
	refs = append([]model.Ref{}, refs...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID() < refs[j].ID() })
	selectors := make([]ResourceSelector, 0, len(refs))
	for _, ref := range refs {
		selectors = append(selectors, selectorFor(ref))
	}
	for name, group := range c.CustomGroups {
		if sameResources(group, selectors) {
			return name
		}
	}
	if c.CustomGroups == nil {
		c.CustomGroups = make(map[string]Group)
	}
	name := c.uniqueGroupName(refs, selectors)
	c.CustomGroups[name] = Group{Resources: selectors}
	return name
}

// uniqueGroupName picks a readable, collision-free name for a new group. A
// single-workload group is named after the workload, qualified with its
// namespace and kind only when a plainer name is already taken. Multi-workload
// groups fall back to a deterministic hashed name.
func (c *Config) uniqueGroupName(refs []model.Ref, selectors []ResourceSelector) string {
	identity := make([]string, 0, len(refs))
	for _, ref := range refs {
		identity = append(identity, ref.ID())
	}
	suffix := func() string {
		sum := sha256.Sum256([]byte(strings.Join(identity, ",")))
		return hex.EncodeToString(sum[:4])
	}
	var candidates []string
	if len(refs) == 1 {
		ref := refs[0]
		candidates = []string{
			ref.Name,
			ref.Namespace + "-" + ref.Name,
			strings.ToLower(ref.Kind) + "-" + ref.Namespace + "-" + ref.Name,
		}
	} else {
		candidates = []string{"group-" + suffix()}
	}
	for _, candidate := range candidates {
		candidate = clampGroupName(strings.ToLower(candidate))
		if !c.nameTaken(candidate) {
			return candidate
		}
	}
	return clampGroupName(strings.ToLower(candidates[0])) + "-" + suffix()
}

func clampGroupName(name string) string {
	if len(name) > 63 {
		return name[:63]
	}
	return name
}

func sameResources(group Group, selectors []ResourceSelector) bool {
	if len(group.Selectors) != 0 || len(group.Resources) != len(selectors) {
		return false
	}
	existing := append([]ResourceSelector{}, group.Resources...)
	sort.Slice(existing, func(i, j int) bool { return selectorID(existing[i]) < selectorID(existing[j]) })
	for index := range selectors {
		if !sameSelector(existing[index], selectors[index]) {
			return false
		}
	}
	return true
}

func selectorFor(ref model.Ref) ResourceSelector {
	return ResourceSelector{Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

func sameSelector(left, right ResourceSelector) bool {
	return left.Kind == right.Kind && left.Namespace == right.Namespace && left.Name == right.Name &&
		len(left.MatchLabels) == 0 && len(right.MatchLabels) == 0 && left.LabelSelector == "" && right.LabelSelector == ""
}

func selectorID(selector ResourceSelector) string {
	return selector.Kind + "/" + selector.Namespace + "/" + selector.Name
}

func validateGroups(label string, groups map[string]Group) error {
	for name, group := range groups {
		if name == "" || len(group.Resources)+len(group.Selectors) == 0 {
			return fmt.Errorf("%s %q must contain resources or selectors", label, name)
		}
		for _, selector := range append(group.Resources, group.Selectors...) {
			if _, err := selector.Selector(); err != nil {
				return fmt.Errorf("%s %q: %w", label, name, err)
			}
		}
	}
	return nil
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
	if err := validateGroups("group", cfg.Groups); err != nil {
		return nil, err
	}
	if err := validateGroups("custom group", cfg.CustomGroups); err != nil {
		return nil, err
	}
	for _, dependency := range cfg.Dependencies {
		if _, ok := cfg.Groups[dependency.Consumer]; !ok {
			return nil, fmt.Errorf("dependency consumer group %q does not exist", dependency.Consumer)
		}
		if _, ok := cfg.Groups[dependency.Provider]; !ok {
			return nil, fmt.Errorf("dependency provider group %q does not exist", dependency.Provider)
		}
	}
	merged := cfg.EffectiveGroups()
	for _, dependency := range cfg.CustomDependencies {
		if _, ok := merged[dependency.Consumer]; !ok {
			return nil, fmt.Errorf("custom dependency consumer group %q does not exist", dependency.Consumer)
		}
		if _, ok := merged[dependency.Provider]; !ok {
			return nil, fmt.Errorf("custom dependency provider group %q does not exist", dependency.Provider)
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
