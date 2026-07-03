package discovery

import (
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/kube"
	"github.com/dcelasun/knbud/internal/model"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

var (
	urlPattern = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://[^\s"'<>]+`)
	dnsPattern = regexp.MustCompile(`(?i)\b[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?){1,}(?::[0-9]+)?\b`)
)

type Result struct {
	Inventory        *model.Inventory
	Groups           map[string][]model.Ref
	Included         map[string]bool
	Excluded         map[string]bool
	UnresolvedGroups []string
	HPAs             map[string]string
	Jobs             []batchv1.Job
	GitOps           config.GitOps
}

func Build(snapshot *kube.Snapshot, cfg *config.Config) (*Result, error) {
	storageClasses := make(map[string]bool, len(cfg.StorageClasses))
	for _, name := range cfg.StorageClasses {
		storageClasses[name] = true
	}
	pvNFS := make(map[string]bool, len(snapshot.PVs))
	for _, pv := range snapshot.PVs {
		pvNFS[pv.Name] = pv.Spec.NFS != nil || pv.Spec.CSI != nil && pv.Spec.CSI.Driver == "nfs.csi.k8s.io"
	}
	pvcNFS := make(map[string]bool, len(snapshot.PVCs))
	for _, pvc := range snapshot.PVCs {
		pvcNFS[pvc.Namespace+"/"+pvc.Name] = storageClasses[lo.FromPtr(pvc.Spec.StorageClassName)] || pvNFS[pvc.Spec.VolumeName]
	}

	workloads := make(map[string]*model.Workload)
	for i := range snapshot.Deployments {
		deployment := &snapshot.Deployments[i]
		addDeployment(workloads, deployment, pvcNFS)
	}
	for i := range snapshot.StatefulSets {
		statefulSet := &snapshot.StatefulSets[i]
		addStatefulSet(workloads, statefulSet, pvcNFS, storageClasses)
	}
	for i := range snapshot.CronJobs {
		cronJob := &snapshot.CronJobs[i]
		addCronJob(workloads, cronJob, pvcNFS)
	}
	excluded := make(map[string]bool)
	for id, workload := range workloads {
		for _, selector := range cfg.Exclude {
			if selector.Matches(workload) {
				excluded[id] = true
				break
			}
		}
	}

	groups := make(map[string][]model.Ref, len(cfg.Groups))
	var unresolvedGroups []string
	for name, group := range cfg.Groups {
		selectors := append(append([]config.ResourceSelector{}, group.Resources...), group.Selectors...)
		groups[name] = config.Resolve(selectors, workloads)
		if len(groups[name]) == 0 {
			unresolvedGroups = append(unresolvedGroups, name)
		}
	}
	sort.Strings(unresolvedGroups)

	edges := explicitEdges(cfg.Dependencies, groups)
	suggestions := make([]model.Suggestion, 0)
	if cfg.Inference.ServiceReferencesEnabled() {
		inferred, ambiguous := inferServiceEdges(snapshot, workloads)
		inferred = filterIgnored(inferred, cfg.Inference.Ignore, workloads)
		edges = append(edges, inferred...)
		suggestions = append(suggestions, ambiguous...)
	}
	suggestions = append(suggestions, inferIngressSuggestions(snapshot, workloads)...)
	edges = deduplicateEdges(edges)

	included := make(map[string]bool)
	for _, workload := range workloads {
		if excluded[workload.Ref.ID()] {
			continue
		}
		if workload.DirectNFS {
			included[workload.Ref.ID()] = true
		}
		for _, selector := range cfg.Include {
			if selector.Matches(workload) {
				included[workload.Ref.ID()] = true
			}
		}
	}

	hpas := make(map[string]string)
	for _, hpa := range snapshot.HPAs {
		kind, err := model.ParseKind(hpa.Spec.ScaleTargetRef.Kind)
		if err != nil {
			continue
		}
		ref := model.Ref{Kind: kind, Namespace: hpa.Namespace, Name: hpa.Spec.ScaleTargetRef.Name}
		hpas[ref.ID()] = hpa.Namespace + "/" + hpa.Name
	}
	gitOpsResources, gitOpsOwnership := discoverGitOps(snapshot, workloads)
	return &Result{
		Inventory: &model.Inventory{
			Workloads: workloads, Edges: edges, Suggestions: suggestions,
			GitOpsResources: gitOpsResources, GitOpsOwnership: gitOpsOwnership,
		},
		Groups: groups, Included: included, Excluded: excluded, UnresolvedGroups: unresolvedGroups,
		HPAs: hpas, Jobs: snapshot.Jobs, GitOps: cfg.GitOps,
	}, nil
}

func discoverGitOps(snapshot *kube.Snapshot, workloads map[string]*model.Workload) (map[string]*model.GitOpsResource, []model.GitOpsOwnership) {
	resources := make(map[string]*model.GitOpsResource)
	for i := range snapshot.Kustomizations {
		addGitOpsResource(resources, model.ProviderFlux, &snapshot.Kustomizations[i])
	}
	for i := range snapshot.HelmReleases {
		addGitOpsResource(resources, model.ProviderFlux, &snapshot.HelmReleases[i])
	}
	for i := range snapshot.Applications {
		addGitOpsResource(resources, model.ProviderArgoCD, &snapshot.Applications[i])
	}

	helmParents := make(map[string][]model.GitOpsRef)
	for i := range snapshot.HelmReleases {
		helmRelease := &snapshot.HelmReleases[i]
		if owner, ok := fluxOwner(helmRelease.GetLabels(), model.KindKustomization, resources); ok {
			helmParents[gitOpsID(model.ProviderFlux, model.KindHelmRelease, helmRelease.GetNamespace(), helmRelease.GetName())] = append(
				helmParents[gitOpsID(model.ProviderFlux, model.KindHelmRelease, helmRelease.GetNamespace(), helmRelease.GetName())], owner,
			)
		}
		entryID := helmRelease.GetNamespace() + "_" + helmRelease.GetName() + "_helm.toolkit.fluxcd.io_HelmRelease"
		for j := range snapshot.Kustomizations {
			entries, _, _ := unstructured.NestedSlice(snapshot.Kustomizations[j].Object, "status", "inventory", "entries")
			for _, raw := range entries {
				entry, ok := raw.(map[string]any)
				if ok && entry["id"] == entryID {
					ref := gitOpsRef(model.ProviderFlux, &snapshot.Kustomizations[j])
					helmParents[gitOpsID(model.ProviderFlux, model.KindHelmRelease, helmRelease.GetNamespace(), helmRelease.GetName())] = append(
						helmParents[gitOpsID(model.ProviderFlux, model.KindHelmRelease, helmRelease.GetNamespace(), helmRelease.GetName())], ref,
					)
				}
			}
		}
	}

	applicationsByName := make(map[string][]model.GitOpsRef)
	for i := range snapshot.Applications {
		ref := gitOpsRef(model.ProviderArgoCD, &snapshot.Applications[i])
		applicationsByName[ref.Name] = append(applicationsByName[ref.Name], ref)
	}
	seen := make(map[string]bool)
	var ownership []model.GitOpsOwnership
	add := func(workload model.Ref, owner model.GitOpsRef) {
		if resources[owner.ID()] == nil {
			return
		}
		id := workload.ID() + "->" + owner.ID()
		if !seen[id] {
			seen[id] = true
			ownership = append(ownership, model.GitOpsOwnership{Workload: workload, Owner: owner})
		}
	}
	for _, workload := range workloads {
		if owner, ok := fluxOwner(workload.Labels, model.KindKustomization, resources); ok {
			add(workload.Ref, owner)
		}
		if owner, ok := fluxOwner(workload.Labels, model.KindHelmRelease, resources); ok {
			add(workload.Ref, owner)
			for _, parent := range helmParents[owner.ID()] {
				add(workload.Ref, parent)
			}
		}
		application := workload.Labels["argocd.argoproj.io/instance"]
		if tracking := workload.Annotations["argocd.argoproj.io/tracking-id"]; tracking != "" {
			application = strings.SplitN(tracking, ":", 2)[0]
		}
		for _, owner := range applicationsByName[application] {
			add(workload.Ref, owner)
		}
	}
	sort.Slice(ownership, func(i, j int) bool {
		left := ownership[i].Workload.ID() + ownership[i].Owner.ID()
		right := ownership[j].Workload.ID() + ownership[j].Owner.ID()
		return left < right
	})
	return resources, ownership
}

func addGitOpsResource(target map[string]*model.GitOpsResource, provider string, item *unstructured.Unstructured) {
	ref := gitOpsRef(provider, item)
	suspended := false
	if provider == model.ProviderFlux {
		suspended, _, _ = unstructured.NestedBool(item.Object, "spec", "suspend")
	} else {
		suspended = strings.EqualFold(item.GetAnnotations()["argocd.argoproj.io/skip-reconcile"], "true")
	}
	target[ref.ID()] = &model.GitOpsResource{Ref: ref, Annotations: item.GetAnnotations(), Suspended: suspended}
}

func gitOpsRef(provider string, item *unstructured.Unstructured) model.GitOpsRef {
	return model.GitOpsRef{
		Provider: provider, APIVersion: item.GetAPIVersion(), Kind: item.GetKind(),
		Namespace: item.GetNamespace(), Name: item.GetName(),
	}
}

func gitOpsID(provider, kind, namespace, name string) string {
	return model.GitOpsRef{Provider: provider, Kind: kind, Namespace: namespace, Name: name}.ID()
}

func fluxOwner(labels map[string]string, kind string, resources map[string]*model.GitOpsResource) (model.GitOpsRef, bool) {
	prefix := "kustomize.toolkit.fluxcd.io"
	if kind == model.KindHelmRelease {
		prefix = "helm.toolkit.fluxcd.io"
	}
	name := labels[prefix+"/name"]
	namespace := labels[prefix+"/namespace"]
	resource := resources[gitOpsID(model.ProviderFlux, kind, namespace, name)]
	if name == "" || namespace == "" || resource == nil {
		return model.GitOpsRef{}, false
	}
	return resource.Ref, true
}

func filterIgnored(edges []model.Edge, ignored []config.IgnoredDependency, workloads map[string]*model.Workload) []model.Edge {
	result := edges[:0]
	for _, edge := range edges {
		skip := false
		for _, rule := range ignored {
			if rule.Consumer.Matches(workloads[edge.Consumer.ID()]) && rule.Provider.Matches(workloads[edge.Provider.ID()]) {
				skip = true
				break
			}
		}
		if !skip {
			result = append(result, edge)
		}
	}
	return result
}

func addDeployment(target map[string]*model.Workload, item *appsv1.Deployment, pvcNFS map[string]bool) {
	replicas := lo.FromPtrOr(item.Spec.Replicas, int32(1))
	workload := &model.Workload{
		Ref:       model.Ref{Kind: model.KindDeployment, Namespace: item.Namespace, Name: item.Name},
		Labels:    item.Labels,
		PodLabels: item.Spec.Template.Labels, Annotations: item.Annotations, PodSpec: &item.Spec.Template.Spec,
		Replicas: replicas, UID: string(item.UID),
	}
	workload.DirectNFS = podUsesNFS(item.Namespace, workload.PodSpec, pvcNFS)
	target[workload.Ref.ID()] = workload
}

func addStatefulSet(target map[string]*model.Workload, item *appsv1.StatefulSet, pvcNFS map[string]bool, nfs map[string]bool) {
	replicas := lo.FromPtrOr(item.Spec.Replicas, int32(1))
	workload := &model.Workload{
		Ref:       model.Ref{Kind: model.KindStatefulSet, Namespace: item.Namespace, Name: item.Name},
		Labels:    item.Labels,
		PodLabels: item.Spec.Template.Labels, Annotations: item.Annotations, PodSpec: &item.Spec.Template.Spec,
		Replicas: replicas, UID: string(item.UID),
	}
	workload.DirectNFS = podUsesNFS(item.Namespace, workload.PodSpec, pvcNFS)
	for _, claim := range item.Spec.VolumeClaimTemplates {
		if nfs[lo.FromPtr(claim.Spec.StorageClassName)] {
			workload.DirectNFS = true
		}
	}
	target[workload.Ref.ID()] = workload
}

func addCronJob(target map[string]*model.Workload, item *batchv1.CronJob, pvcNFS map[string]bool) {
	suspended := lo.FromPtr(item.Spec.Suspend)
	workload := &model.Workload{
		Ref:       model.Ref{Kind: model.KindCronJob, Namespace: item.Namespace, Name: item.Name},
		Labels:    item.Labels,
		PodLabels: item.Spec.JobTemplate.Spec.Template.Labels, Annotations: item.Annotations,
		PodSpec: &item.Spec.JobTemplate.Spec.Template.Spec, Suspended: suspended, UID: string(item.UID),
	}
	workload.DirectNFS = podUsesNFS(item.Namespace, workload.PodSpec, pvcNFS)
	target[workload.Ref.ID()] = workload
}

func podUsesNFS(namespace string, spec *corev1.PodSpec, pvcNFS map[string]bool) bool {
	for _, volume := range spec.Volumes {
		if volume.NFS != nil || volume.CSI != nil && volume.CSI.Driver == "nfs.csi.k8s.io" {
			return true
		}
		if volume.PersistentVolumeClaim != nil && pvcNFS[namespace+"/"+volume.PersistentVolumeClaim.ClaimName] {
			return true
		}
	}
	return false
}

func explicitEdges(dependencies []config.Dependency, groups map[string][]model.Ref) []model.Edge {
	var edges []model.Edge
	for _, dependency := range dependencies {
		for _, consumer := range groups[dependency.Consumer] {
			for _, provider := range groups[dependency.Provider] {
				if consumer.ID() == provider.ID() {
					continue
				}
				edges = append(edges, model.Edge{
					Consumer: consumer, Provider: provider, Type: model.EvidenceExplicit,
					Evidence: dependency.Consumer + " depends on " + dependency.Provider,
				})
			}
		}
	}
	return edges
}

func inferServiceEdges(snapshot *kube.Snapshot, workloads map[string]*model.Workload) ([]model.Edge, []model.Suggestion) {
	configMaps := make(map[string]corev1.ConfigMap, len(snapshot.ConfigMaps))
	for _, item := range snapshot.ConfigMaps {
		configMaps[item.Namespace+"/"+item.Name] = item
	}
	var edges []model.Edge
	var suggestions []model.Suggestion
	for _, consumer := range workloads {
		stringsToScan := podStrings(consumer.PodSpec, consumer.Ref.Namespace, configMaps)
		for _, service := range snapshot.Services {
			if service.Namespace != consumer.Ref.Namespace || len(service.Spec.Selector) == 0 {
				continue
			}
			evidence := referencedService(stringsToScan, service.Name, service.Namespace)
			if evidence == "" {
				continue
			}
			providers := serviceProviders(service, workloads)
			providers = removeRef(providers, consumer.Ref)
			switch len(providers) {
			case 0:
				suggestions = append(suggestions, model.Suggestion{Consumer: consumer.Ref, Evidence: evidence, Reason: "service has no scalable workload"})
			case 1:
				edges = append(edges, model.Edge{Consumer: consumer.Ref, Provider: providers[0], Type: model.EvidenceService, Evidence: evidence})
			default:
				suggestions = append(suggestions, model.Suggestion{Consumer: consumer.Ref, Targets: providers, Evidence: evidence, Reason: "service selector matches multiple workloads"})
			}
		}
	}
	return edges, suggestions
}

func serviceProviders(service corev1.Service, workloads map[string]*model.Workload) []model.Ref {
	selector := labels.SelectorFromSet(service.Spec.Selector)
	var refs []model.Ref
	for _, workload := range workloads {
		if workload.Ref.Namespace == service.Namespace && selector.Matches(labels.Set(workload.PodLabels)) {
			refs = append(refs, workload.Ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID() < refs[j].ID() })
	return refs
}

func podStrings(spec *corev1.PodSpec, namespace string, configMaps map[string]corev1.ConfigMap) []string {
	seen := make(map[string]bool)
	addContainer := func(container corev1.Container) {
		for _, value := range append(container.Command, container.Args...) {
			seen[value] = true
		}
		for _, env := range container.Env {
			if env.Value != "" {
				seen[env.Value] = true
			}
			if env.ValueFrom != nil && env.ValueFrom.ConfigMapKeyRef != nil {
				ref := env.ValueFrom.ConfigMapKeyRef
				if item, ok := configMaps[namespace+"/"+ref.Name]; ok {
					seen[item.Data[ref.Key]] = true
				}
			}
		}
		for _, from := range container.EnvFrom {
			if from.ConfigMapRef != nil {
				if item, ok := configMaps[namespace+"/"+from.ConfigMapRef.Name]; ok {
					for _, value := range item.Data {
						seen[value] = true
					}
				}
			}
		}
	}
	for _, container := range spec.InitContainers {
		addContainer(container)
	}
	for _, container := range spec.Containers {
		addContainer(container)
	}
	for _, volume := range spec.Volumes {
		if volume.ConfigMap != nil {
			if item, ok := configMaps[namespace+"/"+volume.ConfigMap.Name]; ok {
				for _, value := range item.Data {
					seen[value] = true
				}
			}
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	return result
}

func referencedService(values []string, name, namespace string) string {
	for _, value := range values {
		for _, raw := range urlPattern.FindAllString(value, -1) {
			parsed, err := url.Parse(strings.TrimRight(raw, ".,);]"))
			if err == nil && serviceHost(parsed.Hostname(), name, namespace) {
				return parsed.Scheme + "://" + parsed.Host
			}
		}
		for _, raw := range dnsPattern.FindAllString(value, -1) {
			host := strings.Split(raw, ":")[0]
			if serviceHost(host, name, namespace) {
				return raw
			}
		}
		trimmed := strings.TrimSpace(value)
		host := strings.Split(trimmed, ":")[0]
		if serviceHost(host, name, namespace) {
			return trimmed
		}
	}
	return ""
}

func serviceHost(host, name, namespace string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return host == strings.ToLower(name) ||
		host == strings.ToLower(name+"."+namespace) ||
		host == strings.ToLower(name+"."+namespace+".svc") ||
		host == strings.ToLower(name+"."+namespace+".svc.cluster.local")
}

func inferIngressSuggestions(snapshot *kube.Snapshot, workloads map[string]*model.Workload) []model.Suggestion {
	configMaps := make(map[string]corev1.ConfigMap, len(snapshot.ConfigMaps))
	for _, item := range snapshot.ConfigMaps {
		configMaps[item.Namespace+"/"+item.Name] = item
	}
	services := make(map[string]corev1.Service, len(snapshot.Services))
	for _, service := range snapshot.Services {
		services[service.Namespace+"/"+service.Name] = service
	}
	var suggestions []model.Suggestion
	for _, consumer := range workloads {
		values := podStrings(consumer.PodSpec, consumer.Ref.Namespace, configMaps)
		for _, ingress := range snapshot.Ingresses {
			for _, rule := range ingress.Spec.Rules {
				if rule.Host == "" || !containsHost(values, rule.Host) || rule.HTTP == nil {
					continue
				}
				var targets []model.Ref
				for _, path := range rule.HTTP.Paths {
					service, ok := services[ingress.Namespace+"/"+path.Backend.Service.Name]
					if ok {
						targets = append(targets, serviceProviders(service, workloads)...)
					}
				}
				targets = uniqueRefs(removeRef(targets, consumer.Ref))
				if len(targets) > 0 {
					suggestions = append(suggestions, model.Suggestion{Consumer: consumer.Ref, Targets: targets, Evidence: rule.Host, Reason: "ingress hostname reference requires explicit acceptance"})
				}
			}
		}
	}
	return suggestions
}

func containsHost(values []string, host string) bool {
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), strings.ToLower(host)) {
			return true
		}
	}
	return false
}

func removeRef(refs []model.Ref, excluded model.Ref) []model.Ref {
	result := refs[:0]
	for _, ref := range refs {
		if ref.ID() != excluded.ID() {
			result = append(result, ref)
		}
	}
	return result
}

func uniqueRefs(refs []model.Ref) []model.Ref {
	seen := make(map[string]model.Ref)
	for _, ref := range refs {
		seen[ref.ID()] = ref
	}
	result := make([]model.Ref, 0, len(seen))
	for _, ref := range seen {
		result = append(result, ref)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID() < result[j].ID() })
	return result
}

func deduplicateEdges(edges []model.Edge) []model.Edge {
	seen := make(map[string]model.Edge)
	for _, edge := range edges {
		if existing, ok := seen[edge.ID()]; !ok || edge.Type == model.EvidenceExplicit || existing.Type != model.EvidenceExplicit {
			seen[edge.ID()] = edge
		}
	}
	result := make([]model.Edge, 0, len(seen))
	for _, edge := range seen {
		result = append(result, edge)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID() < result[j].ID() })
	return result
}
