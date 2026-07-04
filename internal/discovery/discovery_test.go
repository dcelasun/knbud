package discovery

import (
	"strings"
	"testing"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/kube"
	"github.com/dcelasun/knbud/internal/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildDiscoversNFSAndServiceDependency(t *testing.T) {
	snapshot := &kube.Snapshot{
		PVCs:     []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: new("nfs")}}},
		Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "store"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "store"}}}},
		Deployments: []appsv1.Deployment{
			deployment("app", "store", map[string]string{"app": "store"}, corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}),
			deployment("app", "api", map[string]string{"app": "api"}, corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "STORE", Value: "http://store:8080"}}}}}),
		},
	}
	cfg := &config.Config{Version: 1, StorageClasses: []string{"nfs"}}
	result, err := Build(snapshot, cfg)
	if err != nil {
		t.Fatal(err)
	}
	storeID := model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "store"}.ID()
	if !result.Inventory.Workloads[storeID].DirectNFS {
		t.Fatal("store should be a direct NFS user")
	}
	if len(result.Inventory.Edges) != 1 || result.Inventory.Edges[0].Consumer.Name != "api" || result.Inventory.Edges[0].Provider.Name != "store" {
		t.Fatalf("unexpected edges: %#v", result.Inventory.Edges)
	}
}

func TestBuildDoesNotInspectSecretReferences(t *testing.T) {
	snapshot := &kube.Snapshot{
		Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "store"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "store"}}}},
		Deployments: []appsv1.Deployment{
			deployment("app", "store", map[string]string{"app": "store"}, corev1.PodSpec{}),
			deployment("app", "api", map[string]string{"app": "api"}, corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "STORE", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "store"}, Key: "url"}}}}}}}),
		},
	}
	result, err := Build(snapshot, &config.Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Inventory.Edges) != 0 {
		t.Fatalf("secret reference produced dependency: %#v", result.Inventory.Edges)
	}
}

func TestBuildSuppressesIgnoredInference(t *testing.T) {
	snapshot := &kube.Snapshot{
		Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "store"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "store"}}}},
		Deployments: []appsv1.Deployment{
			deployment("app", "store", map[string]string{"app": "store"}, corev1.PodSpec{}),
			deployment("app", "api", map[string]string{"app": "api"}, corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "STORE", Value: "http://store:8080"}}}}}),
		},
	}
	cfg := &config.Config{
		Version: 1, StorageClasses: []string{"nfs"},
		Inference: config.Inference{Ignore: []config.IgnoredDependency{{
			Consumer: config.ResourceSelector{Kind: "Deployment", Namespace: "app", Name: "api"},
			Provider: config.ResourceSelector{Kind: "Deployment", Namespace: "app", Name: "store"},
		}}},
	}
	result, err := Build(snapshot, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Inventory.Edges) != 0 {
		t.Fatalf("ignored inference remained active: %#v", result.Inventory.Edges)
	}
}

func TestBuildDiscoversDirectNFSVolume(t *testing.T) {
	snapshot := &kube.Snapshot{Deployments: []appsv1.Deployment{
		deployment("app", "direct", map[string]string{"app": "direct"}, corev1.PodSpec{Volumes: []corev1.Volume{{VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "nfs", Path: "/data"}}}}}),
	}}
	result, err := Build(snapshot, &config.Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err != nil {
		t.Fatal(err)
	}
	id := model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "direct"}.ID()
	if !result.Inventory.Workloads[id].DirectNFS {
		t.Fatal("direct NFS volume was not discovered")
	}
}

func TestBuildDiscoversFluxOwnershipChain(t *testing.T) {
	app := deployment("app", "api", nil, corev1.PodSpec{})
	app.Labels = map[string]string{
		"helm.toolkit.fluxcd.io/name":      "application",
		"helm.toolkit.fluxcd.io/namespace": "app",
	}
	helmRelease := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": model.KindHelmRelease,
		"metadata": map[string]any{
			"namespace": "app", "name": "application",
			"labels": map[string]any{
				"kustomize.toolkit.fluxcd.io/name":      "application",
				"kustomize.toolkit.fluxcd.io/namespace": "gitops-system",
			},
		},
	}}
	kustomization := unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": model.KindKustomization,
		"metadata": map[string]any{"namespace": "gitops-system", "name": "application"},
	}}
	result, err := Build(&kube.Snapshot{
		Deployments: []appsv1.Deployment{app}, HelmReleases: []unstructured.Unstructured{helmRelease},
		Kustomizations: []unstructured.Unstructured{kustomization},
	}, &config.Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Inventory.GitOpsOwnership) != 2 {
		t.Fatalf("expected HelmRelease and Kustomization owners, got %#v", result.Inventory.GitOpsOwnership)
	}
}

func TestBootstrapConfigDetectsNFSStorageClasses(t *testing.T) {
	snapshot := &kube.Snapshot{
		StorageClasses: []storagev1.StorageClass{
			{ObjectMeta: metav1.ObjectMeta{Name: "nfs-dynamic"}, Provisioner: "nfs.csi.k8s.io"},
			{ObjectMeta: metav1.ObjectMeta{Name: "local"}, Provisioner: "example.io/local"},
		},
		PVs: []corev1.PersistentVolume{{
			ObjectMeta: metav1.ObjectMeta{Name: "legacy-volume"},
			Spec:       corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{NFS: &corev1.NFSVolumeSource{Server: "nfs", Path: "/data"}}},
		}},
		PVCs: []corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "legacy"},
			Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: new("nfs-legacy"), VolumeName: "legacy-volume"},
		}},
	}
	cfg, err := BootstrapConfig(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.StorageClasses, ","); got != "nfs-dynamic,nfs-legacy" {
		t.Fatalf("unexpected storage classes: %s", got)
	}
	if !cfg.Inference.ServiceReferencesEnabled() {
		t.Fatal("service reference inference should be enabled")
	}
}

func TestBootstrapConfigRejectsClusterWithoutNFSStorageClass(t *testing.T) {
	if _, err := BootstrapConfig(&kube.Snapshot{}); err == nil {
		t.Fatal("expected missing NFS storage class error")
	}
}

func TestFilterDecidedSuggestions(t *testing.T) {
	consumer := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "consumer"}}
	provider := &model.Workload{Ref: model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "provider"}}
	workloads := map[string]*model.Workload{consumer.Ref.ID(): consumer, provider.Ref.ID(): provider}
	suggestion := model.Suggestion{Consumer: consumer.Ref, Targets: []model.Ref{provider.Ref}, Evidence: "app.example"}
	edge := model.Edge{Consumer: consumer.Ref, Provider: provider.Ref}
	if got := filterDecidedSuggestions([]model.Suggestion{suggestion}, []model.Edge{edge}, nil, workloads); len(got) != 0 {
		t.Fatalf("accepted suggestion was not filtered: %#v", got)
	}
	ignored := []config.IgnoredDependency{{
		Consumer: config.ResourceSelector{Kind: consumer.Ref.Kind, Namespace: consumer.Ref.Namespace, Name: consumer.Ref.Name},
		Provider: config.ResourceSelector{Kind: provider.Ref.Kind, Namespace: provider.Ref.Namespace, Name: provider.Ref.Name},
	}}
	if got := filterDecidedSuggestions([]model.Suggestion{suggestion}, nil, ignored, workloads); len(got) != 0 {
		t.Fatalf("ignored suggestion was not filtered: %#v", got)
	}
}

func TestBuildIgnoresTelemetryServiceReference(t *testing.T) {
	snapshot := &kube.Snapshot{
		Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "monitoring", Name: "pushgateway-prometheus-pushgateway"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "pushgateway"}}}},
		Deployments: []appsv1.Deployment{
			deployment("monitoring", "pushgateway-prometheus-pushgateway", map[string]string{"app": "pushgateway"}, corev1.PodSpec{}),
			deployment("monitoring", "backup", map[string]string{"app": "backup"}, corev1.PodSpec{Containers: []corev1.Container{{Name: "backup", Env: []corev1.EnvVar{{Name: "PUSH", Value: "http://pushgateway-prometheus-pushgateway:9091"}}}}}),
		},
	}
	result, err := Build(snapshot, &config.Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Inventory.Edges) != 0 {
		t.Fatalf("telemetry push should not be a dependency: %#v", result.Inventory.Edges)
	}
}

func TestBuildDoesNotTreatTelemetryPortAsTelemetry(t *testing.T) {
	snapshot := &kube.Snapshot{
		Services: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "database"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "database"}}}},
		Deployments: []appsv1.Deployment{
			deployment("app", "database", map[string]string{"app": "database"}, corev1.PodSpec{}),
			deployment("app", "api", map[string]string{"app": "api"}, corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Env: []corev1.EnvVar{{Name: "DATABASE", Value: "http://database:9091"}}}}}),
		},
	}
	result, err := Build(snapshot, &config.Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Inventory.Edges) != 1 || result.Inventory.Edges[0].Provider.Name != "database" {
		t.Fatalf("ordinary service on a telemetry port was ignored: %#v", result.Inventory.Edges)
	}
}

func TestBuildDetectsOperatorManagedWorkload(t *testing.T) {
	controller := true
	prometheus := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "data", Name: "database-replica",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "example.io/v1", Kind: "DatabaseCluster", Name: "database", Controller: &controller}},
		},
		Spec: appsv1.StatefulSetSpec{Replicas: new(int32(1)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{}}},
	}
	result, err := Build(&kube.Snapshot{StatefulSets: []appsv1.StatefulSet{prometheus}}, &config.Config{Version: 1, StorageClasses: []string{"nfs"}})
	if err != nil {
		t.Fatal(err)
	}
	managed := result.Inventory.Workloads[model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "database-replica"}.ID()]
	if managed.ManagedBy == nil || managed.ManagedBy.Kind != "DatabaseCluster" {
		t.Fatalf("expected DatabaseCluster controller, got %#v", managed.ManagedBy)
	}
}

func TestBuildMergesCustomDependencies(t *testing.T) {
	frontend := deployment("web", "frontend", nil, corev1.PodSpec{})
	database := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "data", Name: "database"},
		Spec:       appsv1.StatefulSetSpec{Replicas: new(int32(1)), Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{}}},
	}
	cfg := &config.Config{
		Version: 1, StorageClasses: []string{"nfs"},
		CustomGroups: map[string]config.Group{
			"web":      {Resources: []config.ResourceSelector{{Kind: model.KindDeployment, Namespace: "web", Name: "frontend"}}},
			"database": {Resources: []config.ResourceSelector{{Kind: model.KindStatefulSet, Namespace: "data", Name: "database"}}},
		},
		CustomDependencies: []config.Dependency{{Consumer: "web", Provider: "database"}},
	}
	result, err := Build(&kube.Snapshot{Deployments: []appsv1.Deployment{frontend}, StatefulSets: []appsv1.StatefulSet{database}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, edge := range result.Inventory.Edges {
		if edge.Consumer.Name == "frontend" && edge.Provider.Name == "database" && edge.Type == model.EvidenceExplicit {
			found = true
		}
	}
	if !found {
		t.Fatalf("custom dependency did not produce an explicit edge: %#v", result.Inventory.Edges)
	}
}

func TestFilterDecidedSuggestionsDropsReverseOfEdge(t *testing.T) {
	frontend := model.Ref{Kind: model.KindDeployment, Namespace: "web", Name: "frontend"}
	backend := model.Ref{Kind: model.KindDeployment, Namespace: "web", Name: "backend"}
	edges := []model.Edge{{Consumer: frontend, Provider: backend, Type: model.EvidenceService}}
	suggestion := model.Suggestion{Consumer: backend, Targets: []model.Ref{frontend}, Evidence: "web.example.test"}
	if got := filterDecidedSuggestions([]model.Suggestion{suggestion}, edges, nil, nil); len(got) != 0 {
		t.Fatalf("a candidate reversing an existing edge must be dropped: %#v", got)
	}
}

func TestFilterRelevantSuggestions(t *testing.T) {
	consumer := model.Ref{Kind: model.KindDeployment, Namespace: "app", Name: "consumer"}
	scaled := model.Ref{Kind: model.KindStatefulSet, Namespace: "data", Name: "store"}
	unscaled := model.Ref{Kind: model.KindDeployment, Namespace: "ops", Name: "cert-manager"}
	scope := map[string]bool{consumer.ID(): true, scaled.ID(): true}
	suggestions := []model.Suggestion{
		{Consumer: consumer, Targets: []model.Ref{scaled, unscaled}},
		{Consumer: consumer, Targets: []model.Ref{unscaled}},
		{Consumer: unscaled, Evidence: "diagnostic"},
		{Consumer: consumer, Evidence: "diagnostic"},
	}
	result := filterRelevantSuggestions(suggestions, scope)
	if len(result) != 2 {
		t.Fatalf("expected the unscaled-provider candidate and unscaled-consumer diagnostic to drop: %#v", result)
	}
	if len(result[0].Targets) != 1 || result[0].Targets[0].ID() != scaled.ID() {
		t.Fatalf("expected only the scaled provider to remain: %#v", result[0])
	}
	if len(result[1].Targets) != 0 || result[1].Consumer.ID() != consumer.ID() {
		t.Fatalf("expected the in-scope diagnostic to remain: %#v", result[1])
	}
}

func deployment(namespace, name string, podLabels map[string]string, spec corev1.PodSpec) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: new(int32(1)), Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}, Spec: spec}},
	}
}
