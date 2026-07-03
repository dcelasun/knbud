package discovery

import (
	"testing"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/kube"
	"github.com/dcelasun/knbud/internal/model"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildDiscoversNFSAndServiceDependency(t *testing.T) {
	snapshot := &kube.Snapshot{
		PVCs:     []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: lo.ToPtr("nfs")}}},
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

func deployment(namespace, name string, podLabels map[string]string, spec corev1.PodSpec) appsv1.Deployment {
	return appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       appsv1.DeploymentSpec{Replicas: lo.ToPtr(int32(1)), Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: podLabels}, Spec: spec}},
	}
}
