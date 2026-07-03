package kube

import (
	"context"
	"errors"
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

type Snapshot struct {
	Deployments     []appsv1.Deployment
	StatefulSets    []appsv1.StatefulSet
	CronJobs        []batchv1.CronJob
	Jobs            []batchv1.Job
	PVs             []corev1.PersistentVolume
	PVCs            []corev1.PersistentVolumeClaim
	Services        []corev1.Service
	ConfigMaps      []corev1.ConfigMap
	Ingresses       []networkingv1.Ingress
	NetworkPolicies []networkingv1.NetworkPolicy
	HPAs            []autoscalingv2.HorizontalPodAutoscaler
	Kustomizations  []unstructured.Unstructured
	HelmReleases    []unstructured.Unstructured
	Applications    []unstructured.Unstructured
}

type Client struct {
	Interface kubernetes.Interface
	Dynamic   dynamic.Interface
	Mapper    meta.RESTMapper
}

func New(kubeconfig, contextName string) (*Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes configuration: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create dynamic Kubernetes client: %w", err)
	}
	mapper, err := newRESTMapper(clientset.Discovery())
	if err != nil {
		return nil, err
	}
	return &Client{Interface: clientset, Dynamic: dynamicClient, Mapper: mapper}, nil
}

func newRESTMapper(client discovery.DiscoveryInterface) (meta.RESTMapper, error) {
	resources, err := restmapper.GetAPIGroupResources(client)
	if err != nil {
		return nil, fmt.Errorf("discover Kubernetes API resources: %w", err)
	}
	return restmapper.NewDiscoveryRESTMapper(resources), nil
}

func (c *Client) Snapshot(ctx context.Context) (*Snapshot, error) {
	var snapshot Snapshot
	var wg sync.WaitGroup
	errorsChannel := make(chan error, 14)
	run := func(operation func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := operation(); err != nil {
				errorsChannel <- err
			}
		}()
	}
	run(func() error {
		items, err := c.Interface.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.Deployments = items.Items
		}
		return wrapListError("deployments", err)
	})
	run(func() error {
		items, err := c.Interface.AppsV1().StatefulSets("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.StatefulSets = items.Items
		}
		return wrapListError("statefulsets", err)
	})
	run(func() error {
		items, err := c.Interface.BatchV1().CronJobs("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.CronJobs = items.Items
		}
		return wrapListError("cronjobs", err)
	})
	run(func() error {
		items, err := c.Interface.BatchV1().Jobs("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.Jobs = items.Items
		}
		return wrapListError("jobs", err)
	})
	run(func() error {
		items, err := c.Interface.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.PVs = items.Items
		}
		return wrapListError("persistent volumes", err)
	})
	run(func() error {
		items, err := c.Interface.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.PVCs = items.Items
		}
		return wrapListError("persistent volume claims", err)
	})
	run(func() error {
		items, err := c.Interface.CoreV1().Services("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.Services = items.Items
		}
		return wrapListError("services", err)
	})
	run(func() error {
		items, err := c.Interface.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.ConfigMaps = items.Items
		}
		return wrapListError("configmaps", err)
	})
	run(func() error {
		items, err := c.Interface.NetworkingV1().Ingresses("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.Ingresses = items.Items
		}
		return wrapListError("ingresses", err)
	})
	run(func() error {
		items, err := c.Interface.NetworkingV1().NetworkPolicies("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.NetworkPolicies = items.Items
		}
		return wrapListError("network policies", err)
	})
	run(func() error {
		items, err := c.Interface.AutoscalingV2().HorizontalPodAutoscalers("").List(ctx, metav1.ListOptions{})
		if err == nil {
			snapshot.HPAs = items.Items
		}
		return wrapListError("horizontal pod autoscalers", err)
	})
	run(func() error {
		items, err := c.listOptional(ctx, schema.GroupKind{Group: "kustomize.toolkit.fluxcd.io", Kind: "Kustomization"})
		snapshot.Kustomizations = items
		return err
	})
	run(func() error {
		items, err := c.listOptional(ctx, schema.GroupKind{Group: "helm.toolkit.fluxcd.io", Kind: "HelmRelease"})
		snapshot.HelmReleases = items
		return err
	})
	run(func() error {
		items, err := c.listOptional(ctx, schema.GroupKind{Group: "argoproj.io", Kind: "Application"})
		snapshot.Applications = items
		return err
	})
	wg.Wait()
	close(errorsChannel)
	var failures []error
	for err := range errorsChannel {
		failures = append(failures, err)
	}
	if err := errors.Join(failures...); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func wrapListError(resource string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("list %s: %w", resource, err)
}

func (c *Client) listOptional(ctx context.Context, kind schema.GroupKind) ([]unstructured.Unstructured, error) {
	mapping, err := c.Mapper.RESTMapping(kind)
	if meta.IsNoMatchError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", kind.String(), err)
	}
	items, err := c.Dynamic.Resource(mapping.Resource).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", kind.String(), err)
	}
	return items.Items, nil
}
