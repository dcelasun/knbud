# knbud

`knbud` finds Kubernetes workloads that use NFS, calculates their runtime dependency graph, and scales them safely for NFS maintenance.

It supports Deployments, StatefulSets, and CronJobs. Direct NFS users are discovered from PVC storage classes. Consumers that do not mount NFS are included through explicit or inferred dependencies.

## Build

```console
make vet
make test
make build
```

The resulting binary is `./knbud`.

## Commands

```console
knbud discover --config /path/to/knbud.yaml
knbud plan down --config /path/to/knbud.yaml
knbud plan up --config /path/to/knbud.yaml
knbud down --config /path/to/knbud.yaml
knbud up --config /path/to/knbud.yaml
```

`discover` and `plan` are read-only. `down` and `up` print the plan and require confirmation unless `--yes` is supplied.

Common options:

| Option | Default | Purpose |
| --- | --- | --- |
| `--config` | `knbud.yaml` | Configuration path |
| `--kubeconfig` | client-go default | Kubeconfig path |
| `--context` | current context | Kubeconfig context |
| `--output` | `human` | `human` or `json` |
| `--parallelism` | `8` | Maximum concurrent actions in a wave |
| `--timeout` | `5m` | Timeout per workload |
| `--yes` | `false` | Skip confirmation for `down` and `up` |

## Configuration

```yaml
version: 1

storageClasses:
  - nfs-some-class
  - nfs-another-class

inference:
  serviceReferences: true
  ignore:
    - consumer:
        kind: Deployment
        namespace: example
        name: metrics-reader
      provider:
        kind: Deployment
        namespace: example
        name: optional-metrics

gitOps:
  flux:
    enabled: true
    mode: auto
  argoCD:
    enabled: false

groups:
  application:
    selectors:
      - kind: Deployment
        namespace: application
        matchLabels:
          app.kubernetes.io/part-of: application
  object-store:
    resources:
      - kind: StatefulSet
        namespace: object-store
        name: object-store

dependencies:
  - consumer: application
    provider: object-store

include:
  - kind: Deployment
    namespace: platform-system
    name: storage-operator

exclude:
  - kind: Deployment
    namespace: example
    name: never-manage
```

Selectors require a kind and may use a name, `matchLabels`, or `labelSelector`. Namespace omission matches all namespaces.

An edge means that the consumer requires the provider. Consumers are stopped before providers and restored after providers.

`include` adds workloads that do not directly use NFS. `exclude` prevents workloads from entering a down plan. An excluded workload with existing saved state remains eligible for restoration.

## Dependency inference

Exact same-namespace Service references in container environment values, commands, arguments, and referenced ConfigMaps are active automatically when the Service selector resolves to one scalable workload.

Ingress hostname references and ambiguous Service mappings are suggestions only. Secret contents are never read. Hidden dependencies must be declared explicitly.

Every active edge includes its source and evidence in `discover` and `plan`. `inference.ignore` suppresses a specific inferred edge.

## GitOps controllers

`discover` reports Flux and Argo CD ownership whether or not suspension is enabled. A provider must be explicitly enabled before `down` mutates its resources.

Flux supports automatic ownership selection:

```yaml
gitOps:
  flux:
    enabled: true
    mode: auto
```

Automatic mode uses Flux ownership labels and inventory data. It includes both HelmReleases and their owning Kustomizations, and suspends only owners associated with workloads in the maintenance plan.

Flux and Argo CD also support exact resource selection:

```yaml
gitOps:
  flux:
    enabled: true
    mode: explicit
    resources:
      - kind: Kustomization
        namespace: gitops-system
        name: application
      - kind: HelmRelease
        namespace: application
        name: application
  argoCD:
    enabled: true
    mode: explicit
    resources:
      - kind: Application
        namespace: argocd
        name: application
```

Argo CD supports only explicit mode because workload-to-Application tracking is not a sufficient authorization boundary for automatic mutation.

## Execution

The graph is divided into topological waves. Workloads in one wave run concurrently up to `--parallelism`. The next wave starts only after the entire current wave succeeds.

Before changing a workload, `knbud` stores its original replica or suspension state in the `knbud.io/state` annotation. An up operation discovers annotated workloads directly and removes state only after successful restoration. Interrupted operations can be rerun.

GitOps state is stored separately in `knbud.io/gitops-state`. During `down`, Kustomizations are suspended before HelmReleases and workload changes. During `up`, workloads are restored first, followed by HelmReleases and Kustomizations. Resources that were already suspended remain suspended.

HorizontalPodAutoscalers targeting selected workloads fail planning. CronJobs are suspended and existing active Jobs are allowed to finish; the operation fails if they exceed the workload timeout.
