# knbud - K8s NFS Buddy

`knbud` scales Kubernetes workloads down safely before NFS maintenance and scales them back up afterwards.

It supports Deployments, StatefulSets, and CronJobs.

## Installation

### Releases (recommended)

Download an archive from the [latest release](https://github.com/dcelasun/knbud/releases/latest).

Choose `linux`, `macos`, or `windows`, then choose your CPU architecture:

- `x86_64` for Intel or AMD
- `arm64` for ARM or Apple Silicon

On Linux or macOS:

```sh
tar -xzf knbud_<version>_<os>_<architecture>.tar.gz
chmod +x knbud
./knbud --help
```

On Windows, extract the `.zip` file and run:

```powershell
.\knbud.exe --help
```

### Distro packages

- [Arch Linux (AUR)](https://aur.archlinux.org/packages/knbud)

### Building manually

```sh
make vet
make test
make build
```

The binary is written to `./knbud`.

## Discover

The first stage. `discover` reads the live cluster.

Inspect the cluster without writing a file:

```sh
./knbud discover --dry-run
```

Create or update `knbud.yaml`:

```sh
./knbud discover
```

High-confidence dependencies are saved automatically. Unclear dependencies are shown for review. Flux support and hidden dependencies are handled by short prompts.

Use flags for a non-interactive run:

```sh
./knbud discover --accept-suggestions
./knbud discover --ignore-suggestions
```

Any dependency not auto-discovered must be added to `customDependencies`, see [Configuration](#configuration).

## Scale down and up

Plan commands use only the saved relationships in `knbud.yaml`.

Print a scale down plan:

```sh
./knbud plan down --dry-run
```

Print and apply it:

```sh
./knbud plan down
```

Once the NFS service is online again, scale back up:

```sh
./knbud plan up
```

Skip the confirmation prompt:

```sh
./knbud plan down --yes
./knbud plan up --yes
```

Useful flags:

| Option | Default | Use |
| --- | --- | --- |
| `--config` | `knbud.yaml` | Config file |
| `--kubeconfig` | client-go default | Kubeconfig file |
| `--context` | current context | Kubernetes context |
| `--output` | `human` | `human` or `json` |
| `--parallelism` | `8` | Actions per wave |
| `--timeout` | `5m` | Timeout per workload |

## Configuration

```yaml
version: 1

storageClasses:
  - shared-nfs

inference:
  serviceReferences: true

gitOps:
  flux:
    enabled: true
    mode: auto

customGroups:
  web:
    selectors:
      - kind: Deployment
        namespace: example
        matchLabels:
          app.kubernetes.io/part-of: web
  database:
    resources:
      - kind: StatefulSet
        namespace: example
        name: database

customDependencies:
  - consumer: web
    provider: database

include:
  - kind: Deployment
    namespace: example
    name: storage-operator

exclude:
  - kind: Deployment
    namespace: example
    name: unmanaged
```

`groups` and `dependencies` are managed by `discover`.

Use `customGroups` and `customDependencies` for hand-written rules. `discover` preserves them.

A dependency points from consumer to provider. Consumers scale down first. Providers scale up first.

`include` adds a workload to the scale down plan. `exclude` keeps it out. Saved state still allows an excluded workload to be scaled up.

Selectors support `name`, `matchLabels`, or `labelSelector`. A missing namespace matches every namespace.

## Discovery rules

NFS use is found from:

- PVC storage classes
- NFS persistent volumes
- inline NFS volumes
- the NFS CSI driver
- StatefulSet claim templates

Service dependencies are found in container commands, arguments, environment values, and ConfigMaps. The Service must be in the same namespace. A single matching workload is accepted automatically.

Ambiguous Services and Ingress hostnames require review. Suggestions that cannot change the plan are hidden.

Secrets are never read. Add dependencies stored in Secrets or application settings by hand.

Pushgateway and Alertmanager endpoints are treated as telemetry, not runtime dependencies.

Accepted and inferred dependencies are saved in `knbud.yaml`. Planning does not infer them again.

## GitOps

Flux auto mode suspends owners of workloads in the plan:

```yaml
gitOps:
  flux:
    enabled: true
    mode: auto
```

Flux and Argo CD can use an explicit list:

```yaml
gitOps:
  flux:
    enabled: true
    mode: explicit
    resources:
      - kind: Kustomization
        namespace: gitops-system
        name: example
  argoCD:
    enabled: true
    mode: explicit
    resources:
      - kind: Application
        namespace: argocd
        name: example
```

Argo CD supports explicit mode only.

## Safety

Scaling down (`plan down`) runs consumers before providers. Scaling up (`plan up`) uses the reverse order. Workloads in one wave run in parallel.

Original replica and suspension values are stored in annotations. They are removed only after a successful scale up. Interrupted operations can be run again.

HorizontalPodAutoscalers block planning. Active CronJob Jobs must finish before scale down completes.

Operator-managed workloads produce warnings. Include the operator and add a dependency in `customDependencies` when it must scale down first.
