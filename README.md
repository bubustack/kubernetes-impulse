# ☸️ Kubernetes Impulse

A BubuStack Impulse that triggers Stories from Kubernetes cluster activity.

## 🌟 Highlights

- **Watch Mode**: React to resource state changes (Add/Update/Delete)
- **Events Mode**: React to Kubernetes Event objects (warnings, errors)
- **Flexible Filtering**: By namespace, labels, field selectors, template expressions
- **Debouncing**: Prevent rapid-fire triggers for the same resource
- **Event Aggregation**: Batch multiple events before triggering

## 🚀 Quick Start

1. Install the ImpulseTemplate:

```bash
kubectl apply -f Impulse.yaml
```

2. Create an Impulse instance (see examples below).

## 🔄 Modes

### Watch Mode

Watches for resource state changes using the Kubernetes Watch API.

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Impulse
metadata:
  name: pod-crash-watcher
spec:
  templateRef:
    name: kubernetes
  
  storyRef:
    name: pod-crash-analyzer
  
  with:
    mode: watch
    watch:
      resources:
        - apiVersion: v1
          kind: Pod
          namespace: production
          labelSelector: "tier=backend"
      triggers:
        - Modified
      filters:
        condition: 'and (eq .eventType "Modified") (eq .object.status.phase "Failed")'
        includeOldObject: true
        debounceSeconds: 5
```

**Supported Resources:**
- Core: Pod, Service, ConfigMap, Secret, Namespace, Node, PersistentVolumeClaim
- Apps: Deployment, StatefulSet, DaemonSet, ReplicaSet
- Batch: Job, CronJob
- Networking: Ingress, NetworkPolicy
- Custom Resources: Any CRD with apiVersion and kind

### Events Mode

Watches Kubernetes Event objects created by controllers.

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Impulse
metadata:
  name: cluster-warnings
spec:
  templateRef:
    name: kubernetes
  
  storyRef:
    name: warning-analyzer
  
  with:
    mode: events
    events:
      namespaces:
        - production
        - staging
      types:
        - Warning
      reasons:
        - FailedScheduling
        - Unhealthy
        - BackOff
        - FailedMount
      aggregation:
        enabled: true
        windowSeconds: 300
        minCount: 3
```

**Common Event Reasons:**
- `FailedScheduling` - Pod couldn't be scheduled
- `Unhealthy` - Liveness/readiness probe failed
- `BackOff` - Container restart backoff
- `FailedMount` - Volume mount failed
- `Pulled` - Image pulled successfully
- `Created` - Container created
- `Killing` - Container being killed
- `NodeNotReady` - Node became not ready

## 📥 Story Inputs

### Watch Mode Inputs

```yaml
inputs:
  mode: watch
  eventType: Modified  # Added | Modified | Deleted
  object:
    apiVersion: v1
    kind: Pod
    metadata:
      name: my-pod
      namespace: production
      labels: {...}
      annotations: {...}
    spec: {...}
    status:
      phase: Failed
      containerStatuses:
        - name: main
          state:
            terminated:
              exitCode: 137
              reason: OOMKilled
  oldObject:            # Present when filters.includeOldObject=true and prior state is cached
    status:
      phase: Pending
```

### Events Mode Inputs

```yaml
inputs:
  mode: events
  events:
    - type: Warning
      reason: FailedScheduling
      message: "0/3 nodes available: insufficient memory"
      count: 5
      firstTimestamp: "2026-01-20T10:00:00Z"
      lastTimestamp: "2026-01-20T10:05:00Z"
      involvedObject:
        apiVersion: v1
        kind: Pod
        name: my-pod
        namespace: production
```

## ⚙️ Configuration (`Impulse.spec.with`)

### Watch Configuration

| Field | Type | Description |
|-------|------|-------------|
| `resources` | array | Resources to watch |
| `resources[].apiVersion` | string | API version (e.g., v1, apps/v1) |
| `resources[].kind` | string | Resource kind (e.g., Pod, Deployment) |
| `resources[].namespace` | string | Namespace (empty = all) |
| `resources[].labelSelector` | string | Label selector |
| `resources[].fieldSelector` | string | Field selector |
| `triggers` | array | Event types: Added, Modified, Deleted |
| `filters.condition` | string | Template boolean expression; variables: `.object`, `.oldObject`, `.eventType` |
| `filters.debounceSeconds` | int | Debounce window |
| `filters.includeOldObject` | bool | Include cached previous state in filter/input as `oldObject` |

### Events Configuration

| Field | Type | Description |
|-------|------|-------------|
| `namespaces` | array | Namespaces to watch |
| `types` | array | Normal, Warning |
| `reasons` | array | Event reasons to match |
| `involvedObjectKinds` | array | Filter by object kind |
| `aggregation.enabled` | bool | Enable aggregation |
| `aggregation.windowSeconds` | int | Aggregation window |
| `aggregation.minCount` | int | Minimum events before trigger |

### Session Key Configuration

| Field | Type | Description |
|-------|------|-------------|
| `strategy` | string | auto, unique, or custom |
| `expression` | string | Template expression for custom key (`.object`, `.oldObject`, `.eventType`) |

## 📘 Example Stories

### Pod Crash Notifier

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: pod-crash-analyzer
spec:
  pattern: batch
  steps:
    - name: analyze
      ref:
        name: openai-chat
      with:
        userPrompt: |
          A pod crashed in the cluster. Analyze and suggest fixes:
          
          Pod: {{ inputs.object.metadata.name }}
          Namespace: {{ inputs.object.metadata.namespace }}
          Phase: {{ inputs.object.status.phase }}
          
          Container Statuses:
          {{ inputs.object.status.containerStatuses | tojson }}
    
    - name: notify
      needs: [analyze]
      ref:
        name: slack-notifier
      with:
        channel: "#alerts"
        text: |
          🚨 *Pod Crash Alert*
          
          *Pod:* {{ inputs.object.metadata.name }}
          *Namespace:* {{ inputs.object.metadata.namespace }}
          
          *AI Analysis:*
          {{ steps["analyze"].output.text }}
```

### Deployment Rollout Monitor

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Impulse
metadata:
  name: deployment-monitor
spec:
  templateRef:
    name: kubernetes
  storyRef:
    name: deployment-notifier
  with:
    mode: watch
    watch:
      resources:
        - apiVersion: apps/v1
          kind: Deployment
      triggers:
        - Modified
      filters:
        condition: |
          and
            (eq .object.status.updatedReplicas .object.status.replicas)
            (eq .object.status.availableReplicas .object.status.replicas)
```

### Secret Change Audit

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Impulse
metadata:
  name: secret-audit
spec:
  templateRef:
    name: kubernetes
  storyRef:
    name: secret-change-log
  with:
    mode: watch
    watch:
      resources:
        - apiVersion: v1
          kind: Secret
          labelSelector: "sensitive=true"
      triggers:
        - Modified
        - Deleted
```

## 🔐 RBAC Requirements

The impulse needs permissions to watch resources:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubernetes-impulse
rules:
  # For watch mode
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["get", "list", "watch"]
  # For events mode
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list", "watch"]
  # For durable StoryTrigger submission and StoryRun readback
  - apiGroups: ["runs.bubustack.io"]
    resources: ["storytriggers"]
    verbs: ["create", "get"]
  - apiGroups: ["runs.bubustack.io"]
    resources: ["storyruns"]
    verbs: ["get"]
  - apiGroups: ["runs.bubustack.io"]
    resources: ["storyruns/status"]
    verbs: ["patch"]
  - apiGroups: ["bubustack.io"]
    resources: ["impulses", "impulses/status"]
    verbs: ["get", "patch"]
```

## 🩺 Health Endpoints

- `GET :8080/health` - Liveness probe
- `GET :8080/ready` - Readiness probe

## 🧪 Local Development

```bash
# Build binary
make build

# Run tests
make test

# Build Docker image
make docker-build VERSION=v0.1.0

# Push to registry
make docker-push VERSION=v0.1.0
```


## 🤝 Community & Support

- [Contributing](./CONTRIBUTING.md)
- [Support](./SUPPORT.md)
- [Security Policy](./SECURITY.md)
- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [Discord](https://discord.gg/dysrB7D8H6)

## 📄 License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
