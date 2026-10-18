# Production Deployment Guide

This guide covers deploying the **Sandbox Execution System** across two environments:
1. **Docker Compose:** Ideal for local development, staging, or single-node production servers.
2. **Kubernetes:** Ideal for cloud-native, multi-tenant production clusters.

---

## 1. Deploying with Docker Compose

The `deploy/docker-compose.yml` file sets up the complete isolated stack:
* `sandbox-engine`: FastAPI service managing the container pool on port `8080`.
* `redis`: Session lease registry on port `6379`.
* `squid`: Egress SSRF firewall on port `3128`.

```bash
cd deploy
docker compose up -d
```

### Health Check:
```bash
curl http://localhost:8080/healthz
# Expected: {"status": "healthy", "driver": "DockerDriver", ...}
```

---

## 2. Deploying on Kubernetes

### Step 1: Provision a Dedicated Tainted Node Pool
Untrusted agent sandboxes should **never** share physical nodes with core infrastructure or databases.

Label and taint your sandbox node pool:
* **Taint:** `sandbox=true:NoSchedule`
* **Label:** `sandbox=true`

*(On GKE: `gcloud container node-pools create sandbox-pool --taints sandbox=true:NoSchedule --node-labels sandbox=true --sandbox type=gvisor`)*

---

### Step 2: Apply the gVisor RuntimeClass
```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
scheduling:
  nodeSelector:
    sandbox: "true"
  tolerations:
    - key: "sandbox"
      operator: "Exists"
      effect: "NoSchedule"
```

Apply to cluster:
```bash
kubectl apply -f runtimeclass.yaml
```

---

### Step 3: Create Namespace & Network Lockdown
```bash
kubectl create namespace sandboxes
```

Apply the strict `NetworkPolicy` to isolate sandbox Pods:
```bash
kubectl apply -f deploy/k8s/networkpolicy.yaml
```

---

### Step 4: Deploy `sandbox-engine` Service
Deploy `sandbox-engine` into the `sandboxes` namespace with a ServiceAccount permitted to manage Pods:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbox-engine
  namespace: sandboxes
spec:
  replicas: 2
  selector:
    matchLabels:
      app: sandbox-engine
  template:
    metadata:
      labels:
        app: sandbox-engine
    spec:
      containers:
        - name: engine
          image: my-registry.io/sandbox-engine:latest
          env:
            - name: SANDBOX_DRIVER
              value: "k8s"
            - name: SANDBOX_REDIS_URL
              value: "redis://redis-service.sandboxes:6379/0"
            - name: SANDBOX_WARM_POOL_SIZE
              value: "5"
            - name: SANDBOX_K8S_NAMESPACE
              value: "sandboxes"
            - name: SANDBOX_K8S_RUNTIME_CLASS
              value: "gvisor"
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: sandbox-engine
  namespace: sandboxes
spec:
  selector:
    app: sandbox-engine
  ports:
    - protocol: TCP
      port: 8080
      targetPort: 8080
```

Internal cluster applications connect to:
```python
client = SandboxClient(base_url="http://sandbox-engine.sandboxes.svc.cluster.local:8080")
```
