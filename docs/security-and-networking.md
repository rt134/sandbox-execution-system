# Security & Zero-Trust Architecture

Executing untrusted AI-generated code requires **defense-in-depth across the kernel, network, authentication, and filesystem boundaries**. The Sandbox Execution System combines gVisor userspace kernel isolation with strict egress proxy filtering, token-based daemon authentication, path canonicalization, and cgroup resource limits.

---

## 1. Threat Model & Mitigations

| Threat | Attack Vector | System Defense |
| :--- | :--- | :--- |
| **Container Breakout** | Linux kernel 0-day exploit / privilege escalation | **gVisor (`runsc`)**: Untrusted code never executes syscalls directly on the host kernel. All calls are trapped in Go userspace. Silent fallback is disabled (`allow_runtime_fallback=False`). |
| **Privilege Escalation** | Code attempts `setuid` binaries (`su`, `sudo`, `ping`) | **Capabilities & Privileges**: All Linux capabilities are dropped (`cap_drop=["ALL"]`) and `no-new-privileges:true` is enforced on every container. |
| **Fork Bomb (Host DoS)** | `:(){ :|:& };:` or `os.fork()` recursion exhausting host PID table | **Docker**: `pids_limit=256`. **Kubernetes**: set `podPidsLimit=256` on the sandbox node kubelet (not enforced by the pod spec alone). |
| **Disk Exhaustion (Host DoS)**| `cat /dev/zero > file`, `/exec` writes, or unbounded logs | **UX quota** on `/fs/write` only. **K8s**: `emptyDir.sizeLimit` and `ephemeral-storage` limits; PVC size is a request, not a hard cap unless the StorageClass enforces capacity. **Docker**: json-file log rotation; host workspace cleanup on session TTL. Exec and shell redirection bypass daemon quota checks. |
| **Host Filesystem Traversal** | Agent crafts `workspace_id="../.."` or `../../etc/shadow` | **Path Traversal Defense**: `workspace_id` is validated against `^[a-zA-Z0-9_\-]{1,64}$` and canonical `os.path.realpath` is verified against `workspaces_root`. |
| **SSRF (Cloud Metadata)** | Code queries cloud metadata or RFC1918 via raw TCP/UDP | **Production (K8s)**: egress NetworkPolicy allows Squid only; Squid denylist blocks metadata, RFC1918, CGNAT, and provider-specific endpoints. HTTP(S) clients must honor `http_proxy`/`https_proxy`. Non-proxy egress and DNS tunneling remain an operator hardening task. **Docker dev**: per-sandbox internal networks plus Squid; not a production isolation boundary. |
| **Lateral Movement Between Sandboxes** | Malicious code reaches another sandbox daemon or engine | **K8s**: NetworkPolicy restricts ingress to engine pods on `:5004`. **Docker dev**: one internal bridge per sandbox (engine + Squid + sandbox only). Daemon auth token required; token must not be passed to user processes. |
| **Unauthenticated API Access** | Open access to Engine REST and WebSocket endpoints | **Production**: `SANDBOX_TENANT_KEYS_JSON` binds API keys to tenant IDs. `/livez`, `/healthz`, and `/readyz` stay unauthenticated for probes. Prometheus scrapes `:9090/metrics` (no tenant auth). |
| **K8s Cluster Compromise** | Container accesses `/var/run/secrets/kubernetes.io` | **Pod Spec Lockdown**: `automountServiceAccountToken: false` is enforced on all sandbox Pods. |
| **Host Workload Contamination** | Sandbox co-located on nodes running DB or Core APIs | **Dedicated Tainted Node Pool**: Nodes tainted `sandbox=true:NoSchedule`. |

---

## 2. Authentication & Authorization Architecture

```
User / Agent Caller
       │
       │ HTTP / WebSocket with Authorization: Bearer <API_KEY>
       ▼
┌──────────────────────────────────────────────────────────────┐
│  Sandbox Engine (FastAPI)                                    │
│  • Validates API Key via constant-time comparison            │
│  • Validates workspace_id against directory traversal        │
│  • Enforces atomic workspace concurrency locking             │
│  • Injects per-container internal SHELLCTL_AUTH_TOKEN        │
└──────────────────────────────────────────────────────────────┘
       │
       │ Direct container call with Authorization: Bearer <DAEMON_TOKEN>
       ▼
┌──────────────────────────────────────────────────────────────┐
│  In-Container Daemon (shellctl :5004)                        │
│  • Verifies daemon auth token before executing any command   │
│  • Bound to 127.0.0.1 or isolated overlay network            │
│  • Zero lateral access from adjacent containers             │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Kernel Isolation: gVisor (`runsc`)

```
Untrusted User / Agent Code
            │
            ▼ (Syscalls: clone, open, socket, ptrace, ioctl)
┌──────────────────────────────────────────────┐
│  gVisor Sentry (Userspace Kernel in Go)      │
│  • Emulates 300+ Linux syscalls              │
│  • Separate memory address space             │
│  • Traps malicious kernel exploits           │
└──────────────────────────────────────────────┘
            │
            ▼ (Filtered, benign Host Syscalls)
┌──────────────────────────────────────────────┐
│  Host Linux Kernel                           │
└──────────────────────────────────────────────┘
```

In production, sandboxes specify `runtime: runsc` (in Docker) or `runtimeClassName: gvisor` (in Kubernetes). If `runsc` is unavailable, the engine fails fast by default rather than silently downgrading to `runc`.

---

## 4. Network Egress Firewall: Squid SSRF Proxy

All sandboxes route outbound HTTP/HTTPS traffic through the Squid proxy (`deploy/squid/squid.conf`):

```squid
# ------------------------------------------
# SSRF Protection Rules
# ------------------------------------------
# Block Cloud Metadata
acl cloud_metadata dst 169.254.169.254/32
acl cloud_metadata dst 169.254.0.0/16
http_access deny cloud_metadata

# Block Loopback & Host Interconnects
acl loopback_net dst 127.0.0.0/8
http_access deny loopback_net

# Block Private IPv4 Networks (RFC 1918)
acl rfc1918_class_a dst 10.0.0.0/8
acl rfc1918_class_b dst 172.16.0.0/12
acl rfc1918_class_c dst 192.168.0.0/16
http_access deny rfc1918_class_a
http_access deny rfc1918_class_b
http_access deny rfc1918_class_c

# Block Kubernetes Default Service CIDR
acl k8s_service_cidr dst 10.96.0.0/12
http_access deny k8s_service_cidr

# Allow public internet ports
acl Safe_ports port 80
acl Safe_ports port 443
http_access allow Safe_ports
```

---

## 5. Kubernetes `NetworkPolicy`

Located in `k8s/network-policy.yaml`:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sandbox-isolation-policy
  namespace: sandboxes
spec:
  podSelector:
    matchLabels:
      sandbox.managed: "true"
  policyTypes:
    - Ingress
    - Egress
  ingress:
    # Only allow sandbox-engine to connect to container port 5004
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: sandbox-system
          podSelector:
            matchLabels:
              app.kubernetes.io/name: sandbox-engine
      ports:
        - protocol: TCP
          port: 5004
  egress:
    # 1. Allow CoreDNS (port 53)
    - to:
        - namespaceSelector: {}
          podSelector:
            matchLabels:
              k8s-app: kube-dns
      ports:
        - protocol: UDP
          port: 53
        - protocol: TCP
          port: 53
    # 2. Allow Egress ONLY to Squid Proxy (port 3128)
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: sandbox-system
          podSelector:
            matchLabels:
              app.kubernetes.io/name: squid-proxy
      ports:
        - protocol: TCP
          port: 3128
```
