# Security & Zero-Trust Architecture

Executing untrusted AI-generated code requires **defense-in-depth across the kernel, network, authentication, and filesystem boundaries**. The Sandbox Execution System combines gVisor userspace kernel isolation with strict egress proxy filtering, token-based daemon authentication, path canonicalization, and cgroup resource limits.

---

## 1. Threat Model & Mitigations

| Threat | Attack Vector | System Defense |
| :--- | :--- | :--- |
| **Container Breakout** | Linux kernel 0-day exploit / privilege escalation | **gVisor (`runsc`)**: Untrusted code never executes syscalls directly on the host kernel. All calls are trapped in Go userspace. Silent fallback is disabled (`allow_runtime_fallback=False`). |
| **Privilege Escalation** | Code attempts `setuid` binaries (`su`, `sudo`, `ping`) | **Capabilities & Privileges**: All Linux capabilities are dropped (`cap_drop=["ALL"]`) and `no-new-privileges:true` is enforced on every container. |
| **Fork Bomb (Host DoS)** | `:(){ :|:& };:` or `os.fork()` recursion exhausting host PID table | **cgroups PID Controller (`pids_limit`)**: Containers are capped at 256 PIDs. Any attempt to fork beyond limit returns `EAGAIN` ("Resource temporarily unavailable"). |
| **Disk Exhaustion (Host DoS)**| `cat /dev/zero > file` or infinite log generation | **Disk Quota Enforcement**: `shellctl` checks workspace disk usage before every write and rejects payloads exceeding quota with `HTTP 413 Payload Too Large`. |
| **Host Filesystem Traversal** | Agent crafts `workspace_id="../.."` or `../../etc/shadow` | **Path Traversal Defense**: `workspace_id` is validated against `^[a-zA-Z0-9_\-]{1,64}$` and canonical `os.path.realpath` is verified against `workspaces_root`. |
| **SSRF (Cloud Metadata)** | Code queries `http://169.254.169.254` to steal AWS/GCP IAM roles | **Squid Egress Proxy**: Explicitly denies `169.254.169.254/32` and `169.254.0.0/16`. Sandboxes inherit `http_proxy`/`https_proxy` environment variables. |
| **Lateral Movement Between Sandboxes** | Malicious code in Sandbox A attacks daemon on Sandbox B | **Internal Daemon Token Authentication**: Every sandbox is assigned a cryptographically unique `SHELLCTL_AUTH_TOKEN`. `shellctl` rejects requests without `Authorization: Bearer <token>`. |
| **Unauthenticated API Access** | Open access to Engine REST and WebSocket endpoints | **API Key Authentication**: Engine validates `Authorization: Bearer <key>` or `X-API-Key` on all endpoints and WebSocket handshakes. |
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
