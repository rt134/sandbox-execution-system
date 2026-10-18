# Security & SSRF Mitigation

Executing untrusted AI-generated code requires **defense-in-depth across the kernel, network, and filesystem**. The Sandbox Execution System combines gVisor userspace kernel isolation with strict egress proxy filtering.

---

## 1. Threat Model & Mitigations

| Threat | Attack Vector | System Defense |
| :--- | :--- | :--- |
| **Container Breakout** | Linux kernel 0-day exploit / privilege escalation | **gVisor (`runsc`)**: Untrusted code never executes syscalls directly on the host kernel. All calls are trapped in Go userspace. |
| **SSRF (Cloud Metadata)** | Code queries `http://169.254.169.254` to steal AWS/GCP IAM roles | **Squid Egress Proxy**: Explicitly denies `169.254.169.254/32` and `169.254.0.0/16`. |
| **Internal Lateral Movement** | Code scans internal VPC (`10.0.0.0/8`, `192.168.0.0/16`) or K8s API | **Squid + NetworkPolicy**: Blocks all RFC 1918 private subnets and K8s Service CIDRs (`10.96.0.0/12`). |
| **K8s Cluster Compromise** | Container accesses `/var/run/secrets/kubernetes.io` | **Pod Spec Lockdown**: `automountServiceAccountToken: false` is enforced on all sandbox Pods. |
| **Host Workload Contamination** | Sandbox co-located on nodes running DB or Core APIs | **Dedicated Tainted Node Pool**: Nodes tainted `sandbox=true:NoSchedule`. |
| **Host Filesystem Traversal** | Agent crafts `../../etc/shadow` path in file APIs | **Path Sanitization**: `shellctl` enforces strict prefix validation against `/workspace`. |

---

## 2. Kernel Isolation: gVisor (`runsc`)

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

In production, sandboxes specify `runtime: runsc` (in Docker) or `runtimeClassName: gvisor` (in Kubernetes).

---

## 3. Network Egress Firewall: Squid SSRF Proxy

All sandboxes are placed on an isolated network where direct outbound routing to the internet is blocked. Outbound HTTP/HTTPS traffic must traverse the Squid proxy (`deploy/squid/squid.conf`):

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

## 4. Kubernetes `NetworkPolicy`

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: sandbox-isolation
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
        - podSelector:
            matchLabels:
              app: sandbox-engine
      ports:
        - protocol: TCP
          port: 5004
  egress:
    # Only allow DNS (port 53) and the Squid proxy (port 3128)
    - to:
        - podSelector:
            matchLabels:
              app: squid-proxy
      ports:
        - protocol: TCP
          port: 3128
    - ports:
        - protocol: UDP
          port: 53
```
