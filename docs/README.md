# Sandbox Execution System Documentation

Welcome to the comprehensive documentation for the **Sandbox Execution System**—a high-performance, security-hardened execution platform for **stateless code execution (<50ms latency)** and **stateful AI agent sandboxes**.

---

## Documentation Index

| Guide | Description |
| :--- | :--- |
| **[1. Architecture & Drivers](architecture-and-drivers.md)** | Deep-dive into the dual execution paradigms, the Pluggable Driver Architecture (`DockerDriver` vs `KubernetesDriver`), Warm Pool design, and Sweeper GC. |
| **[2. REST & WebSocket API Reference](api-reference.md)** | Complete specification of all endpoints exposed by `sandbox-engine` on port `8080`, with full JSON schemas, streaming formats, and curl examples. |
| **[3. Python SDK Reference](sdk-reference.md)** | Complete documentation of `sandbox-client`, including `SandboxClient`, `SandboxSession`, file system client, PTY terminal client, and agent tool adapters. |
| **[4. In-Sandbox Daemon (`shellctl`)](daemon-shellctl.md)** | Technical specification of the static Go kernel daemon listening on port `5004` inside every container. |
| **[5. Configuration & Environment Variables](configuration.md)** | Exhaustive reference for all configuration options, environment variables, default values, and secrets. |
| **[6. Security & SSRF Mitigation](security-and-networking.md)** | Kernel isolation via gVisor (`runsc`), network isolation, and Squid SSRF egress proxy ACLs. |
| **[7. Deployment Guide](deployment.md)** | Step-by-step production guides for deploying on single-host Docker Compose and multi-node Kubernetes clusters. |

---

## Quick Navigation

* **Calling the sandbox from Python?** Check out the [Python SDK Reference](sdk-reference.md).
* **Calling the sandbox from Node, Go, or cURL?** Check out the [REST & WebSocket API Reference](api-reference.md).
* **Deploying to production on Kubernetes?** Read the [Deployment Guide](deployment.md) and [Security Guide](security-and-networking.md).
* **Customizing the container runtime or pool size?** See the [Configuration Reference](configuration.md).
