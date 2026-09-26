# ADR 0006: Kubernetes deployment and lag-based autoscaling

- **Status:** Accepted
- **Date:** 2026-09-27

## Context

Docker Compose runs everything on one machine, with fixed replica counts and
no resource isolation. Phase 5's load tests showed the consequence: on a
shared machine, adding processors **starved the broker** (ADR 0005). We need
per-workload resource guarantees, safe rollouts, and processors that scale
with the backlog instead of by hand.

## Decisions

### A Helm chart for the app; backing services kept separate

`deploy/helm/event-analytics` deploys **only what we build**: the API, the
processor and the migrations. Postgres, Redpanda, ClickHouse and Redis are
separate manifests in `deploy/k8s/infra/`, **for local clusters only**. In
production they would be managed services or operator-run clusters with
replication and backups. Coupling stateful infrastructure's lifecycle to
application releases is a common and painful mistake.

### Migrations as a Helm hook

- The migrations Job is a `pre-install,pre-upgrade` hook. If it fails, the
  release stops and **no new pods start against an old schema**. On the first
  install it did fail, because of the bug below, and Helm correctly refused
  to roll out.
- **Gotcha:** hooks run *before* the chart's regular resources exist, so the
  Job can't use the release ConfigMap or Secret. It gets hook-scoped copies
  (weight −10), deleted once the hook succeeds.

### Scale pods, not processes

- In Kubernetes the API runs **1 uvicorn worker per pod** (`WEB_CONCURRENCY=1`).
  The scheduler, HPA, probes and rolling updates all operate on pods, so
  scaling is pods × 1. Compose used one container with 4 processes and
  Prometheus multiprocess mode; that isn't needed here.
- **API:** an HPA on CPU (70%), a PodDisruptionBudget (`minAvailable: 1`) so
  node drains can't take ingestion down, and rolling updates with
  `maxUnavailable: 0`.
- **Probes:** a startup probe allows boot time. Liveness checks only that the
  process is up, never dependencies, so a Kafka blip doesn't restart every
  pod. Readiness checks the DB and Kafka, and failing it only removes the pod
  from the Service.

### Processors autoscale on consumer lag (KEDA)

- **CPU is the wrong signal for a consumer.** An idle processor waiting on a
  slow ClickHouse uses no CPU while lag explodes. The right signal is
  **consumer lag**: messages not yet processed.
- KEDA's Kafka scaler computes `replicas = ceil(total lag / lagThreshold)`,
  bounded by min and max. It reads the lag through the Kafka protocol, so it
  works with Redpanda unchanged.
- **Max replicas = partition count** (6 in production). A partition is
  consumed by exactly one member of a group, so extra replicas would sit idle.
  `allowIdleConsumers: false` enforces this.
- **Gotcha: KEDA's `cooldownPeriod` only governs scaling to zero.** Scaling
  down between min and max is done by the HPA that KEDA creates, whose
  default scale-down stabilization window is **300 seconds**. In the first
  demo, processors stayed at 3 for over 4 minutes after the lag cleared. The
  window is now an explicit setting (300 s in production, 60 s on the
  laptop), and replicas are removed **one at a time** every 30 seconds.
- Graceful shutdown: `terminationGracePeriodSeconds: 60`. On SIGTERM the
  processor finishes its batch, commits, and leaves the group, so partitions
  move immediately.

### The service-link gotcha

The first migration Job crashed with:

```
clickhouse_port: Input should be a valid integer ... input_value='tcp://10.96.168.173:8123'
```

For every Service in a namespace, Kubernetes injects legacy **service-link**
environment variables into each pod, in the form `<SERVICE>_PORT=tcp://ip:port`.
The `clickhouse` Service therefore produced `CLICKHOUSE_PORT`, which collided
with our setting of the same name. All app pods now set
`enableServiceLinks: false`, since we use DNS names. This kind of bug never
shows up in Compose.

### Advertised broker address must be fully qualified

Redpanda advertises `redpanda.analytics.svc.cluster.local:9092`. KEDA runs in
the `keda` namespace, where the short name `redpanda` doesn't resolve.
Clients connect to the address the broker hands back in metadata, not the
bootstrap address.

### Pod security

All app pods run non-root (UID 1000) with a **read-only root filesystem**
(an `emptyDir` is mounted at `/tmp` for heartbeats), drop all capabilities,
disallow privilege escalation, and use the `RuntimeDefault` seccomp profile.

## Verified on a local kind cluster (8 GB laptop)

1. `make k8s-up` creates the cluster, installs metrics-server and KEDA,
   starts the backing services, builds and loads the image, and installs the
   chart. Migrations ran as the hook and pods became ready.
2. End to end through the cluster: ingest via the API Service, stored by the
   processor, and a funnel query returns the events.
3. **Autoscaling cycle** (`make k8s-burst`, an in-cluster k6 Job,
   **129,000 events, 0 failures**):

   | t (s) | Processor replicas | Consumer lag |
   |---|---|---|
   | 0 | 1 | 0 |
   | 18 | 1 | 17,122 |
   | 37 | **3** (KEDA: "above target") | 56,193 |
   | 55 | 3 | 75,421 |
   | 79 | 3 | **0** |
   | after tuning | 3 → 2 → 1, 37 s apart ("All metrics below target") | 0 |

   The API's CPU-based HPA also scaled from 1 to 2 pods during the burst.
4. The whole cluster (control plane, KEDA, metrics-server, backing services
   and app) used **2.0 GB**.

## Consequences

- Local Kubernetes needs the Compose stack stopped, because memory is tight
  on 8 GB.
- Production still needs managed backing services, an ingress or load
  balancer, a secret manager (`existingSecret`), Prometheus Operator
  ServiceMonitors instead of annotations, and topology spread across nodes
  and zones. The chart's values leave room for each.
