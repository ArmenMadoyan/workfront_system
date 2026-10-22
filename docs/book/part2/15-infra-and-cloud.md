# Chapter 15 — Infrastructure & Cloud

## In our system (local)
Everything runs via Docker Compose + a local launcher (`main.py`): Postgres
(Homebrew, `wal_level=logical`), Kafka (KRaft, no Zookeeper), Debezium/Kafka
Connect, Redis, Jaeger, Kafka-UI, RedisInsight. App services run as processes
(api with multiple uvicorn workers; a worker fleet; ws-fanout). This is a faithful
*shape* of production, scaled down.

## Local → production mapping (AWS as example)

| Local | AWS managed | Why |
|---|---|---|
| Postgres (Homebrew) | **Aurora PostgreSQL** | managed HA, read replicas, autoscaling storage |
| Kafka (KRaft) | **MSK** (or Confluent Cloud) | managed brokers, scaling, durability |
| Debezium on Connect | **MSK Connect** | managed CDC connectors |
| Redis | **ElastiCache (Redis)** | managed, clustered |
| OpenSearch | **Amazon OpenSearch Service** | managed search/read model |
| App processes | **EKS** (k8s) pods / ECS / Fargate | orchestration, autoscaling, rollouts |
| Jaeger/metrics | **Datadog** / Grafana Cloud / AWS X-Ray | managed observability |
| Object storage | **S3** | attachments, backups, data lake |
| Secrets/config | **Secrets Manager / SSM** | no secrets in images (12-factor) |

The point isn't AWS specifically — it's that **every component has a managed
equivalent**, and you'd buy the undifferentiated heavy lifting (brokers, DB HA,
search) and keep your own code in containers.

## Kubernetes & autoscaling
- **Deployments** for stateless services (api, worker, ws-fanout) — trivially
  horizontally scalable because they hold no local state (12-factor).
- **HPA** (Horizontal Pod Autoscaler) on CPU/latency for the API.
- **KEDA** for the workers — scale on **Kafka consumer lag** (the right signal:
  scale workers when the cascade backlog grows, scale to zero when idle). This is
  the cloud-native version of "the worker tier never bottlenecked, just add more."
- **StatefulSets / managed services** for stores (or use the managed cloud ones).
- **Service mesh** (Istio/Linkerd) for mTLS, retries, and traffic shaping; the
  OTel collector runs as a **sidecar** or DaemonSet.

## Resource right-sizing (tie to our numbers)
We measured ~582 moves/s on one box with the worker tier idle. So for 10M/day
(~116/s avg): a few small API pods behind an HPA, a handful of worker pods behind
KEDA on lag, a modest Aurora instance + a read replica, a small MSK cluster with
enough partitions for cascade parallelism. **Over-provisioned for the target** —
the spend goes to HA/redundancy, not raw throughput.

## Cost & reliability levers
- **Multi-AZ** for Aurora/MSK/ElastiCache (HA).
- **Partitions** sized for peak cascade concurrency, not average QPS.
- **Autoscale on the right metric** (lag for workers, latency/CPU for API).
- **Spot/Fargate** for stateless workers to cut cost.

## Interview soundbites

- "Stateless services + **KEDA scaling workers on Kafka consumer lag** — scale on
  the backlog signal, not CPU."
- "Every local component has a **managed equivalent** (Aurora, MSK, ElastiCache,
  OpenSearch); I buy the undifferentiated parts and keep my code in **EKS**."
- "Sizing is driven by our measured ~582/s headroom — for 10M/day the spend is
  about **HA and partitions**, not throughput."

## Likely follow-ups

- *"How do you autoscale the workers?"* → KEDA on consumer lag; scale to zero when
  idle, burst when a cascade storm hits.
- *"Self-host Kafka or managed?"* → Managed (MSK/Confluent) unless you have a
  platform team; brokers are undifferentiated heavy lifting.
- *"How do you do zero-downtime deploys with a schema change?"* → Expand/contract
  migrations + rolling deploys (Ch. 16).
