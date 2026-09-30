# Queue service

The API accepts email and subscription jobs into a shared Redis-backed BullMQ queue. Run API/WebSocket replicas separately from worker replicas. Each API replica serves the dashboard, JSON API, and `/ws` on the same port; connections are not session-affine, and a reconnect receives a fresh snapshot from Redis.

![Uploading image.png…]()

## Run locally

Requirements: Node.js 20+ and Docker Compose.

```sh
npm ci
docker compose up --build
```

Open http://localhost:3000. The API accepts `POST /api/email` with `{ "to", "subject", "text" }` and `POST /api/subscriptions` with `{ "email" }`. Both respond with `202` after Redis accepts the job. Subscription duplicates return `409`. Email requests accept an optional `Idempotency-Key` header; reuse the same key when retrying an HTTP request.

## Verify

With the Compose stack running, execute:

```sh
npm run check
npm run verify
```

The verification script checks health, WebSocket snapshots, worker completion, subscription duplicate handling, validation, and the dashboard. It submits 500 synthetic jobs at up to 50 concurrent HTTP requests and waits for the queue to drain. These local results are a smoke test, not a production capacity guarantee. The dashboard button runs the same type of synthetic test, with a maximum of 500 requests per run. Test jobs are only accepted when `ENABLE_QUEUE_TEST=true`; Compose enables this for local development and the Kubernetes example disables it.

The dashboard's queue test button submits synthetic `queue-test` jobs through the subscription route with `X-Queue-Test: true`; workers process them without delivering mail or creating subscriber records. Compose enables this with `ENABLE_QUEUE_TEST=true`; it is disabled in the Kubernetes example. The control measures bounded browser-side HTTP concurrency and is a smoke/load probe, not a substitute for a coordinated multi-client load test.

Email jobs fail promptly until SMTP settings are set. Provide `SMTP_HOST`, `SMTP_PORT`, `SMTP_SECURE`, `SMTP_USER`, `SMTP_PASS`, and `SMTP_FROM` to the worker. Do not commit credentials.

## Scale out

Build and push the image, replace both `your-registry/relay:1.0.0` image references in `kubernetes.yaml` with that image, and provision `relay-secrets` through your cluster's secret manager with `redis-url`, `smtp-host`, `smtp-from`, `smtp-user`, and `smtp-password`. Do not put production credentials in shell history or source control. `REDIS_URL` should point to a production Redis service with authentication, TLS, persistence, backups, and a tested failover policy. Apply the manifest, then put an ingress/load balancer in front of the `relay-api` Service with WebSocket upgrades enabled:

```sh
docker build -t your-registry/relay:1.0.0 .
docker push your-registry/relay:1.0.0
kubectl apply -f kubernetes.yaml
```

The Kubernetes manifest is a deployment template, not a complete cloud environment: provide the image, secret, production Redis, and an ingress/load balancer for your cluster before applying it. Do not expose the Redis service publicly.

The sample starts five API pods with a 5,000-connection per-pod ceiling (a configuration limit, not a measured capacity claim) and three worker pods. Scale API/WebSocket and worker capacity independently after load testing:

```sh
kubectl scale deployment/relay-api --replicas=3
kubectl scale deployment/relay-worker --replicas=10
```

`WORKER_CONCURRENCY` is per worker pod (default 20). Tune it to the SMTP provider's sending limits, Redis capacity, CPU/memory, and measured job latency. Do not set it to 20,000 on one process. A 20,000-job burst is queued durably and drained at the sustainable downstream rate; no application can guarantee all mail is delivered if the provider rejects, throttles, or permanently bounces it. BullMQ retries transient failures and retains exhausted jobs for inspection. SMTP cannot guarantee exactly-once delivery if a worker crashes after the provider accepts a message but before Redis records completion.

The dashboard reads at most ten recent jobs and API replicas refresh the shared snapshot once per second, broadcasting only when it changes. Slow WebSocket clients are disconnected and can reconnect for a fresh snapshot. Set `MAX_WS_CLIENTS` based on load-tested per-pod memory and file-descriptor limits. Queue depth, worker failures, Redis latency/availability, provider throttles, and active WebSocket counts should be monitored before increasing replicas.

The Compose Redis volume is for local development, not high availability. Production should use managed/clustered Redis with persistence and tested backups/failover; use load tests with the actual Redis, ingress, SMTP provider, message sizes, and target client counts before claiming 100,000-request or 20,000-client capacity.
