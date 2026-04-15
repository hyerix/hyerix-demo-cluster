# Aerolux Logistics — scenario

A realistic NATS topology for a fictional international freight / shipping
company. Three regional JetStream clusters (EU, US, APAC) joined by a
full-mesh gateway supercluster, with one leaf-node edge POP per region.
Streams, consumers, KV, object stores, and micro-services are all seeded so
any observability tool — Hyerix Signal AI in particular — has meaningful
production-shaped data to explore within ~90 seconds of connecting.

## Topology

```
     ┌──────────────┐    gateway    ┌──────────────┐
     │  aerolux-eu  │ ◀───────────▶ │  aerolux-us  │
     │  3 JS nodes  │               │  3 JS nodes  │
     └──────┬───────┘               └──────┬───────┘
            │           gateway            │
            │     ┌────────────────┐       │
            └────▶│ aerolux-apac   │◀──────┘
                  │   3 JS nodes   │
                  └──────┬─────────┘
                         │
       leaf ┌────────────┼────────────┐ leaf
            ▼            ▼            ▼
       edge-         edge-         edge-
       frankfurt     virginia      singapore
```

## Streams (10)

| Name | Subjects | Replicas | Retention | Cap | Placement | Notes |
|---|---|---|---|---|---|---|
| `ORDERS_EU`     | `orders.eu.>`     | 3 | 7d  | 512 MB | `aerolux-eu`   | regional workqueue |
| `ORDERS_US`     | `orders.us.>`     | 3 | 7d  | 512 MB | `aerolux-us`   | regional workqueue |
| `ORDERS_APAC`   | `orders.apac.>`   | 3 | 7d  | 512 MB | `aerolux-apac` | regional workqueue |
| `ORDERS_GLOBAL` | *(sourced)*       | 3 | 30d | 2 GB   | `aerolux-eu`   | **sources from** `ORDERS_EU`+`ORDERS_US`+`ORDERS_APAC` |
| `ORDERS_ARCHIVE`| *(mirrored)*      | 3 | 365d| 4 GB   | `aerolux-us`   | **mirror of** `ORDERS_GLOBAL` — cross-region backup |
| `SHIPMENTS`     | `shipments.>`     | 3 | 14d | 512 MB |  | carrier bookings |
| `TRACKING`      | `tracking.>`      | 3 | 7d  | 1 GB   |  | high-volume GPS pings |
| `PAYMENTS`      | `payments.>`      | 3 | 30d | 512 MB |  | payment capture events |
| `AUDIT`         | `audit.>`         | 3 | 90d | 256 MB |  | compliance trail |
| `NOTIFICATIONS` | `notifications.>` | 3 | 24h | 128 MB |  | ephemeral, discard=old |

## Consumers (15)

| Stream | Consumer | Type | Notes |
|---|---|---|---|
| `ORDERS_GLOBAL`  | `processor`        | durable pull  | healthy baseline, ack_wait 10s |
| `ORDERS_GLOBAL`  | `archiver`         | durable pull  | batch, ack_wait 60s |
| `ORDERS_GLOBAL`  | `fraud-check`      | durable pull  | **villain** — pulls, never acks |
| `ORDERS_GLOBAL`  | `realtime-feed`    | durable push  | delivers to `feeds.realtime.orders` |
| `ORDERS_EU`      | `eu-pricing`       | durable pull  | healthy, regional |
| `ORDERS_US`      | `us-pricing`       | durable pull  | healthy, regional |
| `SHIPMENTS`      | `dispatch-router`  | durable push  | delivers to `feeds.dispatch` |
| `SHIPMENTS`      | `customs-clearance`| durable pull  | slightly lagging |
| `TRACKING`       | `event-stream`     | durable push  | high volume |
| `TRACKING`       | `analytics-batch`  | durable pull  | batch every 30s |
| `PAYMENTS`       | `settlement`       | durable pull  | healthy |
| `PAYMENTS`       | `reconciliation`   | push, flow-controlled | idle heartbeat 5s |
| `AUDIT`          | `siem-export`      | durable pull  | slow steady |
| `NOTIFICATIONS`  | `email-sender`     | durable push  | healthy |
| `NOTIFICATIONS`  | `sms-sender`       | durable push  | slightly lagging |

Only `fraud-check` plus the explicitly healthy pulls have live subscribers in
the generator; the rest exist as definitions so they are visible in Hyerix's
topology view, and lag accumulates naturally for the ones that don't pull.

## KV buckets (4)

| Bucket | Keys | History | TTL | Purpose |
|---|---|---|---|---|
| `feature-flags`     | 12 | 10 | — | rollout %, revisions on 2 keys |
| `service-registry`  | 8  | 5  | — | endpoint / version / region / healthy |
| `pricing-config`    | 6  | 20 | — | surge + discount config |
| `customer-sessions` | 60 | 1  | 15m | ephemeral with TTL (demonstrates `max_age`) |

## Object stores (3)

| Bucket | Entries | Content |
|---|---|---|
| `invoices`        | 25 | fake PDFs, 5–50 KB each |
| `product-catalog` | 15 | fake PNGs, 10–200 KB each, with `mime`/`category` metadata |
| `shipping-labels` | 30 | fake PDFs, 5–15 KB each, keyed by tracking id |

## Micro-services (4)

| Name | Version | Endpoint |
|---|---|---|
| `checkout-validate` | 1.4.2 | `checkout.validate` |
| `inventory-check`   | 2.1.0 | `inventory.check`   |
| `pricing-quote`     | 3.0.1 | `pricing.quote`     |
| `fraud-score`       | 0.9.5 | `fraud.score`       |

A client loop sends ~5 requests/sec across the services so their request
counts are non-zero in the Hyerix services view.

## Traffic profile

- **Baseline**: ~10 orders/sec (40% EU, 40% US, 20% APAC). ~30% generate a
  `payments.captured`, ~50% generate 1–3 `tracking.*` events, ~5% generate
  `audit.>` events, ~30% generate a shipment booking. Notifications fire
  ~3/sec.
- **Initial seed** (before `--live`): 500 orders per region = 1,500 orders
  plus derived events.
- **Backlog spike**: at `T+90s`, 5,000 orders pushed to `orders.eu.created`
  over 10s.
- **Services**: ~5 requests/sec split across the 4 services.

## What Signal AI should find

Within ~90 seconds of connecting:

> `ORDERS_GLOBAL.fraud-check` has growing `num_pending` and non-zero
> `num_redelivered`. Its `ack_wait` is 30s and messages are timing out and
> being redelivered instead of acked — typically a downstream service hang.
> The other `ORDERS_GLOBAL` consumers (`processor`, `archiver`) are healthy
> for comparison.

It should also, without being asked:

- Note the `orders.eu.*` backlog spike at around T+90s
- Surface `pricing.dynamic_surge` and `fraud.strict_mode` as KV keys with
  recent revisions
- Report the `aerolux-eu` / `aerolux-us` / `aerolux-apac` clusters as
  healthy gateway peers
- Show `ORDERS_GLOBAL` with source relationships and `ORDERS_ARCHIVE` as a
  cross-region mirror

## Running standalone

```bash
pip install -r requirements.txt
python generate.py --nats-url nats://localhost:24222 --live
```

Flags:

- `--reset` — delete streams / KV / object stores before seeding
- `--live`  — keep baseline + run services + run villain + schedule spike
- `--nats-url` — override the NATS endpoint (default: the in-compose
  regional entry points `nats-eu-1`, `nats-us-1`, `nats-apac-1`)
