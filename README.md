# hyerix-demo-cluster

**Runnable NATS JetStream demo cluster for Hyerix. A realistic multi-region supercluster modelled after a multinational freight company — Aerolux Logistics. Spin it up in one command.**

> [!IMPORTANT]
> **Hyerix is launching today on Product Hunt.** Vote, comment, or share feedback: [producthunt.com/products/hyerix](https://www.producthunt.com/products/hyerix)

<p align="center">
  <img alt="Platforms" src="https://img.shields.io/badge/platforms-macOS%20%7C%20Linux%20%7C%20Windows-1f2937">
  &nbsp;
  <img alt="Requires Docker" src="https://img.shields.io/badge/requires-Docker-06b6d4">
  &nbsp;
  <img alt="License MIT" src="https://img.shields.io/badge/license-MIT-737373">
</p>

This repository spins up a 3-region NATS JetStream supercluster (9 hub nodes + 3 edge POPs) and seeds it with the shape of data a real international freight company would run on NATS: regional order streams, a global sourcing stream, a cross-region archive mirror, KV buckets, object stores, micro-services, and a deliberately broken consumer so any observability tool has something interesting to diagnose.

It was built to give [Hyerix](https://hyerix.ai) trial users an immediate "aha" moment with **Signal AI**, but it's equally useful as a standalone dev environment for learning NATS superclusters, testing clients, or evaluating monitoring tools.

## Quick start

```bash
git clone https://github.com/hyerix/hyerix-demo-cluster
cd hyerix-demo-cluster
docker compose up -d
```

First run takes 2–3 minutes while the `nats:2.12-alpine` and `python:3.11-slim` images are pulled. After that it comes up in ~30 seconds.

You now have:

- **3 regional clusters** — `aerolux-eu`, `aerolux-us`, `aerolux-apac` — 9 JetStream nodes total, joined by a full-mesh gateway supercluster
- **3 edge POPs** — `edge-frankfurt`, `edge-virginia`, `edge-singapore` — one leaf node per region
- **10 streams**, **15 consumers**, **4 KV buckets**, **3 object stores**, **4 micro-services**
- **Live traffic** — ~10 orders/sec baseline, a villain consumer, and a 5000-message backlog spike at T+90s

Watch the generator:

```bash
docker compose logs -f generator
```

Connect any NATS client to `nats://localhost:24222` and the rest of the topology is auto-discovered via INFO frames.

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

## What gets created

See [scenarios/aerolux/scenario.md](scenarios/aerolux/scenario.md) for the full spec. Highlights:

- `ORDERS_EU`, `ORDERS_US`, `ORDERS_APAC` — regional workqueues with placement pinned to their home cluster
- `ORDERS_GLOBAL` — sources from all three regionals (visible as a topology relation)
- `ORDERS_ARCHIVE` — mirrors `ORDERS_GLOBAL`, lives in a *different* region (`aerolux-us`) as a cross-region backup
- `SHIPMENTS`, `TRACKING`, `PAYMENTS`, `AUDIT`, `NOTIFICATIONS` — domain streams with diverse retention / cap / discard policies
- 15 consumers — mix of push / pull / ordered / durable / healthy / lagging / one villain
- 4 KV buckets including `customer-sessions` with a 15-minute TTL
- 3 object stores seeded with fake PDFs and PNGs
- 4 NATS micro-services (`checkout-validate`, `inventory-check`, `pricing-quote`, `fraud-score`) with real request traffic

## Connect Hyerix to the demo cluster

[Hyerix](https://hyerix.ai?utm_source=github&utm_medium=readme&utm_campaign=demo-cluster) is an AI-native desktop GUI for NATS infrastructure. If you have it installed:

1. Open the connection manager
2. Choose **Import from Hyerix file**
3. Pick [`.hyerix/demo-cluster.hyerix`](.hyerix/demo-cluster.hyerix) from this repository

Or create a new connection pointing to `nats://localhost:24222` — no authentication required.

Within ~90 seconds, ask Signal AI:

```
why is ORDERS_GLOBAL.fraud-check falling behind?
```

You should get a concrete diagnosis about `num_pending` growth, redelivery, and `ack_wait` timeouts — with `processor` / `archiver` called out as healthy for comparison.

Don't have Hyerix? [Start a free 14-day trial](https://hyerix.ai?utm_source=github&utm_medium=readme&utm_campaign=demo-cluster#download) — macOS, Windows, and Linux, no signup required.

## Run the generator standalone

If you have Python and a NATS instance already running, you can run the generator directly:

```bash
cd scenarios/aerolux
pip install -r requirements.txt
python generate.py --nats-url nats://localhost:24222 --live
```

Flags:

- `--reset` — delete all Aerolux streams / KV / object stores before seeding
- `--live`  — keep publishing baseline traffic + run the villain + scheduled spike + services
- `--nats-url` — override the NATS endpoint (comma-separated for multi-server)

## Stop and clean up

```bash
docker compose down           # stop containers, keep volumes
docker compose down -v        # stop and delete all demo data
```

## Requirements

- Docker Desktop (or Docker Engine + compose plugin). That's it.
- If you want to run the generator standalone, Python 3.11+ with `nats-py>=2.7`.

## Ports used

| Port range | Purpose |
|---|---|
| `24222–24224` | EU region client ports (`nats-eu-1..3`) |
| `24232–24234` | US region client ports (`nats-us-1..3`) |
| `24242–24244` | APAC region client ports (`nats-apac-1..3`) |
| `24252–24254` | Edge POPs (`edge-frankfurt`, `edge-virginia`, `edge-singapore`) |
| `28222–28254` | HTTP monitor endpoints (same numbering + 4000) |

Hyerix auto-discovers the rest of the supercluster from any entry point, so you only need `24222` to get in. Internal routing / gateway / leafnode ports (`6222`, `7222`, `7422`) are not published to the host.

## Layout

```
hyerix-demo-cluster/
  docker-compose.yml           # 13 services: 9 hub + 3 edge + generator
  config/
    nats-eu.conf               # regional cluster configs
    nats-us.conf
    nats-apac.conf
    leaf-frankfurt.conf        # edge POP configs
    leaf-virginia.conf
    leaf-singapore.conf
  scenarios/
    aerolux/
      Dockerfile
      generate.py              # seed + --live generator
      requirements.txt
      scenario.md
  .hyerix/
    demo-cluster.hyerix        # pre-built connection file for Hyerix
  LICENSE
  README.md
```

## Related

- **[hyerix.ai](https://hyerix.ai?utm_source=github&utm_medium=readme&utm_campaign=demo-cluster)** — the desktop app this scenario was built for
- **[docs.hyerix.ai](https://docs.hyerix.ai)** — Hyerix documentation, including Signal AI prompt patterns
- **[nats-io/nats-server](https://github.com/nats-io/nats-server)** — the NATS server and JetStream implementation
- **[nats-io/nats.py](https://github.com/nats-io/nats.py)** — the Python client used by the generator

## License

MIT — see [LICENSE](LICENSE). This demo stack is meant to be forked, copied, and modified freely. The Hyerix desktop application itself is commercial software, separately licensed — see [hyerix.ai/terms](https://hyerix.ai/terms).

*Aerolux Logistics is a fictional company invented for this demo; no resemblance to any real freight operator is intended.*
