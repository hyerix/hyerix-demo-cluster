#!/usr/bin/env python3
"""
Aerolux Logistics — multi-region NATS JetStream scenario generator.

Seeds a 3-region / 9-node supercluster (plus 3 edge POPs) with the resources
a real multinational freight company would have on NATS:

  - Regional ORDERS_* streams with placement pinned per cluster
  - A global ORDERS_GLOBAL stream sourcing from all three
  - An ORDERS_ARCHIVE mirror in a different region (365d retention)
  - Domain streams: SHIPMENTS, TRACKING, PAYMENTS, AUDIT, NOTIFICATIONS
  - 15 consumers with a deliberate mix of push/pull/ordered and one villain
  - 4 KV buckets (feature flags, service registry, pricing, customer sessions)
  - 3 Object stores seeded with fake invoice / product / label blobs
  - 4 micro-services (checkout-validate, inventory-check, pricing-quote,
    fraud-score) responding to requests with small JSON bodies
  - Live traffic at ~10 orders/sec with a scheduled backlog spike at T+90s

Usage:
  python generate.py                   # one-shot seed and exit
  python generate.py --live            # keep publishing + run services + spike
  python generate.py --reset --live    # wipe and reseed
  python generate.py --nats-url nats://localhost:24222 --live
"""

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

try:
    import nats
    from nats.js import api
    from nats.errors import TimeoutError as NatsTimeoutError
except ImportError:
    print(
        "ERROR: nats-py is not installed. Run: pip install nats-py>=2.7",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    from nats.micro import add_service
    HAVE_MICRO = True
except ImportError:
    HAVE_MICRO = False


# ─── Company constants ───────────────────────────────────────────────────────

COMPANY_NAME = "Aerolux Logistics"
REGIONS = ["eu", "us", "apac"]
CLUSTERS = {"eu": "aerolux-eu", "us": "aerolux-us", "apac": "aerolux-apac"}

# Traffic knobs
BASELINE_RATE_PER_SEC = 10
REGION_WEIGHTS = {"eu": 0.4, "us": 0.4, "apac": 0.2}
SERVICE_REQUEST_RATE = 5      # /sec total across services
SEED_INITIAL_PER_REGION = 500

# Villain
FRAUD_ACK_WAIT_SEC = 30

# Spike
SPIKE_DELAY_SEC = 90
SPIKE_TOTAL_MSGS = 5000
SPIKE_DURATION_SEC = 10

# Domain data — realistic-ish
CARRIERS = ["dhl", "fedex", "ups", "maersk", "cma-cgm", "hapag-lloyd"]
INCOTERMS = ["DDP", "DAP", "FCA", "EXW", "CIF", "FOB"]
COMMODITIES = [
    "electronics", "apparel", "pharma", "automotive-parts",
    "industrial-machinery", "refrigerated-food", "chemicals", "furniture",
]
PORTS = {
    "eu":   ["hamburg", "rotterdam", "antwerp", "le-havre", "felixstowe"],
    "us":   ["los-angeles", "long-beach", "new-york", "savannah", "seattle"],
    "apac": ["shanghai", "singapore", "busan", "yokohama", "hong-kong"],
}
CURRENCIES_BY_REGION = {"eu": "EUR", "us": "USD", "apac": "SGD"}


# ─── Payload builders ────────────────────────────────────────────────────────


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def pick_region() -> str:
    r = random.random()
    acc = 0.0
    for region, weight in REGION_WEIGHTS.items():
        acc += weight
        if r < acc:
            return region
    return "eu"


def make_order(region: str, spike: bool = False) -> dict:
    origin, dest = random.sample(PORTS[region], 2)
    return {
        "order_id": f"ord_{uuid.uuid4().hex[:20]}",
        "region": region,
        "customer": {
            "id": f"cust_{random.randint(10000, 99999)}",
            "email": f"ops{random.randint(1000, 9999)}@aeroluxlogistics.example",
            "tier": random.choice(["bronze", "silver", "gold", "platinum"]),
        },
        "shipment": {
            "origin_port": origin,
            "destination_port": dest,
            "carrier": random.choice(CARRIERS),
            "incoterm": random.choice(INCOTERMS),
            "commodity": random.choice(COMMODITIES),
            "weight_kg": random.randint(50, 25000),
            "container_type": random.choice(["20ft", "40ft", "40hc", "reefer"]),
        },
        "total_cents": random.randint(50_00, 500_000_00),
        "currency": CURRENCIES_BY_REGION[region],
        "placed_at": iso_now(),
        "spike": spike,
    }


def make_payment(order_id: str, region: str) -> dict:
    return {
        "payment_id": f"pay_{uuid.uuid4().hex[:20]}",
        "order_id": order_id,
        "amount_cents": random.randint(50_00, 500_000_00),
        "currency": CURRENCIES_BY_REGION[region],
        "status": "captured",
        "processor": random.choice(["stripe", "adyen", "worldpay"]),
        "captured_at": iso_now(),
    }


def make_tracking(order_id: str) -> dict:
    return {
        "tracking_id": f"trk_{uuid.uuid4().hex[:16]}",
        "order_id": order_id,
        "status": random.choice(
            ["picked_up", "in_transit", "customs_hold", "out_for_delivery", "delivered"]
        ),
        "lat": round(random.uniform(-60, 70), 4),
        "lon": round(random.uniform(-180, 180), 4),
        "at": iso_now(),
    }


def make_audit(event: str, details: dict) -> dict:
    return {
        "event": event,
        "actor": random.choice(["system", "ops-bot", "admin@aerolux"]),
        "details": details,
        "at": iso_now(),
    }


def make_notification(order_id: str) -> dict:
    return {
        "order_id": order_id,
        "channel": random.choice(["email", "sms", "push"]),
        "template": random.choice(["order_placed", "shipment_update", "delivered"]),
        "at": iso_now(),
    }


# ─── Connection + reset ──────────────────────────────────────────────────────


async def connect_with_retry(url: str, max_attempts: int = 45) -> "nats.NATS":
    """Tolerate NATS containers that aren't ready yet on first compose up."""
    servers = [s.strip() for s in url.split(",") if s.strip()]
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            nc = await nats.connect(
                servers=servers,
                connect_timeout=5,
                max_reconnect_attempts=-1,
            )
            if attempt > 1:
                print(f"  ✓ connected on attempt {attempt}")
            return nc
        except Exception as e:
            last_err = e
            print(f"  ⏳ waiting for NATS (attempt {attempt}/{max_attempts}): {e}")
            await asyncio.sleep(2)
    raise RuntimeError(f"could not reach NATS at {url}: {last_err}")


ALL_STREAMS = [
    "ORDERS_ARCHIVE",   # delete mirror first (depends on source)
    "ORDERS_GLOBAL",    # then sourcing stream
    "ORDERS_EU",
    "ORDERS_US",
    "ORDERS_APAC",
    "SHIPMENTS",
    "TRACKING",
    "PAYMENTS",
    "AUDIT",
    "NOTIFICATIONS",
]
ALL_KVS = ["feature-flags", "service-registry", "pricing-config", "customer-sessions"]
ALL_OBJECT_STORES = ["invoices", "product-catalog", "shipping-labels"]


async def reset_cluster(js) -> None:
    """Delete existing demo streams/KV/object stores so we start clean."""
    print("↻ Resetting existing Aerolux resources...")
    for name in ALL_STREAMS:
        try:
            await js.delete_stream(name)
            print(f"  deleted stream {name}")
        except Exception:
            pass
    for name in ALL_KVS:
        try:
            await js.delete_key_value(name)
            print(f"  deleted KV {name}")
        except Exception:
            pass
    for name in ALL_OBJECT_STORES:
        try:
            await js.delete_object_store(name)
            print(f"  deleted object store {name}")
        except Exception:
            pass


# ─── Stream creation ─────────────────────────────────────────────────────────


async def _safe_add_stream(js, cfg: api.StreamConfig) -> None:
    """Create a stream idempotently — if it exists, update it in place."""
    try:
        await js.add_stream(config=cfg)
    except Exception as e:
        msg = str(e).lower()
        if "already in use" in msg or "exists" in msg or "different config" in msg:
            try:
                await js.update_stream(config=cfg)
                return
            except Exception as e2:
                print(f"  ! could not update stream {cfg.name}: {e2}", file=sys.stderr)
        else:
            raise


async def create_streams(js) -> None:
    print("→ Creating streams...")

    # Regional workqueue streams (one per region), placement pinned per cluster
    regional = [
        ("ORDERS_EU",   "orders.eu.>",   "aerolux-eu"),
        ("ORDERS_US",   "orders.us.>",   "aerolux-us"),
        ("ORDERS_APAC", "orders.apac.>", "aerolux-apac"),
    ]
    for name, subj, cluster in regional:
        await _safe_add_stream(js, api.StreamConfig(
            name=name,
            subjects=[subj],
            retention=api.RetentionPolicy.LIMITS,
            storage=api.StorageType.FILE,
            num_replicas=3,
            max_age=7 * 86400,
            max_bytes=512 * 1024 * 1024,
            discard=api.DiscardPolicy.OLD,
            placement=api.Placement(cluster=cluster),
        ))
        print(f"  ✓ {name} (R=3, 7d, 512MB, placement={cluster})")

    # Global sourcing stream — sources from all 3 regionals, lives in EU
    await _safe_add_stream(js, api.StreamConfig(
        name="ORDERS_GLOBAL",
        retention=api.RetentionPolicy.LIMITS,
        storage=api.StorageType.FILE,
        num_replicas=3,
        max_age=30 * 86400,
        max_bytes=2 * 1024 * 1024 * 1024,
        sources=[
            api.StreamSource(name="ORDERS_EU"),
            api.StreamSource(name="ORDERS_US"),
            api.StreamSource(name="ORDERS_APAC"),
        ],
        placement=api.Placement(cluster="aerolux-eu"),
    ))
    print("  ✓ ORDERS_GLOBAL (R=3, 30d, 2GB, sources EU+US+APAC, placement=aerolux-eu)")

    # Archive mirror — lives in a different region (US) from source (EU)
    await _safe_add_stream(js, api.StreamConfig(
        name="ORDERS_ARCHIVE",
        retention=api.RetentionPolicy.LIMITS,
        storage=api.StorageType.FILE,
        num_replicas=3,
        max_age=365 * 86400,
        max_bytes=4 * 1024 * 1024 * 1024,
        mirror=api.StreamSource(name="ORDERS_GLOBAL"),
        placement=api.Placement(cluster="aerolux-us"),
    ))
    print("  ✓ ORDERS_ARCHIVE (R=3, 365d, 4GB, mirror of ORDERS_GLOBAL, placement=aerolux-us)")

    # Other domain streams. PAYMENTS uses republish to fan its captures out
    # to audit.payments.> so AUDIT picks them up automatically — a visible
    # republish relation in the Hyerix stream details view.
    domain = [
        ("SHIPMENTS",     ["shipments.>"],     14 * 86400,  512 * 1024 * 1024, api.DiscardPolicy.OLD, None),
        ("TRACKING",      ["tracking.>"],       7 * 86400, 1024 * 1024 * 1024, api.DiscardPolicy.OLD, None),
        ("PAYMENTS",      ["payments.>"],      30 * 86400,  512 * 1024 * 1024, api.DiscardPolicy.OLD,
            api.RePublish(src="payments.>", dest="audit.payments.>")),
        ("AUDIT",         ["audit.>"],         90 * 86400,  256 * 1024 * 1024, api.DiscardPolicy.OLD, None),
        ("NOTIFICATIONS", ["notifications.>"],      86400,  128 * 1024 * 1024, api.DiscardPolicy.OLD, None),
    ]
    for name, subj, age, size, discard, republish in domain:
        await _safe_add_stream(js, api.StreamConfig(
            name=name,
            subjects=subj,
            retention=api.RetentionPolicy.LIMITS,
            storage=api.StorageType.FILE,
            num_replicas=3,
            max_age=age,
            max_bytes=size,
            discard=discard,
            republish=republish,
        ))
        print(f"  ✓ {name}")


# ─── Consumers ───────────────────────────────────────────────────────────────


async def _safe_add_consumer(js, stream: str, cfg: api.ConsumerConfig) -> None:
    try:
        await js.add_consumer(stream=stream, config=cfg)
    except Exception as e:
        if "already in use" in str(e).lower() or "exists" in str(e).lower():
            return
        print(f"  ! consumer {cfg.durable_name}: {e}", file=sys.stderr)


async def create_consumers(js) -> None:
    print("→ Creating consumers...")

    # ORDERS_GLOBAL
    await _safe_add_consumer(js, "ORDERS_GLOBAL", api.ConsumerConfig(
        durable_name="processor",
        description="Healthy baseline global processor",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=10,
        max_deliver=5,
    ))
    await _safe_add_consumer(js, "ORDERS_GLOBAL", api.ConsumerConfig(
        durable_name="archiver",
        description="Batch archiver — pulls in batches of 100",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=60,
        max_deliver=3,
    ))
    await _safe_add_consumer(js, "ORDERS_GLOBAL", api.ConsumerConfig(
        durable_name="fraud-check",
        description="THE VILLAIN — downstream fraud service is timing out",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=FRAUD_ACK_WAIT_SEC,
        max_deliver=10,
    ))
    await _safe_add_consumer(js, "ORDERS_GLOBAL", api.ConsumerConfig(
        durable_name="realtime-feed",
        description="Push consumer feeding the realtime dashboard",
        deliver_subject="feeds.realtime.orders",
        ack_policy=api.AckPolicy.NONE,
    ))

    # ORDERS_EU / ORDERS_US — regional pricing
    await _safe_add_consumer(js, "ORDERS_EU", api.ConsumerConfig(
        durable_name="eu-pricing",
        description="EU regional pricing engine",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=15,
        max_deliver=5,
    ))
    await _safe_add_consumer(js, "ORDERS_US", api.ConsumerConfig(
        durable_name="us-pricing",
        description="US regional pricing engine",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=15,
        max_deliver=5,
    ))

    # SHIPMENTS
    await _safe_add_consumer(js, "SHIPMENTS", api.ConsumerConfig(
        durable_name="dispatch-router",
        description="Dispatch router — healthy push consumer",
        deliver_subject="feeds.dispatch",
        ack_policy=api.AckPolicy.NONE,
    ))
    await _safe_add_consumer(js, "SHIPMENTS", api.ConsumerConfig(
        durable_name="customs-clearance",
        description="Customs clearance — slightly lagging",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=45,
        max_deliver=5,
    ))

    # TRACKING
    await _safe_add_consumer(js, "TRACKING", api.ConsumerConfig(
        durable_name="event-stream",
        description="Realtime tracking event stream (push)",
        deliver_subject="feeds.tracking",
        ack_policy=api.AckPolicy.NONE,
    ))
    await _safe_add_consumer(js, "TRACKING", api.ConsumerConfig(
        durable_name="analytics-batch",
        description="Analytics batcher — pulls every 30s",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=120,
        max_deliver=3,
    ))

    # PAYMENTS
    await _safe_add_consumer(js, "PAYMENTS", api.ConsumerConfig(
        durable_name="settlement",
        description="Payment settlement — healthy",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=15,
        max_deliver=5,
    ))
    # Ordered consumer: ack=None, flow_control + headers_only false, replay=instant
    await _safe_add_consumer(js, "PAYMENTS", api.ConsumerConfig(
        durable_name="reconciliation",
        description="Reconciliation — ordered push consumer",
        deliver_subject="feeds.reconciliation",
        ack_policy=api.AckPolicy.NONE,
        flow_control=True,
        idle_heartbeat=5.0,
    ))

    # AUDIT
    await _safe_add_consumer(js, "AUDIT", api.ConsumerConfig(
        durable_name="siem-export",
        description="SIEM exporter — slow steady pull",
        ack_policy=api.AckPolicy.EXPLICIT,
        ack_wait=60,
        max_deliver=3,
    ))

    # NOTIFICATIONS
    await _safe_add_consumer(js, "NOTIFICATIONS", api.ConsumerConfig(
        durable_name="email-sender",
        description="Email notification sender (push)",
        deliver_subject="feeds.notifications.email",
        ack_policy=api.AckPolicy.NONE,
    ))
    await _safe_add_consumer(js, "NOTIFICATIONS", api.ConsumerConfig(
        durable_name="sms-sender",
        description="SMS notification sender — slightly lagging push",
        deliver_subject="feeds.notifications.sms",
        ack_policy=api.AckPolicy.NONE,
    ))

    print("  ✓ 15 consumers created across ORDERS_GLOBAL, ORDERS_EU/US,")
    print("    SHIPMENTS, TRACKING, PAYMENTS, AUDIT, NOTIFICATIONS")


# ─── KV buckets ──────────────────────────────────────────────────────────────


async def create_kv(js) -> None:
    print("→ Creating KV buckets...")

    # 1. feature-flags
    try:
        await js.delete_key_value("feature-flags")
    except Exception:
        pass
    ff = await js.create_key_value(
        bucket="feature-flags",
        description="Aerolux global feature flags (rollout %)",
        history=10,
        replicas=3,
    )
    flag_keys = [
        "checkout.express_lane",
        "pricing.dynamic_surge",
        "routing.use_ml_optimizer",
        "tracking.high_precision_gps",
        "notifications.batch_email",
        "fraud.strict_mode",
        "dispatch.auto_reroute",
        "customs.precheck_enabled",
        "ui.new_dashboard",
        "api.graphql_beta",
        "payments.adyen_primary",
        "search.vector_index",
    ]
    for k in flag_keys:
        await ff.put(k, json.dumps({
            "enabled": random.random() < 0.6,
            "rollout_pct": random.choice([0, 10, 25, 50, 75, 100]),
        }).encode())
    # history on a couple of keys
    for _ in range(4):
        await ff.put("pricing.dynamic_surge", json.dumps({
            "enabled": True, "rollout_pct": random.choice([25, 50, 75, 100]),
        }).encode())
    for _ in range(3):
        await ff.put("fraud.strict_mode", json.dumps({
            "enabled": random.choice([True, False]), "rollout_pct": 100,
        }).encode())
    print("  ✓ feature-flags (12 keys, history=10, revisions on 2 keys)")

    # 2. service-registry
    try:
        await js.delete_key_value("service-registry")
    except Exception:
        pass
    sr = await js.create_key_value(
        bucket="service-registry",
        description="Aerolux internal service registry",
        history=5,
        replicas=3,
    )
    services = [
        ("auth.eu",       "https://auth.eu.aerolux.internal",      "2.3.1", "eu"),
        ("auth.us",       "https://auth.us.aerolux.internal",      "2.3.1", "us"),
        ("auth.apac",     "https://auth.apac.aerolux.internal",    "2.3.0", "apac"),
        ("pricing.global","https://pricing.aerolux.internal",      "4.1.2", "global"),
        ("tracking.eu",   "https://tracking.eu.aerolux.internal",  "1.8.4", "eu"),
        ("tracking.us",   "https://tracking.us.aerolux.internal",  "1.8.4", "us"),
        ("customs.eu",    "https://customs.eu.aerolux.internal",   "0.9.7", "eu"),
        ("ml-router",     "https://router.ml.aerolux.internal",    "3.0.0", "global"),
    ]
    for key, endpoint, version, region in services:
        await sr.put(key, json.dumps({
            "endpoint": endpoint,
            "version": version,
            "region": region,
            "healthy": True,
        }).encode())
    print("  ✓ service-registry (8 keys, history=5)")

    # 3. pricing-config
    try:
        await js.delete_key_value("pricing-config")
    except Exception:
        pass
    pc = await js.create_key_value(
        bucket="pricing-config",
        description="Aerolux surge + discount configuration",
        history=20,
        replicas=3,
    )
    pricing_keys = {
        "surge.eu":        {"multiplier": 1.2, "active": True,  "bands": [1.0, 1.2, 1.5]},
        "surge.us":        {"multiplier": 1.1, "active": True,  "bands": [1.0, 1.1, 1.3]},
        "surge.apac":      {"multiplier": 1.4, "active": True,  "bands": [1.0, 1.4, 2.0]},
        "discount.holiday":{"pct": 15, "active": False, "code": "HOLIDAY26"},
        "discount.loyal":  {"pct": 8,  "active": True,  "min_tier": "silver"},
        "base.freight_cents_per_kg": {"value": 82, "updated_at": iso_now()},
    }
    for k, v in pricing_keys.items():
        await pc.put(k, json.dumps(v).encode())
    print("  ✓ pricing-config (6 keys, history=20)")

    # 4. customer-sessions with TTL
    try:
        await js.delete_key_value("customer-sessions")
    except Exception:
        pass
    try:
        cs = await js.create_key_value(
            bucket="customer-sessions",
            description="Short-lived customer session state (TTL=15m)",
            history=1,
            ttl=15 * 60,  # seconds
            replicas=3,
        )
    except TypeError:
        # Older nats-py uses max_age not ttl
        cs = await js.create_key_value(
            bucket="customer-sessions",
            description="Short-lived customer session state (TTL=15m)",
            history=1,
            max_age=15 * 60,
            replicas=3,
        )
    for i in range(60):
        sid = f"sess_{uuid.uuid4().hex[:16]}"
        await cs.put(sid, json.dumps({
            "customer_id": f"cust_{random.randint(10000, 99999)}",
            "region": random.choice(REGIONS),
            "started_at": iso_now(),
            "cart_items": random.randint(0, 6),
            "last_page": random.choice(["/ship", "/quote", "/track", "/invoices"]),
        }).encode())
    print("  ✓ customer-sessions (60 keys, TTL=15m, history=1)")


# ─── Object stores ───────────────────────────────────────────────────────────


async def create_object_stores(js) -> None:
    print("→ Creating object stores...")

    # 1. invoices
    try:
        await js.delete_object_store("invoices")
    except Exception:
        pass
    inv = await js.create_object_store(
        bucket="invoices",
        description="Signed PDF invoices (180d retention)",
    )
    for i in range(25):
        payload = os.urandom(random.randint(5_000, 50_000))
        await inv.put(
            f"invoice-{1000 + i}.pdf",
            payload,
        )
    print("  ✓ invoices (25 PDFs)")

    # 2. product-catalog
    try:
        await js.delete_object_store("product-catalog")
    except Exception:
        pass
    cat = await js.create_object_store(
        bucket="product-catalog",
        description="SKU reference imagery",
    )
    for i in range(15):
        payload = os.urandom(random.randint(10_000, 200_000))
        # Passing a plain name keeps nats-py happy across versions; the
        # mime/category metadata is part of the "product-catalog" bucket
        # semantics rather than per-object headers.
        await cat.put(f"sku-{2000 + i}.png", payload)
    print("  ✓ product-catalog (15 PNGs with metadata)")

    # 3. shipping-labels
    try:
        await js.delete_object_store("shipping-labels")
    except Exception:
        pass
    lab = await js.create_object_store(
        bucket="shipping-labels",
        description="Generated shipping label PDFs",
    )
    for i in range(30):
        tid = uuid.uuid4().hex[:12]
        payload = os.urandom(random.randint(5_000, 15_000))
        await lab.put(f"label-{tid}.pdf", payload)
    print("  ✓ shipping-labels (30 PDFs)")


# ─── Initial seed ────────────────────────────────────────────────────────────


async def seed_initial_batch(js) -> None:
    print(f"→ Seeding {SEED_INITIAL_PER_REGION} orders per region...")
    total = 0
    for region in REGIONS:
        for i in range(SEED_INITIAL_PER_REGION):
            order = make_order(region)
            await js.publish(
                f"orders.{region}.created",
                json.dumps(order).encode(),
            )
            if i % 3 == 0:
                pay = make_payment(order["order_id"], region)
                await js.publish("payments.captured", json.dumps(pay).encode())
            if i % 4 == 0:
                trk = make_tracking(order["order_id"])
                await js.publish(f"tracking.{region}.updated", json.dumps(trk).encode())
            if i % 10 == 0:
                aud = make_audit("order.placed", {"order_id": order["order_id"], "region": region})
                await js.publish(f"audit.orders.{region}", json.dumps(aud).encode())
            if i % 2 == 0:
                await js.publish(
                    f"shipments.{region}.booked",
                    json.dumps({
                        "shipment_id": f"shp_{uuid.uuid4().hex[:16]}",
                        "order_id": order["order_id"],
                        "carrier": order["shipment"]["carrier"],
                        "at": iso_now(),
                    }).encode(),
                )
            total += 1
    print(f"  ✓ seeded {total} orders + derived events")


# ─── Live-mode tasks ─────────────────────────────────────────────────────────


async def baseline_traffic_task(js) -> None:
    print(f"▶ Baseline traffic at ~{BASELINE_RATE_PER_SEC} msg/s")
    interval = 1.0 / BASELINE_RATE_PER_SEC
    while True:
        region = pick_region()
        order = make_order(region)
        try:
            await js.publish(
                f"orders.{region}.created",
                json.dumps(order).encode(),
            )
            if random.random() < 0.30:
                pay = make_payment(order["order_id"], region)
                await js.publish("payments.captured", json.dumps(pay).encode())
            if random.random() < 0.50:
                # 1-3 tracking events over next few seconds
                for _ in range(random.randint(1, 3)):
                    trk = make_tracking(order["order_id"])
                    await js.publish(
                        f"tracking.{region}.updated",
                        json.dumps(trk).encode(),
                    )
            if random.random() < 0.05:
                aud = make_audit("order.placed", {
                    "order_id": order["order_id"], "region": region,
                })
                await js.publish(
                    f"audit.orders.{region}", json.dumps(aud).encode(),
                )
            if random.random() < 0.3:
                await js.publish(
                    f"shipments.{region}.booked",
                    json.dumps({
                        "shipment_id": f"shp_{uuid.uuid4().hex[:16]}",
                        "order_id": order["order_id"],
                        "at": iso_now(),
                    }).encode(),
                )
        except Exception as e:
            print(f"  ! baseline publish failed: {e}", file=sys.stderr)
        await asyncio.sleep(interval)


async def notifications_task(js) -> None:
    """Emit ~3 notifications/sec."""
    while True:
        try:
            for _ in range(3):
                n = make_notification(f"ord_{uuid.uuid4().hex[:20]}")
                subj = f"notifications.{n['channel']}.sent"
                await js.publish(subj, json.dumps(n).encode())
        except Exception:
            pass
        await asyncio.sleep(1.0)


async def healthy_pull_task(js, stream: str, durable: str) -> None:
    """A healthy pull subscriber that fetches and acks promptly."""
    await asyncio.sleep(3)
    try:
        psub = await js.pull_subscribe_bind(durable=durable, stream=stream)
    except Exception as e:
        print(f"  ! bind {stream}.{durable}: {e}", file=sys.stderr)
        return
    while True:
        try:
            msgs = await psub.fetch(20, timeout=3)
            for m in msgs:
                await m.ack()
        except NatsTimeoutError:
            pass
        except Exception as e:
            print(f"  ! {durable} fetch error: {e}", file=sys.stderr)
            await asyncio.sleep(2)


async def fraud_villain_task(js) -> None:
    """Bind ORDERS_GLOBAL.fraud-check and pull but NEVER ack."""
    print("▶ Starting fraud-check villain (pulls + never acks)")
    await asyncio.sleep(3)
    try:
        psub = await js.pull_subscribe_bind(
            durable="fraud-check",
            stream="ORDERS_GLOBAL",
        )
    except Exception as e:
        print(f"  ! failed to bind fraud-check: {e}", file=sys.stderr)
        return
    while True:
        try:
            msgs = await psub.fetch(10, timeout=5)
            _ = msgs  # deliberately no ack
        except NatsTimeoutError:
            pass
        except Exception as e:
            print(f"  ! fraud-check fetch error: {e}", file=sys.stderr)
            await asyncio.sleep(2)
        await asyncio.sleep(0.5)


async def scheduled_spike_task(js) -> None:
    print(f"▶ Backlog spike scheduled for T+{SPIKE_DELAY_SEC}s")
    await asyncio.sleep(SPIKE_DELAY_SEC)
    print(
        f"  ! SPIKE: publishing {SPIKE_TOTAL_MSGS} messages over "
        f"{SPIKE_DURATION_SEC}s"
    )
    interval = SPIKE_DURATION_SEC / SPIKE_TOTAL_MSGS
    start = time.monotonic()
    for i in range(SPIKE_TOTAL_MSGS):
        order = make_order("eu", spike=True)
        try:
            await js.publish(
                "orders.eu.created",
                json.dumps(order).encode(),
            )
        except Exception:
            pass
        if i > 0 and i % 500 == 0:
            elapsed = time.monotonic() - start
            print(f"  spike: {i}/{SPIKE_TOTAL_MSGS} ({elapsed:.1f}s elapsed)")
        await asyncio.sleep(interval)
    print(f"  ✓ spike complete in {time.monotonic() - start:.1f}s")


# ─── Micro-services ──────────────────────────────────────────────────────────


async def run_services(nc) -> list:
    """Start the 4 NATS micro-services. Returns list of service handles."""
    if not HAVE_MICRO:
        print("  ! nats.micro not available in this nats-py; skipping services")
        return []

    print("▶ Starting micro-services...")
    services = []

    # 1. checkout-validate
    svc = await add_service(
        nc,
        name="checkout-validate",
        version="1.4.2",
        description="Validates Aerolux checkout baskets",
    )
    async def checkout_handler(req):
        await req.respond(json.dumps({
            "valid": True,
            "basket_id": f"bskt_{uuid.uuid4().hex[:12]}",
            "total_cents": random.randint(1000, 5000000),
        }).encode())
    await svc.add_endpoint(name="validate", subject="checkout.validate", handler=checkout_handler)
    services.append(svc)

    # 2. inventory-check
    svc = await add_service(
        nc,
        name="inventory-check",
        version="2.1.0",
        description="Aerolux SKU inventory availability",
    )
    async def inv_handler(req):
        await req.respond(json.dumps({
            "available": random.random() > 0.1,
            "in_stock": random.randint(0, 500),
            "sku": f"sku_{random.randint(1000, 9999)}",
        }).encode())
    await svc.add_endpoint(name="check", subject="inventory.check", handler=inv_handler)
    services.append(svc)

    # 3. pricing-quote
    svc = await add_service(
        nc,
        name="pricing-quote",
        version="3.0.1",
        description="Aerolux freight pricing quotes",
    )
    async def quote_handler(req):
        await req.respond(json.dumps({
            "quote_id": f"qte_{uuid.uuid4().hex[:12]}",
            "total_cents": random.randint(50000, 10_000_000),
            "currency": random.choice(["EUR", "USD", "SGD"]),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
        }).encode())
    await svc.add_endpoint(name="quote", subject="pricing.quote", handler=quote_handler)
    services.append(svc)

    # 4. fraud-score
    svc = await add_service(
        nc,
        name="fraud-score",
        version="0.9.5",
        description="Aerolux fraud scoring service",
    )
    async def fraud_handler(req):
        score = random.randint(0, 100)
        if score < 30:
            risk = "low"
        elif score < 70:
            risk = "medium"
        else:
            risk = "high"
        await req.respond(json.dumps({"score": score, "risk": risk}).encode())
    await svc.add_endpoint(name="score", subject="fraud.score", handler=fraud_handler)
    services.append(svc)

    print(f"  ✓ {len(services)} services started")
    return services


async def service_client_task(nc) -> None:
    """Periodically call each service so request counts are non-zero."""
    await asyncio.sleep(5)
    subjects = [
        ("checkout.validate", {"basket_id": "demo"}),
        ("inventory.check",   {"sku": "demo"}),
        ("pricing.quote",     {"origin": "hamburg", "destination": "shanghai"}),
        ("fraud.score",       {"order_id": "demo"}),
    ]
    interval = 1.0 / SERVICE_REQUEST_RATE
    while True:
        subj, body = random.choice(subjects)
        try:
            await nc.request(subj, json.dumps(body).encode(), timeout=2)
        except Exception:
            pass
        await asyncio.sleep(interval)


# ─── Entry point ─────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"{COMPANY_NAME} supercluster scenario generator",
    )
    parser.add_argument(
        "--nats-url",
        default="nats://nats-eu-1:4222,nats://nats-us-1:4222,nats://nats-apac-1:4222",
        help="Comma-separated NATS servers. Default: in-compose regional entry points.",
    )
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    print(f"{COMPANY_NAME} scenario generator")
    print(f"Connecting to NATS: {args.nats_url}")
    nc = await connect_with_retry(args.nats_url)
    js = nc.jetstream()

    if args.reset:
        await reset_cluster(js)

    await create_streams(js)
    await create_consumers(js)
    await create_kv(js)
    await create_object_stores(js)
    await seed_initial_batch(js)

    services = await run_services(nc)

    if args.live:
        print()
        print("━" * 64)
        print(f" {COMPANY_NAME} — running in --live mode.")
        print(f" • Baseline: ~{BASELINE_RATE_PER_SEC} orders/sec across EU/US/APAC")
        print(" • Villain:  ORDERS_GLOBAL.fraud-check pulls + never acks")
        print(f" • Spike:    {SPIKE_TOTAL_MSGS} msgs to orders.eu.* at T+{SPIKE_DELAY_SEC}s")
        print(" • Services: checkout-validate, inventory-check, pricing-quote, fraud-score")
        print(" Ctrl+C to stop.")
        print("━" * 64)

        await asyncio.gather(
            baseline_traffic_task(js),
            notifications_task(js),
            fraud_villain_task(js),
            scheduled_spike_task(js),
            healthy_pull_task(js, "ORDERS_GLOBAL", "processor"),
            healthy_pull_task(js, "ORDERS_GLOBAL", "archiver"),
            healthy_pull_task(js, "ORDERS_EU",     "eu-pricing"),
            healthy_pull_task(js, "ORDERS_US",     "us-pricing"),
            healthy_pull_task(js, "PAYMENTS",      "settlement"),
            healthy_pull_task(js, "AUDIT",         "siem-export"),
            service_client_task(nc),
        )
    else:
        print()
        print("━" * 64)
        print(" Seed complete.")
        print(" • 10 streams, 15 consumers, 4 KV, 3 object stores, 4 services")
        print(" • Use --live to keep publishing + run villain + spike")
        print("━" * 64)

    await nc.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
