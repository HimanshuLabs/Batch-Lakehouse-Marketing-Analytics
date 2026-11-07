from __future__ import annotations

import json
import os
import random
import shutil
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker


# Master data (customers, products, campaigns) comes from one fixed stream so
# every batch carries byte-identical base rows. Facts and changes use their
# own streams seeded from BATCH_DATE, so each batch is deterministic and
# different batch dates produce different increments.
MASTER_SEED = 42
fake = Faker("en_IN")
random.seed(MASTER_SEED)
Faker.seed(MASTER_SEED)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()
RAW_DIR = PROJECT_ROOT / "data" / "raw" / f"batch_date={BATCH_DATE}"

# All master-data dates are relative to this fixed anchor, never to the clock.
# It is the first batch date, so BATCH_DATE=2026-05-18 reproduces the original
# raw batch (all CSVs byte for byte; web events apart from the random UUIDs).
ANCHOR_DATE = date(2026, 5, 18)
BATCH_DAY = date.fromisoformat(BATCH_DATE)
BATCH_INDEX = (BATCH_DAY - ANCHOR_DATE).days
IS_ANCHOR_BATCH = BATCH_INDEX == 0

# Fact IDs are unique across batches: BATCH_INDEX * ID_BLOCK + n. Batch 0 keeps
# the original 1..count IDs. Every count must stay below ID_BLOCK, and the IDs
# fit Silver/Gold/staging INTEGER (int32) for ~21,000 days after the anchor.
ID_BLOCK = 100_000
INT32_MAX = 2_147_483_647

# The anchor batch keeps the original volumes over a 120-day window. Later
# batches are increments over FACT_WINDOW_DAYS with volumes scaled to the same
# daily rate.
FACT_WINDOW_DAYS = 14
ANCHOR_COUNTS = {"web_events": 8000, "orders": 2000, "order_items": 5000, "ad_spend": 1000}
INCREMENT_COUNTS = {"web_events": 1000, "orders": 250, "order_items": 625, "ad_spend": 125}

# Changed master records per batch. The anchor batch keeps the original
# change set (customer 5, product 5, campaign 4) and its original timestamps;
# later batches pick keys with an RNG seeded from BATCH_DATE and stamp them at
# BATCH_DATE 08:00:00.
CHANGE_COUNTS = {"customers": 3, "products": 2, "campaigns": 1}
CHANGE_TIME = "08:00:00"
ANCHOR_CHANGE_TIMESTAMPS = {
    "customers": "2026-05-18T08:13:04.430089",
    "products": "2026-05-18T08:13:04.434602",
    "campaigns": "2026-05-18T08:13:04.437584",
}
# Rows used as dirty records, the duplicate row, or the anchor change set are
# never picked for later changes.
NON_CHANGEABLE_IDS = {"customers": {1, 2, 3, 4, 5}, "products": {1, 2, 3, 4, 5}, "campaigns": {1, 2, 3, 4}}

RAW_FILES = [
    "customers.csv",
    "products.csv",
    "campaigns.csv",
    "orders.csv",
    "order_items.csv",
    "ad_spend.csv",
    "web_events.json",
]

SCHEMA_VERSION = "v1"
SOURCE_SYSTEM = "project_1_behavioral_events"

GENDERS = ["Male", "Female", "Other"]
MEMBERSHIP_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
LANGUAGES = ["English", "Hindi", "Telugu", "Tamil", "Kannada", "Marathi", "Bengali"]
USER_SEGMENTS = ["Student", "Working Professional", "Small Business", "Enterprise"]
JOURNEY_STAGES = ["awareness", "consideration", "cart", "checkout", "purchase", "retention"]

PRODUCT_CATEGORIES = ["Electronics", "Fashion", "Home", "Beauty", "Fitness", "Books"]
PRODUCT_NAMES = [
    "Wireless Mouse", "Mechanical Keyboard", "Running Shoes", "Face Serum",
    "Yoga Mat", "Bluetooth Speaker", "Office Chair", "Laptop Stand",
    "Smart Watch", "Backpack", "LED Monitor", "Water Bottle"
]
DISCOUNT_PERCENTS = [0, 5, 10, 15, 20, 25, 30, 40]
CAMPAIGN_STATUSES = ["planned", "active", "paused", "completed"]

EVENT_TYPES = ["page_view", "product_view", "search", "add_to_cart", "checkout", "purchase"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD", None]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
OPERATING_SYSTEMS = ["Android", "iOS", "Windows", "Ubuntu", "macOS"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
NETWORK_TYPES = ["4G", "5G", "WiFi", "Broadband"]
TRAFFIC_SOURCES = ["google", "facebook", "instagram", "email", "organic", "affiliate"]
RECOMMENDATION_ALGORITHMS = ["collaborative_filtering", "content_based", "ranking_model", "popular_items"]
AB_TEST_GROUPS = ["A", "B", "control"]
COUNTRIES = ["India"]


def random_date(rng: random.Random, anchor: date, start_days_ago: int, end_days_ago: int = 0) -> date:
    days_ago = rng.randint(end_days_ago, start_days_ago)
    return anchor - timedelta(days=days_ago)


def random_datetime(rng: random.Random, anchor: date, start_days_ago: int, end_days_ago: int = 0) -> datetime:
    base_date = random_date(rng, anchor, start_days_ago, end_days_ago)
    return datetime.combine(base_date, datetime.min.time()) + timedelta(
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
    )


def validate_counts(counts: dict[str, int]) -> None:
    # Each batch owns one ID_BLOCK of fact IDs; a count that reaches the block
    # size would collide with the next batch's IDs.
    too_large = {name: count for name, count in counts.items() if not 0 < count < ID_BLOCK}
    if too_large:
        raise ValueError(
            f"Per-batch fact counts must be between 1 and {ID_BLOCK - 1}: {too_large}"
        )


def fact_id(n: int) -> int:
    value = BATCH_INDEX * ID_BLOCK + n
    if value > INT32_MAX:
        raise ValueError(f"Fact ID {value} exceeds INT32_MAX for BATCH_DATE={BATCH_DATE}")
    return value


# ---------------------------------------------------------------------------
# Master data: identical base rows in every batch (fixed seed, fixed anchor)
# ---------------------------------------------------------------------------


def generate_customers(count: int = 500) -> pd.DataFrame:
    rows = []

    for user_id in range(1, count + 1):
        home_city = fake.city()
        home_state = fake.state()

        rows.append(
            {
                "user_id": user_id,
                "user_name": fake.name(),
                "email": fake.email(),
                "gender": random.choice(GENDERS),
                "age": random.randint(18, 65),
                "membership_tier": random.choice(MEMBERSHIP_TIERS),
                "loyalty_points": random.randint(0, 50000),
                "preferred_language": random.choice(LANGUAGES),
                "home_city": home_city,
                "home_state": home_state,
                "country": "India",
                "user_segment": random.choice(USER_SEGMENTS),
                "is_prime_user": random.choice([True, False]),
                "schema_version": SCHEMA_VERSION,
                "source": "crm_customer_master",
                "updated_at": random_datetime(random, ANCHOR_DATE, 120, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "email"] = "invalid-email"
    df.loc[1, "home_city"] = None
    df.loc[2, "age"] = -5

    # Duplicate natural key
    duplicate_row = df.iloc[[3]].copy()
    df = pd.concat([df, duplicate_row], ignore_index=True)

    return df


def generate_products(count: int = 150) -> pd.DataFrame:
    rows = []

    for product_id in range(1, count + 1):
        original_price = round(random.uniform(199, 99999), 2)
        discount_percent = random.choice(DISCOUNT_PERCENTS)
        discounted_price = round(original_price * (1 - discount_percent / 100), 2)

        rows.append(
            {
                "product_id": product_id,
                "product_name": random.choice(PRODUCT_NAMES),
                "category": random.choice(PRODUCT_CATEGORIES),
                "original_price": original_price,
                "discount_percent": discount_percent,
                "discounted_price": discounted_price,
                "inventory_remaining": random.randint(0, 10000),
                "schema_version": SCHEMA_VERSION,
                "source": "product_catalog",
                "updated_at": random_datetime(random, ANCHOR_DATE, 90, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "category"] = None
    df.loc[1, "original_price"] = -1000
    df.loc[2, "discount_percent"] = 150

    # Duplicate natural key
    duplicate_row = df.iloc[[3]].copy()
    df = pd.concat([df, duplicate_row], ignore_index=True)

    return df


def generate_campaigns(count: int = 40) -> pd.DataFrame:
    rows = []

    for campaign_id in range(1, count + 1):
        start_date = random_date(random, ANCHOR_DATE, 180, 10)
        end_date = start_date + timedelta(days=random.randint(15, 90))

        rows.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": f"{random.choice(TRAFFIC_SOURCES).title()} Campaign {campaign_id}",
                "traffic_source": random.choice(TRAFFIC_SOURCES),
                "ab_test_group": random.choice(AB_TEST_GROUPS),
                "target_segment": random.choice(USER_SEGMENTS),
                "budget": round(random.uniform(10000, 500000), 2),
                "campaign_status": random.choice(CAMPAIGN_STATUSES),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "schema_version": SCHEMA_VERSION,
                "source": "marketing_campaign_master",
                "updated_at": random_datetime(random, ANCHOR_DATE, 60, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "campaign_id"] = None
    df.loc[1, "traffic_source"] = "unknown_channel"
    df.loc[2, "end_date"] = "1900-01-01"

    return df


# ---------------------------------------------------------------------------
# Per-batch changes: appended as extra rows (the base row stays in the file)
# ---------------------------------------------------------------------------


def base_row(df: pd.DataFrame, key: str, key_value: int) -> pd.DataFrame:
    matches = df.index[df[key] == key_value]
    return df.loc[[matches[0]]].copy()


def pick_change_keys(rng: random.Random, df: pd.DataFrame, key: str, entity: str) -> list[int]:
    candidates = sorted(
        int(value)
        for value in df[key].dropna().unique()
        if int(value) not in NON_CHANGEABLE_IDS[entity]
    )
    return sorted(rng.sample(candidates, CHANGE_COUNTS[entity]))


def apply_customer_changes(df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    if IS_ANCHOR_BATCH:
        changed = base_row(df, "user_id", 5)
        changed["membership_tier"] = "Platinum"
        changed["loyalty_points"] = 99999
        changed["user_segment"] = "Enterprise"
        changed["is_prime_user"] = True
        changed["updated_at"] = ANCHOR_CHANGE_TIMESTAMPS["customers"]
        return pd.concat([df, changed], ignore_index=True)

    changes = []
    for user_id in pick_change_keys(rng, df, "user_id", "customers"):
        changed = base_row(df, "user_id", user_id)
        current_tier = changed["membership_tier"].iloc[0]
        current_segment = changed["user_segment"].iloc[0]
        # SCD2-tracked attributes only: loyalty_tier and customer_segment.
        changed["membership_tier"] = rng.choice([t for t in MEMBERSHIP_TIERS if t != current_tier])
        changed["user_segment"] = rng.choice([s for s in USER_SEGMENTS if s != current_segment])
        changed["updated_at"] = f"{BATCH_DATE}T{CHANGE_TIME}"
        changes.append(changed)

    return pd.concat([df, *changes], ignore_index=True)


def apply_product_changes(df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    if IS_ANCHOR_BATCH:
        changed = base_row(df, "product_id", 5)
        changed["discount_percent"] = 35
        changed["discounted_price"] = round(float(changed["original_price"].iloc[0]) * 0.65, 2)
        changed["inventory_remaining"] = 0
        changed["updated_at"] = ANCHOR_CHANGE_TIMESTAMPS["products"]
        return pd.concat([df, changed], ignore_index=True)

    changes = []
    for product_id in pick_change_keys(rng, df, "product_id", "products"):
        changed = base_row(df, "product_id", product_id)
        current_discount = changed["discount_percent"].iloc[0]
        new_discount = rng.choice([d for d in DISCOUNT_PERCENTS if d != current_discount])
        # SCD2-tracked attribute: price (discounted_price). discount_percent
        # changes with it so the row stays internally consistent.
        changed["discount_percent"] = new_discount
        changed["discounted_price"] = round(float(changed["original_price"].iloc[0]) * (1 - new_discount / 100), 2)
        changed["updated_at"] = f"{BATCH_DATE}T{CHANGE_TIME}"
        changes.append(changed)

    return pd.concat([df, *changes], ignore_index=True)


def apply_campaign_changes(df: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    if IS_ANCHOR_BATCH:
        changed = df.iloc[[3]].copy()
        changed["budget"] = round(float(changed["budget"].iloc[0]) * 1.25, 2)
        changed["campaign_status"] = "paused"
        changed["updated_at"] = ANCHOR_CHANGE_TIMESTAMPS["campaigns"]
        return pd.concat([df, changed], ignore_index=True)

    changes = []
    for campaign_id in pick_change_keys(rng, df, "campaign_id", "campaigns"):
        changed = base_row(df, "campaign_id", campaign_id)
        current_status = changed["campaign_status"].iloc[0]
        # SCD2-tracked attributes: budget and campaign_status.
        changed["budget"] = round(float(changed["budget"].iloc[0]) * rng.choice([1.1, 1.25, 1.5]), 2)
        changed["campaign_status"] = rng.choice([s for s in CAMPAIGN_STATUSES if s != current_status])
        changed["updated_at"] = f"{BATCH_DATE}T{CHANGE_TIME}"
        changes.append(changed)

    return pd.concat([df, *changes], ignore_index=True)


# ---------------------------------------------------------------------------
# Facts: per-batch increments with batch-unique IDs
# ---------------------------------------------------------------------------


def generate_web_events(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    campaigns: pd.DataFrame,
    rng: random.Random,
    faker: Faker,
    uuid_rng: random.Random,
    window_days: int,
    count: int,
) -> list[dict]:
    valid_users = customers.dropna(subset=["user_id"]).drop_duplicates("user_id")
    valid_products = products.dropna(subset=["product_id"]).drop_duplicates("product_id")
    valid_campaign_ids = campaigns["campaign_id"].dropna().astype(int).unique().tolist()

    user_records = valid_users.to_dict("records")
    product_records = valid_products.to_dict("records")

    def new_uuid() -> str:
        return str(uuid.UUID(int=uuid_rng.getrandbits(128), version=4))

    events = []

    for _ in range(count):
        user = rng.choice(user_records)
        product = rng.choice(product_records)
        event_time = random_datetime(rng, BATCH_DAY, window_days, 0)
        event_type = rng.choice(EVENT_TYPES)

        quantity = rng.randint(1, 5) if event_type in ["add_to_cart", "checkout", "purchase"] else 0
        discounted_price = float(product["discounted_price"])
        cart_value = round(quantity * discounted_price, 2)

        event = {
            "event_id": new_uuid(),
            "session_id": new_uuid(),
            "user_id": int(user["user_id"]),
            "user_name": user["user_name"],
            "email": user["email"],
            "gender": user["gender"],
            "age": int(user["age"]) if pd.notna(user["age"]) else None,
            "membership_tier": user["membership_tier"],
            "loyalty_points": int(user["loyalty_points"]),
            "preferred_language": user["preferred_language"],
            "home_city": user["home_city"],
            "home_state": user["home_state"],
            "event_time": event_time.isoformat(),
            "event_type": event_type,
            "user_journey_stage": rng.choice(JOURNEY_STAGES),
            "user_segment": user["user_segment"],
            "is_prime_user": bool(user["is_prime_user"]),
            "product_id": int(product["product_id"]),
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": quantity,
            "original_price": float(product["original_price"]),
            "discount_percent": float(product["discount_percent"]),
            "discounted_price": discounted_price,
            "cart_value": cart_value,
            "inventory_remaining": int(product["inventory_remaining"]),
            "search_query": rng.choice([faker.word(), faker.word(), None]),
            "time_on_page_sec": rng.randint(1, 900),
            "scroll_depth_percent": rng.randint(0, 100),
            "hover_duration_ms": rng.randint(0, 30000),
            "session_duration_sec": rng.randint(10, 7200),
            "items_viewed_in_session": rng.randint(1, 50),
            "repeat_product_view_count": rng.randint(0, 10),
            "time_since_last_event_ms": rng.randint(100, 600000),
            "recommendation_rank": rng.randint(1, 20),
            "recommendation_clicked": rng.choice([True, False]),
            "recommendation_algorithm": rng.choice(RECOMMENDATION_ALGORITHMS),
            "click_position": rng.randint(1, 50),
            "engagement_score": round(rng.uniform(0, 1), 4),
            "purchase_probability": round(rng.uniform(0, 1), 4),
            "cart_abandonment_probability": round(rng.uniform(0, 1), 4),
            "ab_test_group": rng.choice(AB_TEST_GROUPS),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "device_type": rng.choice(DEVICE_TYPES),
            "operating_system": rng.choice(OPERATING_SYSTEMS),
            "browser": rng.choice(BROWSERS),
            "network_type": rng.choice(NETWORK_TYPES),
            "app_version": f"{rng.randint(1, 5)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}",
            "traffic_source": rng.choice(TRAFFIC_SOURCES),
            "campaign_id": rng.choice(valid_campaign_ids + [None]),
            "api_latency_ms": rng.randint(20, 5000),
            "page_load_time_ms": rng.randint(100, 10000),
            "fraud_score": round(rng.uniform(0, 1), 4),
            "country": "India",
            "city": rng.choice([user["home_city"], faker.city()]),
            "ip_address": faker.ipv4_public(),
            "schema_version": SCHEMA_VERSION,
            "source": SOURCE_SYSTEM,
            "event_timestamp": event_time.isoformat(),
        }

        events.append(event)

    # Dirty event for quarantine testing
    events.append(
        {
            "event_id": None,
            "session_id": new_uuid(),
            "user_id": 999999,
            "user_name": None,
            "email": "bad-email",
            "gender": "Unknown",
            "age": -10,
            "membership_tier": "Diamond",
            "loyalty_points": -999,
            "preferred_language": None,
            "home_city": None,
            "home_state": None,
            "event_time": "bad-timestamp",
            "event_type": "unknown_event",
            "user_journey_stage": "broken_stage",
            "user_segment": "ghost_segment",
            "is_prime_user": None,
            "product_id": 999999,
            "product_name": None,
            "category": None,
            "quantity": -5,
            "original_price": -100,
            "discount_percent": 500,
            "discounted_price": -50,
            "cart_value": -999,
            "inventory_remaining": -1,
            "search_query": None,
            "time_on_page_sec": -1,
            "scroll_depth_percent": 999,
            "hover_duration_ms": -1,
            "session_duration_sec": -1,
            "items_viewed_in_session": -1,
            "repeat_product_view_count": -1,
            "time_since_last_event_ms": -1,
            "recommendation_rank": -1,
            "recommendation_clicked": None,
            "recommendation_algorithm": "unknown_algo",
            "click_position": -1,
            "engagement_score": 9.9,
            "purchase_probability": 9.9,
            "cart_abandonment_probability": 9.9,
            "ab_test_group": "Z",
            "payment_method": "gold_coin",
            "device_type": "smart_fridge",
            "operating_system": "TempleOS",
            "browser": "UnknownBrowser",
            "network_type": "carrier_pigeon",
            "app_version": "bad-version",
            "traffic_source": "unknown_channel",
            "campaign_id": 999999,
            "api_latency_ms": -100,
            "page_load_time_ms": -100,
            "fraud_score": 9.9,
            "country": "India",
            "city": None,
            "ip_address": "999.999.999.999",
            "schema_version": SCHEMA_VERSION,
            "source": SOURCE_SYSTEM,
            "event_timestamp": "bad-timestamp",
        }
    )

    return events


def generate_orders(web_events: list[dict], rng: random.Random, count: int) -> pd.DataFrame:
    purchase_events = [event for event in web_events if event.get("event_type") == "purchase"]

    rows = []
    for n in range(1, count + 1):
        event = rng.choice(purchase_events)

        rows.append(
            {
                "order_id": fact_id(n),
                "user_id": event["user_id"],
                "campaign_id": event["campaign_id"],
                "order_timestamp": event["event_timestamp"],
                "payment_method": event["payment_method"] or rng.choice(["UPI", "Credit Card", "Debit Card"]),
                "cart_value": max(float(event["cart_value"]), 199.0),
                "fraud_score": event["fraud_score"],
                "country": event["country"],
                "city": event["city"],
                "schema_version": SCHEMA_VERSION,
                "source": "orders_from_purchase_events",
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "order_id"] = df.loc[1, "order_id"]
    df.loc[2, "cart_value"] = -500
    df.loc[3, "user_id"] = 999999
    df.loc[4, "order_timestamp"] = "not-a-date"

    return df


def generate_order_items(orders: pd.DataFrame, products: pd.DataFrame, rng: random.Random, count: int) -> pd.DataFrame:
    valid_order_ids = orders["order_id"].dropna().astype(int).unique().tolist()
    valid_products = products.dropna(subset=["product_id"]).drop_duplicates("product_id").to_dict("records")

    rows = []

    for n in range(1, count + 1):
        product = rng.choice(valid_products)
        quantity = rng.randint(1, 5)
        discounted_price = float(product["discounted_price"])
        line_amount = round(quantity * discounted_price, 2)

        rows.append(
            {
                "order_item_id": fact_id(n),
                "order_id": rng.choice(valid_order_ids),
                "product_id": int(product["product_id"]),
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": quantity,
                "original_price": float(product["original_price"]),
                "discount_percent": float(product["discount_percent"]),
                "discounted_price": discounted_price,
                "line_amount": line_amount,
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "product_id"] = 999999
    df.loc[1, "quantity"] = -3
    df.loc[2, "line_amount"] = 1

    return df


def generate_ad_spend(campaigns: pd.DataFrame, rng: random.Random, window_days: int, count: int) -> pd.DataFrame:
    valid_campaigns = campaigns.dropna(subset=["campaign_id"]).drop_duplicates("campaign_id").to_dict("records")

    rows = []

    for n in range(1, count + 1):
        campaign = rng.choice(valid_campaigns)
        impressions = rng.randint(1000, 500000)
        clicks = rng.randint(10, impressions)

        rows.append(
            {
                "spend_id": fact_id(n),
                "campaign_id": int(campaign["campaign_id"]),
                "traffic_source": campaign["traffic_source"],
                "spend_date": random_date(rng, BATCH_DAY, window_days, 0).isoformat(),
                "impressions": impressions,
                "clicks": clicks,
                "spend_amount": round(rng.uniform(500, 100000), 2),
                "schema_version": SCHEMA_VERSION,
                "source": "marketing_ad_platform",
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "spend_amount"] = -100
    df.loc[1, "clicks"] = df.loc[1, "impressions"] + 100
    df.loc[2, "campaign_id"] = 999999
    df.loc[3, "spend_date"] = None

    return df


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------


def write_json_lines(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def fsync_file(path: Path) -> None:
    with path.open("rb") as file:
        os.fsync(file.fileno())


def raw_dir_is_complete(raw_dir: Path) -> bool:
    return all((raw_dir / name).is_file() and (raw_dir / name).stat().st_size > 0 for name in RAW_FILES)


def build_batch() -> dict[str, object]:
    customers = generate_customers()
    products = generate_products()
    campaigns = generate_campaigns()

    if IS_ANCHOR_BATCH:
        # Batch 0 continues the master stream, exactly as the original
        # generator did, so it reproduces the original raw batch.
        change_rng = fact_rng = random
        fact_faker = fake
        window_days = 120
        counts = ANCHOR_COUNTS
    else:
        change_rng = random.Random(f"changes|{BATCH_DATE}")
        fact_rng = random.Random(f"facts|{BATCH_DATE}")
        fact_faker = Faker("en_IN")
        fact_faker.seed_instance(f"facts|{BATCH_DATE}")
        window_days = FACT_WINDOW_DAYS
        counts = INCREMENT_COUNTS

    validate_counts(counts)

    customers = apply_customer_changes(customers, change_rng)
    products = apply_product_changes(products, change_rng)
    campaigns = apply_campaign_changes(campaigns, change_rng)

    uuid_rng = random.Random(f"uuid|{BATCH_DATE}")
    web_events = generate_web_events(
        customers, products, campaigns, fact_rng, fact_faker, uuid_rng, window_days, counts["web_events"]
    )
    orders = generate_orders(web_events, fact_rng, counts["orders"])
    order_items = generate_order_items(orders, products, fact_rng, counts["order_items"])
    ad_spend = generate_ad_spend(campaigns, fact_rng, window_days, counts["ad_spend"])

    return {
        "customers.csv": customers,
        "products.csv": products,
        "campaigns.csv": campaigns,
        "orders.csv": orders,
        "order_items.csv": order_items,
        "ad_spend.csv": ad_spend,
        "web_events.json": web_events,
    }


def write_batch(raw_dir: Path, outputs: dict[str, object]) -> None:
    # Write all seven files into a hidden sibling folder, then rename it into
    # place, so a batch folder only ever appears complete.
    # Temp folders left by earlier interrupted runs of this batch are never
    # renamed into place, so they are safe to remove.
    for stale in raw_dir.parent.glob(f".tmp_{raw_dir.name}_*"):
        shutil.rmtree(stale, ignore_errors=True)

    tmp_dir = raw_dir.parent / f".tmp_{raw_dir.name}_{os.getpid()}"
    tmp_dir.mkdir(parents=True)

    for name in RAW_FILES:
        path = tmp_dir / name
        output = outputs[name]
        if name.endswith(".json"):
            write_json_lines(path, output)  # type: ignore[arg-type]
        else:
            output.to_csv(path, index=False)  # type: ignore[union-attr]
        fsync_file(path)

    if raw_dir.exists():
        # Only an incomplete folder can reach this point; keep it for inspection.
        aside = raw_dir.parent / f".partial_{raw_dir.name}_{os.getpid()}"
        raw_dir.rename(aside)
        print(f"Moved incomplete folder aside: {aside}")

    tmp_dir.rename(raw_dir)


def main() -> None:
    if BATCH_INDEX < 0:
        raise SystemExit(f"BATCH_DATE={BATCH_DATE} is before ANCHOR_DATE={ANCHOR_DATE.isoformat()}.")

    if raw_dir_is_complete(RAW_DIR):
        print(f"Raw data for {RAW_DIR.name} is already complete. Raw batches are immutable; nothing written.")
        return

    RAW_DIR.parent.mkdir(parents=True, exist_ok=True)
    write_batch(RAW_DIR, build_batch())

    print(f"Raw data generated at: {RAW_DIR}")
    print("Files created:")
    for file_path in sorted(RAW_DIR.iterdir()):
        print(f"- {file_path.name}")


if __name__ == "__main__":
    main()
