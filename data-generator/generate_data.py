from __future__ import annotations

import json
import os
import random
import shutil
import uuid
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
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

# Master data dates are anchored, never based on execution time. Source IDs and
# existing key functions stay unchanged; regenerated facts have new semantics.
ANCHOR_DATE = date(2026, 5, 18)
BATCH_DAY = date.fromisoformat(BATCH_DATE)
BATCH_INDEX = (BATCH_DAY - ANCHOR_DATE).days
IS_ANCHOR_BATCH = BATCH_INDEX == 0

# Fact IDs are unique across batches: BATCH_INDEX * ID_BLOCK + n. Batch 0 keeps
# the original 1..count IDs. Every count must stay below ID_BLOCK, and the IDs
# fit Silver/Gold/staging INTEGER (int32) for ~21,000 days after the anchor.
ID_BLOCK = 100_000
INT32_MAX = 2_147_483_647

# The anchor batch uses the original volume ceilings over a 120-day window. Later
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
    "generation_truth.json",
]

SESSION_TIMEOUT_SECONDS = 30 * 60
CURRENCY = "INR"
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

# Attributes added after the first data set (order status, tax and shipping,
# ad-platform results, product brand and status, campaign objective). Each value
# comes from its own random stream, seeded from the row's ID, so none of the
# draws above changes and a row has the same value in every batch.
ORDER_STATUSES = ["completed", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.88, 0.07, 0.05]
TAX_RATE = 0.18                      # GST on the net order value
FREE_SHIPPING_THRESHOLD = 4999.00   # INR: orders from this net value ship free
SHIPPING_FEE = 99.00
PRODUCT_STATUSES = ["active", "discontinued"]
DISCONTINUED_SHARE = 0.06
PRODUCT_BRANDS = {
    "Electronics": ["Voltix", "Nexora", "Pixelo"],
    "Fashion": ["Urbane", "Threadly", "Kairo"],
    "Home": ["Hearthly", "Nestwell", "Casaro"],
    "Beauty": ["Lumera", "Velvetine", "Aurae"],
    "Fitness": ["Stridex", "Corefit", "Pulseon"],
    "Books": ["Inkwell", "Pageturn", "Leaflet"],
}
CAMPAIGN_TYPES = ["awareness", "consideration", "conversion", "retention"]

# Prices: a realistic INR list-price range per category (log-uniform, so cheap
# products are more common than expensive ones), ending in 9 like a price tag.
PRICE_RANGES = {
    "Electronics": (799, 14999),
    "Fashion": (399, 3999),
    "Home": (299, 7999),
    "Beauty": (149, 1499),
    "Fitness": (299, 4999),
    "Books": (149, 899),
}
# Units per line: most lines are one unit. The generator's 1..5 draw is kept
# (so no later draw moves) and mapped onto these quantities.
QUANTITY_OF_DRAW = {1: 1, 2: 1, 3: 1, 4: 2, 5: 3}

# The ad platform's daily feed is one row per campaign, day and region. Each
# campaign targets a few of these states. Spend comes from the campaign's
# budget; clicks, impressions, conversions and attributed revenue follow from
# the spend through the channel's cost per click and click-through rate and a
# return on ad spend, so the platform's figures agree with each other.
AD_REGIONS = ["Andhra Pradesh", "Bihar", "Gujarat", "Haryana", "Karnataka", "Kerala", "Madhya Pradesh",
              "Maharashtra", "Punjab", "Rajasthan", "Tamil Nadu", "Telangana", "Uttar Pradesh", "West Bengal"]
AD_REGIONS_PER_CAMPAIGN = (2, 5)
# The states a city can be in (the customer master's states). An order or a web
# event from the customer's home city is in the customer's home state; any
# other city is always placed in the same state.
INDIA_STATES = ["Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa", "Gujarat", "Haryana",
                "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur",
                "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu",
                "Telangana", "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal"]
# traffic_source: (cost per click in INR, click-through rate), each a (low, high) range.
CHANNEL_BENCHMARKS = {
    "google": ((18.0, 45.0), (0.020, 0.060)),
    "facebook": ((8.0, 22.0), (0.008, 0.020)),
    "instagram": ((9.0, 25.0), (0.006, 0.018)),
    "email": ((6.0, 14.0), (0.020, 0.050)),
    "affiliate": ((12.0, 30.0), (0.010, 0.030)),
    "organic": ((5.0, 12.0), (0.010, 0.030)),
}
DEFAULT_BENCHMARK = ((10.0, 30.0), (0.010, 0.030))
# The return on ad spend the platform reports for a campaign-day-region
# (lognormal: median about 3, rarely above 8), and the value of one conversion.
PLATFORM_ROAS = (1.1, 0.45)
PLATFORM_ROAS_CAP = 9.0
PLATFORM_CONVERSION_VALUE = (2500.0, 9000.0)


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


def product_brand(product_id: int, category: object) -> str:
    brands = PRODUCT_BRANDS.get(category) or sorted(brand for names in PRODUCT_BRANDS.values() for brand in names)
    return random.Random(f"brand|{product_id}").choice(brands)


def add_product_brand_and_status(df: pd.DataFrame) -> pd.DataFrame:
    """brand and product_status, inserted after category; their own streams per product."""
    brand = [product_brand(int(pid), category) for pid, category in zip(df["product_id"], df["category"])]
    status = [PRODUCT_STATUSES[random.Random(f"product_status|{int(pid)}").random() < DISCONTINUED_SHARE]
              for pid in df["product_id"]]
    position = df.columns.get_loc("category") + 1
    df.insert(position, "brand", brand)
    df.insert(position + 1, "product_status", status)
    return df


def list_price(category: str, position: float) -> float:
    """The list price at position 0..1 of the category's range: log-uniform, ending in 9."""
    low, high = PRICE_RANGES[category]
    price = low * (high / low) ** position
    return float(max(low, int(price // 10) * 10 + 9))


def generate_products(count: int = 150) -> pd.DataFrame:
    rows = []

    for product_id in range(1, count + 1):
        # The same draws, in the same order, as before prices depended on the
        # category: the first one now places the price inside its category's range.
        price_position = (random.uniform(199, 99999) - 199) / (99999 - 199)
        discount_percent = random.choice(DISCOUNT_PERCENTS)
        product_name = random.choice(PRODUCT_NAMES)
        category = random.choice(PRODUCT_CATEGORIES)
        original_price = list_price(category, price_position)
        discounted_price = round(original_price * (1 - discount_percent / 100), 2)

        rows.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
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
    df = add_product_brand_and_status(df)

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
                # A lifetime budget in INR, sized to the shop: a campaign sells a
                # few dozen orders of a few thousand rupees each.
                "budget": round(random.uniform(25000, 300000), 2),
                "campaign_status": random.choice(CAMPAIGN_STATUSES),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "schema_version": SCHEMA_VERSION,
                "source": "marketing_campaign_master",
                "updated_at": random_datetime(random, ANCHOR_DATE, 60, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)
    # The campaign's objective, from its own stream per campaign.
    df.insert(df.columns.get_loc("traffic_source") + 1, "campaign_type",
              [random.Random(f"campaign_type|{int(cid)}").choice(CAMPAIGN_TYPES) for cid in df["campaign_id"]])

    # budget is a lifetime authorization in INR, not a daily allowance. Changes
    # replace that authorization; SCD2 versions are never additive budgets.
    df["updated_at"] = df["start_date"] + "T00:00:00"

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
        # Sold out and withdrawn: a tracked status change in the same version.
        changed["product_status"] = "discontinued"
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

    # One more product changes only its status (SCD2-tracked), picked by a
    # separate stream so the price changes above keep their draws.
    status_rng = random.Random(f"product_status_change|{BATCH_DATE}")
    price_changed = {int(change["product_id"].iloc[0]) for change in changes}
    candidates = sorted(int(value) for value in df["product_id"].dropna().unique()
                        if int(value) not in NON_CHANGEABLE_IDS["products"] and int(value) not in price_changed)
    changed = base_row(df, "product_id", status_rng.choice(candidates))
    changed["product_status"] = "active" if changed["product_status"].iloc[0] == "discontinued" else "discontinued"
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

        quantity = QUANTITY_OF_DRAW[rng.randint(1, 5)] if event_type in ["add_to_cart", "checkout", "purchase"] else 0
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

    assign_sessions(events, campaigns)
    # Where the event happened: the state of its city (a field of its own, from
    # the city alone, so no draw above changes).
    for event in events:
        event["state"] = event_state(event)

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


def event_state(event: dict) -> str | None:
    city = event.get("city")
    if not isinstance(city, str) or not city:
        return None
    if city == event.get("home_city") and isinstance(event.get("home_state"), str):
        return event["home_state"]
    return random.Random(f"state|{city}").choice(INDIA_STATES)


def assign_sessions(events: list[dict], campaigns: pd.DataFrame) -> None:
    """A visit belongs to one customer and attribution context; >30m idle ends it.

    Event timestamps are action times, in source Asia/Kolkata time. Counters
    describe only the prefix observed at that event, never the final session.
    A purchase ends a visit. Browsing can stop without purchase or skip stages.
    """
    rng = random.Random(f"sessions|{BATCH_DATE}")
    customer_fields = (
        "user_id", "user_name", "email", "gender", "age", "membership_tier",
        "loyalty_points", "preferred_language", "home_city", "home_state",
        "user_segment", "is_prime_user", "country", "city",
    )
    context_fields = ("session_id", "campaign_id", "traffic_source", "ab_test_group",
                      "device_type", "operating_system", "browser", "network_type",
                      "app_version", "ip_address")
    campaign_context = campaigns.dropna(subset=["campaign_id"]).drop_duplicates("campaign_id").set_index("campaign_id")
    intervals: dict[int, list[tuple[datetime, datetime]]] = {}
    offset = 0
    while offset < len(events):
        length = min(rng.choice([1, 2, 2, 3, 3, 4, 5, 8]), len(events) - offset)
        visit = events[offset:offset + length]
        # No resampling purchase outcomes from probabilities or anomaly scores.
        purchase = next((i for i, e in enumerate(visit) if e["event_type"] == "purchase"), None)
        if purchase is not None:
            visit = visit[:purchase + 1]
        first = visit[0].copy()
        if first["campaign_id"] is None:
            first["traffic_source"] = "organic"
        else:
            campaign = campaign_context.loc[first["campaign_id"]]
            first["traffic_source"] = campaign["traffic_source"]
            first["ab_test_group"] = campaign["ab_test_group"]
        gaps = [0] + [rng.randint(1, 900) for _ in visit[1:]]
        duration = sum(gaps)
        window = 120 if IS_ANCHOR_BATCH else FACT_WINDOW_DAYS - 1
        for _ in range(100):
            start = random_datetime(rng, BATCH_DAY, window)
            end = start + timedelta(seconds=duration)
            if end.date() > BATCH_DAY:
                continue
            if all(start > old_end + timedelta(seconds=SESSION_TIMEOUT_SECONDS)
                   or end < old_start - timedelta(seconds=SESSION_TIMEOUT_SECONDS)
                   for old_start, old_end in intervals.get(first["user_id"], [])):
                break
        else:
            raise ValueError("Could not place a non-overlapping customer session")
        intervals.setdefault(first["user_id"], []).append((start, end))
        elapsed, viewed = 0, {}
        for event, gap in zip(visit, gaps):
            event.update({key: first[key] for key in customer_fields + context_fields})
            elapsed += gap
            timestamp = (start + timedelta(seconds=elapsed)).isoformat()
            event["event_time"] = event["event_timestamp"] = timestamp
            event["session_duration_sec"] = elapsed
            event["time_since_last_event_ms"] = gap * 1000
            event["time_on_page_sec"] = min(event["time_on_page_sec"], gap)
            if event["event_type"] == "product_view":
                viewed[event["product_id"]] = viewed.get(event["product_id"], 0) + 1
            event["items_viewed_in_session"] = len(viewed)
            event["repeat_product_view_count"] = max(viewed.get(event["product_id"], 0) - 1, 0)
            event["user_journey_stage"] = {
                "page_view": "awareness", "product_view": "consideration", "search": "consideration",
                "add_to_cart": "cart", "checkout": "checkout", "purchase": "purchase",
            }[event["event_type"]]
        offset += len(visit)
    events.sort(key=lambda event: (event["event_timestamp"], event["event_id"]))


def generate_orders(web_events: list[dict], rng: random.Random, count: int) -> pd.DataFrame:
    """Orders without amounts; cart_value is filled from the items later.

    A purchase event can create at most one order. count is a ceiling, not a
    reason to fabricate repeated transactions from the same event. The explicit
    duplicate-ID quarantine fixture is still applied after sampling.
    """
    purchase_events = [event for event in web_events if event.get("event_type") == "purchase"]

    rows = []
    selected = rng.sample(purchase_events, min(count, len(purchase_events)))
    if len(selected) < 5:
        raise ValueError("At least five purchase events are needed for order fixtures")
    for n, event in enumerate(selected, 1):

        rows.append(
            {
                "order_id": fact_id(n),
                "user_id": event["user_id"],
                "campaign_id": event["campaign_id"],
                "order_timestamp": event["event_timestamp"],
                "payment_method": event["payment_method"] or rng.choice(["UPI", "Credit Card", "Debit Card"]),
                "cart_value": None,
                "fraud_score": event["fraud_score"],
                "country": event["country"],
                "city": event["city"],
                "state": event["state"],
                "schema_version": SCHEMA_VERSION,
                "source": "orders_from_purchase_events",
            }
        )

    df = pd.DataFrame(rows)

    # Dirty record: a duplicate order ID (both rows get that order's total).
    df.loc[0, "order_id"] = df.loc[1, "order_id"]

    return df


def generate_order_items(orders: pd.DataFrame, products: pd.DataFrame, rng: random.Random, count: int) -> pd.DataFrame:
    """Clean order items, with the original generator's random draws."""
    valid_order_ids = orders["order_id"].dropna().astype(int).unique().tolist()
    valid_products = products.dropna(subset=["product_id"]).drop_duplicates("product_id").to_dict("records")

    rows = []

    for n in range(1, count + 1):
        product = rng.choice(valid_products)
        quantity = QUANTITY_OF_DRAW[rng.randint(1, 5)]
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

    return pd.DataFrame(rows)


def fill_empty_orders(orders: pd.DataFrame, items: pd.DataFrame, fill_rng: random.Random) -> pd.DataFrame:
    """Give every order at least one item by moving one item from an order with two or more.

    fill_rng is its own stream, so the main fact stream (and with it ad spend)
    is not disturbed. Donors and their items are chosen from sorted lists, so
    the result is deterministic per BATCH_DATE.
    """
    items = items.copy()
    order_ids = sorted(set(orders["order_id"].dropna().astype(int)))
    if len(items) < len(order_ids):
        raise ValueError(f"{len(items)} order items cannot cover {len(order_ids)} orders")

    items_by_order: dict[int, list[int]] = {}
    for index, order_id in items["order_id"].items():
        items_by_order.setdefault(int(order_id), []).append(index)

    for empty_order in (order_id for order_id in order_ids if order_id not in items_by_order):
        donor = fill_rng.choice(sorted(order_id for order_id, indexes in items_by_order.items() if len(indexes) >= 2))
        moved = fill_rng.choice(sorted(items_by_order[donor]))
        items_by_order[donor].remove(moved)
        items_by_order[empty_order] = [moved]
        items.loc[moved, "order_id"] = empty_order

    return items


def order_charges(net: float) -> tuple[float, float, float]:
    """(tax_amount, shipping_amount, total_amount) of an order with this net value."""
    # Exact decimal arithmetic, rounded half up to the cent, so the amounts are
    # the same in pandas, Spark and Postgres.
    cents = Decimal("0.01")
    net_value = Decimal(str(net))
    tax = (net_value * Decimal(str(TAX_RATE))).quantize(cents, rounding=ROUND_HALF_UP)
    shipping = Decimal("0.00") if net >= FREE_SHIPPING_THRESHOLD else Decimal(str(SHIPPING_FEE)).quantize(cents)
    return float(tax), float(shipping), float((net_value + tax + shipping).quantize(cents, rounding=ROUND_HALF_UP))


def add_order_status_and_charges(orders: pd.DataFrame) -> pd.DataFrame:
    """order_status, tax_amount, shipping_amount and total_amount, inserted after cart_value.

    cart_value stays the net value (the sum of the lines); total_amount is what
    the customer was charged: net + tax + shipping. It has that meaning in
    every layer (Silver, Gold, the warehouse, the marts and the exports). The status comes from its
    own stream per order ID, so a duplicated order ID has one status.
    """
    orders = orders.copy()
    status = [random.Random(f"order_status|{int(order_id)}").choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)[0]
              for order_id in orders["order_id"]]
    charges = [order_charges(float(net)) for net in orders["cart_value"]]
    position = orders.columns.get_loc("cart_value") + 1
    orders.insert(position, "tax_amount", [tax for tax, _, _ in charges])
    orders.insert(position + 1, "shipping_amount", [shipping for _, shipping, _ in charges])
    orders.insert(position + 2, "total_amount", [total for _, _, total in charges])
    orders.insert(orders.columns.get_loc("payment_method"), "order_status", status)
    return orders


def generate_orders_and_items(
    web_events: list[dict],
    products: pd.DataFrame,
    rng: random.Random,
    fill_rng: random.Random,
    order_count: int,
    item_count: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Orders whose cart_value is the sum of their own items' line_amount.

    1. orders and items with the original random draws (rng)
    2. every empty order takes one item from an order with two or more (fill_rng)
    3. cart_value = round(sum of the order's clean line_amounts, 2)
    4. the dirty records last, so they stay dirty
    """
    orders = generate_orders(web_events, rng, order_count)
    items = generate_order_items(orders, products, rng, item_count)
    items = fill_empty_orders(orders, items, fill_rng)

    totals = items.groupby("order_id")["line_amount"].sum().round(2)
    orders["cart_value"] = orders["order_id"].map(totals).astype(float)
    orders = add_order_status_and_charges(orders)

    # Dirty order records
    orders.loc[2, "cart_value"] = -500
    orders.loc[3, "user_id"] = 999999
    orders.loc[4, "order_timestamp"] = "not-a-date"

    # Dirty order item records (their orders' totals stay the clean sum)
    items.loc[0, "product_id"] = 999999
    items.loc[1, "quantity"] = -3
    items.loc[2, "line_amount"] = 1

    return orders, items


def delivery_surge(campaign_id: int, spend_day: date) -> bool:
    """Independent hidden simulation assignment, never a model input column."""
    return random.Random(f"surge|{campaign_id}|{spend_day}").random() < 0.08


def campaign_regions(campaign_id: int) -> list[tuple[str, float]]:
    """The states a campaign targets and the share of its spend each one gets."""
    rng = random.Random(f"regions|{campaign_id}")
    regions = sorted(rng.sample(AD_REGIONS, rng.randint(*AD_REGIONS_PER_CAMPAIGN)))
    weights = [rng.uniform(0.5, 2.0) for _ in regions]
    return [(region, weight / sum(weights)) for region, weight in zip(regions, weights)]


def split_spend(amount: float, shares: list[float]) -> list[float]:
    """amount in whole paise per share; the last part takes the rounding remainder, so the parts sum to amount."""
    cents = int(round(amount * 100))
    parts = [int(cents * share) for share in shares[:-1]]
    parts.append(cents - sum(parts))
    return [part / 100 for part in parts]


def generate_ad_spend(campaigns: pd.DataFrame, rng: random.Random, window_days: int, count: int) -> pd.DataFrame:
    # The ad platform's feed: one row per campaign, day and targeted region, in
    # INR. count is the number of sampled campaign-days; every region of a
    # sampled campaign-day is in the feed, and campaign_day_spend repeats the
    # platform's total for the campaign-day on each of its rows, so a missing
    # or altered regional row is detectable. Only the stated lifetime period
    # is eligible; budget revisions replace the cap, not add daily caps. The
    # budget is compared with the spend summed over regions and days.
    # Non-anchor windows are (batch_date-window_days, batch_date], so adjacent
    # configured 14-day windows cannot bill their shared boundary twice.
    days = window_days + 1 if IS_ANCHOR_BATCH else window_days
    candidates = []
    for campaign_id, versions in campaigns.dropna(subset=["campaign_id"]).groupby("campaign_id"):
        versions = versions.sort_values("updated_at")
        for ago in range(days):
            spend_day = BATCH_DAY - timedelta(days=ago)
            known = versions[versions["updated_at"] <= spend_day.isoformat() + "T00:00:00"]
            if known.empty:
                continue
            campaign = known.iloc[-1].to_dict()
            start, end = date.fromisoformat(campaign["start_date"]), date.fromisoformat(campaign["end_date"])
            if start <= spend_day <= end:
                candidates.append((int(campaign_id), spend_day, campaign, (end-start).days+1))
    selected = rng.sample(candidates, min(count, len(candidates)))
    if len(selected) < 4:
        raise ValueError("At least four eligible campaign-days are needed for spend fixtures")
    rows = []
    for campaign_id, spend_day, campaign, period_days in selected:
        daily_rng = random.Random(f"delivery|{campaign_id}|{spend_day}")
        # Overlapping distributions: surge truth is NOT a spend>budget label.
        # Normal stochastic delivery can also overspend; neither is clipped.
        surge = delivery_surge(campaign_id, spend_day)
        factor = daily_rng.lognormvariate(-0.3, 0.7) * (1.8 if surge else 1.0)
        amount = max(round(float(campaign["budget"]) / period_days * factor, 2), 1.0)
        (cpc_range, ctr_range) = CHANNEL_BENCHMARKS.get(campaign["traffic_source"], DEFAULT_BENCHMARK)
        regions = campaign_regions(campaign_id)
        # A little day-to-day movement between the regions, renormalized.
        shares = [share * daily_rng.uniform(0.85, 1.15) for _, share in regions]
        shares = [share / sum(shares) for share in shares]
        for (region, _), regional_spend in zip(regions, split_spend(amount, shares)):
            # Clicks from the spend and the channel's cost per click; impressions
            # from the clicks and its click-through rate; conversions and their
            # value from the return the platform reports on that spend.
            clicks = max(1, round(regional_spend / daily_rng.uniform(*cpc_range)))
            impressions = max(clicks, round(clicks / daily_rng.uniform(*ctr_range)))
            roas = min(daily_rng.lognormvariate(*PLATFORM_ROAS), PLATFORM_ROAS_CAP)
            value = daily_rng.uniform(*PLATFORM_CONVERSION_VALUE)
            # A whole number of conversions with the expected value spend x ROAS /
            # value: the fraction decides whether one more conversion happened.
            expected = regional_spend * roas / value
            conversions = min(clicks, int(expected) + (daily_rng.random() < expected - int(expected)))
            rows.append({
                "spend_id": fact_id(len(rows) + 1), "campaign_id": campaign_id,
                "traffic_source": campaign["traffic_source"], "spend_date": spend_day.isoformat(),
                "country": "India", "state": region,
                "impressions": impressions, "clicks": clicks,
                "conversions": conversions, "attributed_revenue": round(conversions * value, 2),
                "spend_amount": regional_spend, "campaign_day_spend": amount,
                "schema_version": SCHEMA_VERSION, "source": "marketing_ad_platform",
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------


def append_dirty(df: pd.DataFrame, source_index: int, **values) -> tuple[pd.DataFrame, int]:
    """Append a copy of one row with the given values; return the frame and the new row's index."""
    row = df.iloc[[source_index]].copy()
    for column, value in values.items():
        row[column] = value
    df = pd.concat([df, row], ignore_index=True)
    return df, len(df) - 1


def add_attribute_fixtures(outputs: dict[str, pd.DataFrame]) -> dict[str, dict[int, str]]:
    """One deliberately dirty row per new attribute, appended to its file.

    They are new rows with IDs of their own (above every generated ID of the
    batch), added after all facts exist, so no existing row and no draw
    changes. Silver must quarantine each of them.
    """
    fixtures: dict[str, dict[int, str]] = {name: {} for name in outputs}

    def add(name: str, source_index: int, reason: str, **values) -> None:
        outputs[name], index = append_dirty(outputs[name], source_index, **values)
        fixtures[name][index] = reason

    stamp = f"{BATCH_DATE}T{CHANGE_TIME}"
    add("products.csv", 5, "invalid_product_status", product_id=900001, product_status="recalled", updated_at=stamp)
    add("products.csv", 6, "missing_brand", product_id=900002, brand=None, updated_at=stamp)
    add("campaigns.csv", 5, "invalid_campaign_type", campaign_id=900001, campaign_type="brand_lift", updated_at=stamp)
    add("orders.csv", 5, "invalid_order_status", order_id=fact_id(ID_BLOCK - 1), order_status="shipped")
    add("orders.csv", 6, "negative_tax", order_id=fact_id(ID_BLOCK - 2), tax_amount=-1.0)
    add("orders.csv", 7, "negative_shipping", order_id=fact_id(ID_BLOCK - 3), shipping_amount=-99.0)
    add("orders.csv", 8, "total_amount_mismatch", order_id=fact_id(ID_BLOCK - 4),
        total_amount=round(float(outputs["orders.csv"].loc[8, "total_amount"]) + 10, 2))
    # Ad spend: every dirty row is an extra row, so each real campaign-day keeps
    # all its regional rows and still sums to its campaign_day_spend.
    spend = outputs["ad_spend.csv"]
    add("ad_spend.csv", 0, "negative_spend", spend_id=fact_id(ID_BLOCK - 1), spend_amount=-100)
    add("ad_spend.csv", 1, "clicks_exceed_impressions", spend_id=fact_id(ID_BLOCK - 2),
        clicks=int(spend.loc[1, "impressions"]) + 100)
    # A campaign nobody has delivered: a valid row with an invalid reference,
    # kept in the facts on the invalid-reference member. Its own campaign-day.
    add("ad_spend.csv", 2, "missing_campaign", spend_id=fact_id(ID_BLOCK - 3), campaign_id=999999,
        campaign_day_spend=float(spend.loc[2, "spend_amount"]))
    add("ad_spend.csv", 3, "missing_date", spend_id=fact_id(ID_BLOCK - 4), spend_date=None)
    add("ad_spend.csv", 5, "conversions_exceed_clicks", spend_id=fact_id(ID_BLOCK - 5),
        conversions=int(spend.loc[5, "clicks"]) + 1)
    add("ad_spend.csv", 6, "negative_attributed_revenue", spend_id=fact_id(ID_BLOCK - 6), attributed_revenue=-1.0)
    add("ad_spend.csv", 7, "missing_region", spend_id=fact_id(ID_BLOCK - 7), state=None)
    return fixtures


def write_json_lines(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def fsync_file(path: Path) -> None:
    with path.open("rb") as file:
        os.fsync(file.fileno())


def fsync_dir(path: Path) -> None:
    # A directory's entries (new files, a rename) are durable only once the
    # directory itself is fsynced.
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def raw_dir_is_complete(raw_dir: Path) -> bool:
    # Legacy seven-file batches remain immutable. The new truth sidecar does
    # not authorize automatic replacement of an existing complete raw batch.
    # A CSV always has its header; an empty batch has an empty JSON-lines file.
    return all((raw_dir / name).is_file() and (name.endswith(".json") or (raw_dir / name).stat().st_size > 0)
               for name in RAW_FILES if name != "generation_truth.json")


def build_batch() -> dict[str, object]:
    customers = generate_customers()
    products = generate_products()
    campaigns = generate_campaigns()

    if IS_ANCHOR_BATCH:
        # Batch 0 continues the fixed master stream; session and spend
        # semantics are applied deterministically below.
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
    fill_rng = random.Random(f"order_fill|{BATCH_DATE}")
    orders, order_items = generate_orders_and_items(
        web_events, products, fact_rng, fill_rng, counts["orders"], counts["order_items"]
    )
    ad_spend = generate_ad_spend(campaigns, random.Random(f"spend|{BATCH_DATE}"), window_days, counts["ad_spend"])
    truth = [{"kind": "contract", "generator_version": "session_budget_v3", "currency": CURRENCY,
              "budget_grain": "campaign lifetime authorization, latest known revision replaces prior cap",
              "budget_period": "start_date through end_date inclusive, Asia/Kolkata",
              "spend_grain": "observed campaign-day-region; every region of a sampled campaign-day is present; "
                             "sampled days are not a complete billing ledger",
              "session_timeout_seconds": SESSION_TIMEOUT_SECONDS,
              "session_counters": "observed prefix, not final duration or future events",
              "labels_are_model_inputs": False}]
    for row_index, row in ad_spend.iterrows():
        if True:
            truth.append({"kind": "delivery_scenario", "spend_id": int(row["spend_id"]),
                          "delivery_surge": delivery_surge(int(row["campaign_id"]), date.fromisoformat(row["spend_date"]))})
    for table, fixtures in {
        "customers.csv": {0: "invalid_email", 1: "missing_city", 2: "negative_age", 500: "duplicate_customer"},
        "products.csv": {0: "missing_category", 1: "nonpositive_price", 2: "invalid_discount", 150: "duplicate_product"},
        "campaigns.csv": {0: "missing_id", 1: "invalid_channel", 2: "invalid_period"},
        "orders.csv": {0: "duplicate_order_id", 2: "negative_total", 3: "missing_customer", 4: "invalid_timestamp"},
        "order_items.csv": {0: "missing_product", 1: "negative_quantity", 2: "line_amount_mismatch"},
        "web_events.json": {len(web_events)-1: "invalid_event"},
    }.items():
        truth.extend({"kind": "quality_fixture", "table": table, "row_index": index, "reason": reason}
                     for index, reason in fixtures.items())

    outputs = {
        "customers.csv": customers,
        "products.csv": products,
        "campaigns.csv": campaigns,
        "orders.csv": orders,
        "order_items.csv": order_items,
        "ad_spend.csv": ad_spend,
    }
    for table, fixtures in add_attribute_fixtures(outputs).items():
        truth.extend({"kind": "quality_fixture", "table": table, "row_index": index, "reason": reason}
                     for index, reason in fixtures.items())

    return {**outputs, "web_events.json": web_events, "generation_truth.json": truth}


def write_batch(raw_dir: Path, outputs: dict[str, object]) -> None:
    # Write all source files and the separate simulation truth into a hidden sibling folder, then rename it into
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
    fsync_dir(tmp_dir)

    if raw_dir.exists():
        # Only an incomplete folder can reach this point; keep it for inspection.
        aside = raw_dir.parent / f".partial_{raw_dir.name}_{os.getpid()}"
        raw_dir.rename(aside)
        print(f"Moved incomplete folder aside: {aside}")

    tmp_dir.rename(raw_dir)
    fsync_dir(raw_dir.parent)


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
