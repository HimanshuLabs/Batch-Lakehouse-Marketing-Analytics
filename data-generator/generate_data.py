from __future__ import annotations

import json
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker


fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BATCH_DATE = date.today().isoformat()
RAW_DIR = PROJECT_ROOT / "data" / "raw" / f"batch_date={BATCH_DATE}"

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


def random_date(start_days_ago: int, end_days_ago: int = 0) -> date:
    days_ago = random.randint(end_days_ago, start_days_ago)
    return date.today() - timedelta(days=days_ago)


def random_datetime(start_days_ago: int, end_days_ago: int = 0) -> datetime:
    base_date = random_date(start_days_ago, end_days_ago)
    return datetime.combine(base_date, datetime.min.time()) + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


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
                "updated_at": random_datetime(120, 0).isoformat(),
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

    # SCD2-style changed customer
    changed_customer = df.iloc[[4]].copy()
    changed_customer["membership_tier"] = "Platinum"
    changed_customer["loyalty_points"] = 99999
    changed_customer["user_segment"] = "Enterprise"
    changed_customer["is_prime_user"] = True
    changed_customer["updated_at"] = datetime.now().isoformat()
    df = pd.concat([df, changed_customer], ignore_index=True)

    return df


def generate_products(count: int = 150) -> pd.DataFrame:
    rows = []

    for product_id in range(1, count + 1):
        original_price = round(random.uniform(199, 99999), 2)
        discount_percent = random.choice([0, 5, 10, 15, 20, 25, 30, 40])
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
                "updated_at": random_datetime(90, 0).isoformat(),
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

    # SCD2-style changed product
    changed_product = df.iloc[[4]].copy()
    changed_product["discount_percent"] = 35
    changed_product["discounted_price"] = round(float(changed_product["original_price"].iloc[0]) * 0.65, 2)
    changed_product["inventory_remaining"] = 0
    changed_product["updated_at"] = datetime.now().isoformat()
    df = pd.concat([df, changed_product], ignore_index=True)

    return df


def generate_campaigns(count: int = 40) -> pd.DataFrame:
    rows = []

    for campaign_id in range(1, count + 1):
        start_date = random_date(180, 10)
        end_date = start_date + timedelta(days=random.randint(15, 90))

        rows.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": f"{random.choice(TRAFFIC_SOURCES).title()} Campaign {campaign_id}",
                "traffic_source": random.choice(TRAFFIC_SOURCES),
                "ab_test_group": random.choice(AB_TEST_GROUPS),
                "target_segment": random.choice(USER_SEGMENTS),
                "budget": round(random.uniform(10000, 500000), 2),
                "campaign_status": random.choice(["planned", "active", "paused", "completed"]),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "schema_version": SCHEMA_VERSION,
                "source": "marketing_campaign_master",
                "updated_at": random_datetime(60, 0).isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "campaign_id"] = None
    df.loc[1, "traffic_source"] = "unknown_channel"
    df.loc[2, "end_date"] = "1900-01-01"

    # SCD2-style changed campaign
    changed_campaign = df.iloc[[3]].copy()
    changed_campaign["budget"] = round(float(changed_campaign["budget"].iloc[0]) * 1.25, 2)
    changed_campaign["campaign_status"] = "paused"
    changed_campaign["updated_at"] = datetime.now().isoformat()
    df = pd.concat([df, changed_campaign], ignore_index=True)

    return df


def generate_web_events(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    campaigns: pd.DataFrame,
    count: int = 8000,
) -> list[dict]:
    valid_users = customers.dropna(subset=["user_id"]).drop_duplicates("user_id")
    valid_products = products.dropna(subset=["product_id"]).drop_duplicates("product_id")
    valid_campaign_ids = campaigns["campaign_id"].dropna().astype(int).unique().tolist()

    user_records = valid_users.to_dict("records")
    product_records = valid_products.to_dict("records")

    events = []

    for _ in range(count):
        user = random.choice(user_records)
        product = random.choice(product_records)
        event_time = random_datetime(120, 0)
        event_type = random.choice(EVENT_TYPES)

        quantity = random.randint(1, 5) if event_type in ["add_to_cart", "checkout", "purchase"] else 0
        discounted_price = float(product["discounted_price"])
        cart_value = round(quantity * discounted_price, 2)

        event = {
            "event_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
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
            "user_journey_stage": random.choice(JOURNEY_STAGES),
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
            "search_query": random.choice([fake.word(), fake.word(), None]),
            "time_on_page_sec": random.randint(1, 900),
            "scroll_depth_percent": random.randint(0, 100),
            "hover_duration_ms": random.randint(0, 30000),
            "session_duration_sec": random.randint(10, 7200),
            "items_viewed_in_session": random.randint(1, 50),
            "repeat_product_view_count": random.randint(0, 10),
            "time_since_last_event_ms": random.randint(100, 600000),
            "recommendation_rank": random.randint(1, 20),
            "recommendation_clicked": random.choice([True, False]),
            "recommendation_algorithm": random.choice(RECOMMENDATION_ALGORITHMS),
            "click_position": random.randint(1, 50),
            "engagement_score": round(random.uniform(0, 1), 4),
            "purchase_probability": round(random.uniform(0, 1), 4),
            "cart_abandonment_probability": round(random.uniform(0, 1), 4),
            "ab_test_group": random.choice(AB_TEST_GROUPS),
            "payment_method": random.choice(PAYMENT_METHODS),
            "device_type": random.choice(DEVICE_TYPES),
            "operating_system": random.choice(OPERATING_SYSTEMS),
            "browser": random.choice(BROWSERS),
            "network_type": random.choice(NETWORK_TYPES),
            "app_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
            "traffic_source": random.choice(TRAFFIC_SOURCES),
            "campaign_id": random.choice(valid_campaign_ids + [None]),
            "api_latency_ms": random.randint(20, 5000),
            "page_load_time_ms": random.randint(100, 10000),
            "fraud_score": round(random.uniform(0, 1), 4),
            "country": "India",
            "city": random.choice([user["home_city"], fake.city()]),
            "ip_address": fake.ipv4_public(),
            "schema_version": SCHEMA_VERSION,
            "source": SOURCE_SYSTEM,
            "event_timestamp": event_time.isoformat(),
        }

        events.append(event)

    # Dirty event for quarantine testing
    events.append(
        {
            "event_id": None,
            "session_id": str(uuid.uuid4()),
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


def generate_orders(web_events: list[dict], count: int = 2000) -> pd.DataFrame:
    purchase_events = [event for event in web_events if event.get("event_type") == "purchase"]

    rows = []
    for order_id in range(1, count + 1):
        event = random.choice(purchase_events)

        rows.append(
            {
                "order_id": order_id,
                "user_id": event["user_id"],
                "campaign_id": event["campaign_id"],
                "order_timestamp": event["event_timestamp"],
                "payment_method": event["payment_method"] or random.choice(["UPI", "Credit Card", "Debit Card"]),
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


def generate_order_items(orders: pd.DataFrame, products: pd.DataFrame, count: int = 5000) -> pd.DataFrame:
    valid_order_ids = orders["order_id"].dropna().astype(int).unique().tolist()
    valid_products = products.dropna(subset=["product_id"]).drop_duplicates("product_id").to_dict("records")

    rows = []

    for order_item_id in range(1, count + 1):
        product = random.choice(valid_products)
        quantity = random.randint(1, 5)
        discounted_price = float(product["discounted_price"])
        line_amount = round(quantity * discounted_price, 2)

        rows.append(
            {
                "order_item_id": order_item_id,
                "order_id": random.choice(valid_order_ids),
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


def generate_ad_spend(campaigns: pd.DataFrame, count: int = 1000) -> pd.DataFrame:
    valid_campaigns = campaigns.dropna(subset=["campaign_id"]).drop_duplicates("campaign_id").to_dict("records")

    rows = []

    for spend_id in range(1, count + 1):
        campaign = random.choice(valid_campaigns)
        impressions = random.randint(1000, 500000)
        clicks = random.randint(10, impressions)

        rows.append(
            {
                "spend_id": spend_id,
                "campaign_id": int(campaign["campaign_id"]),
                "traffic_source": campaign["traffic_source"],
                "spend_date": random_date(120, 0).isoformat(),
                "impressions": impressions,
                "clicks": clicks,
                "spend_amount": round(random.uniform(500, 100000), 2),
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


def write_json_lines(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    customers = generate_customers()
    products = generate_products()
    campaigns = generate_campaigns()
    web_events = generate_web_events(customers, products, campaigns)
    orders = generate_orders(web_events)
    order_items = generate_order_items(orders, products)
    ad_spend = generate_ad_spend(campaigns)

    customers.to_csv(RAW_DIR / "customers.csv", index=False)
    products.to_csv(RAW_DIR / "products.csv", index=False)
    campaigns.to_csv(RAW_DIR / "campaigns.csv", index=False)
    orders.to_csv(RAW_DIR / "orders.csv", index=False)
    order_items.to_csv(RAW_DIR / "order_items.csv", index=False)
    ad_spend.to_csv(RAW_DIR / "ad_spend.csv", index=False)
    write_json_lines(RAW_DIR / "web_events.json", web_events)

    print(f"Raw data generated at: {RAW_DIR}")
    print("Files created:")
    for file_path in sorted(RAW_DIR.iterdir()):
        print(f"- {file_path.name}")


if __name__ == "__main__":
    main()