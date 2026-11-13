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


# Every value comes from a named random stream (stream("customer", 17), ...),
# so a row is the same in every batch and one field's draws never move
# another's. Facts and changes use streams named after BATCH_DATE: a batch is
# deterministic, and different batch dates produce different increments.
MASTER_SEED = 42
# Faker writes the free text about people (their names) and the visits'
# IP addresses, with the Indian locale. It is seeded per record or per batch,
# like the streams, so its output is as repeatable as theirs.
FAKER_LOCALE = "en_IN"
fake = Faker(FAKER_LOCALE)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BATCH_DATE = date.fromisoformat(os.environ.get("BATCH_DATE") or date.today().isoformat()).isoformat()
RAW_DIR = PROJECT_ROOT / "data" / "raw" / f"batch_date={BATCH_DATE}"

# Master data dates are anchored, never based on execution time.
ANCHOR_DATE = date(2026, 5, 18)
BATCH_DAY = date.fromisoformat(BATCH_DATE)
BATCH_INDEX = (BATCH_DAY - ANCHOR_DATE).days
IS_ANCHOR_BATCH = BATCH_INDEX == 0

# Fact IDs are unique across batches: BATCH_INDEX * ID_BLOCK + n. Batch 0 keeps
# the original 1..count IDs. Every count must stay below ID_BLOCK, and the IDs
# fit Silver/Gold/staging INTEGER (int32) for ~21,000 days after the anchor.
ID_BLOCK = 100_000
INT32_MAX = 2_147_483_647

# The anchor batch covers a 120-day window; later batches are increments over
# FACT_WINDOW_DAYS with volumes scaled to the same daily rate. web_events is
# the number of events; orders and ad_spend are ceilings (an order exists only
# for a purchase event, a spend row only for a live campaign-day). An order's
# items are the lines of the cart that was bought, so they have no count.
ANCHOR_WINDOW_DAYS = 120
FACT_WINDOW_DAYS = 14
ANCHOR_COUNTS = {"web_events": 8000, "orders": 2000, "ad_spend": 1000}
INCREMENT_COUNTS = {"web_events": 1000, "orders": 250, "ad_spend": 125}

# Changed master records per batch. The anchor batch has a fixed change set
# (customer 5, product 5, campaign 4) with its original timestamps; later
# batches pick keys with a stream named after the batch date and stamp them
# at 08:00:00 of that day.
CHANGE_COUNTS = {"customers": 3, "products": 2, "campaigns": 1}   # Type 2 changes; see the change functions
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
GENERATOR_VERSION = "coherent_source_v6"

SESSION_TIMEOUT_SECONDS = 30 * 60
# Every monetary field of every file is in Indian rupees (INR).
CURRENCY = "INR"
SCHEMA_VERSION = "v1"
SOURCE_SYSTEM = "project_1_behavioral_events"

# --- Customers ------------------------------------------------------------------
GENDERS = ["Male", "Female", "Other"]
GENDER_WEIGHTS = [0.49, 0.49, 0.02]
# Reserved example domains: no address can belong to a real person.
EMAIL_DOMAINS = ["example.com", "example.org", "example.net"]

# A city is in its real state (Delhi is a union territory, listed as the state
# of New Delhi). The weight is the city's relative share of customers.
CITIES = [
    ("Mumbai", "Maharashtra", 9), ("Pune", "Maharashtra", 5), ("Nagpur", "Maharashtra", 2),
    ("Nashik", "Maharashtra", 1), ("Thane", "Maharashtra", 2),
    ("Bengaluru", "Karnataka", 8), ("Mysuru", "Karnataka", 1), ("Mangaluru", "Karnataka", 1),
    ("Hubballi", "Karnataka", 1),
    ("Chennai", "Tamil Nadu", 6), ("Coimbatore", "Tamil Nadu", 2), ("Madurai", "Tamil Nadu", 1),
    ("Tiruchirappalli", "Tamil Nadu", 1),
    ("Hyderabad", "Telangana", 7), ("Warangal", "Telangana", 1),
    ("Visakhapatnam", "Andhra Pradesh", 2), ("Vijayawada", "Andhra Pradesh", 1), ("Guntur", "Andhra Pradesh", 1),
    ("Kolkata", "West Bengal", 6), ("Howrah", "West Bengal", 1), ("Siliguri", "West Bengal", 1),
    ("Ahmedabad", "Gujarat", 4), ("Surat", "Gujarat", 3), ("Vadodara", "Gujarat", 1), ("Rajkot", "Gujarat", 1),
    ("Jaipur", "Rajasthan", 3), ("Jodhpur", "Rajasthan", 1), ("Udaipur", "Rajasthan", 1), ("Kota", "Rajasthan", 1),
    ("Lucknow", "Uttar Pradesh", 3), ("Kanpur", "Uttar Pradesh", 2), ("Varanasi", "Uttar Pradesh", 1),
    ("Agra", "Uttar Pradesh", 1), ("Noida", "Uttar Pradesh", 2),
    ("Bhopal", "Madhya Pradesh", 2), ("Indore", "Madhya Pradesh", 2), ("Gwalior", "Madhya Pradesh", 1),
    ("Patna", "Bihar", 2), ("Gaya", "Bihar", 1),
    ("Kochi", "Kerala", 2), ("Thiruvananthapuram", "Kerala", 1), ("Kozhikode", "Kerala", 1),
    ("Ludhiana", "Punjab", 1), ("Amritsar", "Punjab", 1), ("Jalandhar", "Punjab", 1),
    ("Gurugram", "Haryana", 3), ("Faridabad", "Haryana", 1),
    ("New Delhi", "Delhi", 8),
    ("Bhubaneswar", "Odisha", 1), ("Cuttack", "Odisha", 1),
    ("Guwahati", "Assam", 1), ("Ranchi", "Jharkhand", 1), ("Jamshedpur", "Jharkhand", 1),
    ("Raipur", "Chhattisgarh", 1), ("Dehradun", "Uttarakhand", 1), ("Panaji", "Goa", 1),
]
CITY_STATE = {city: state for city, state, _ in CITIES}
# The language most people of a state speak at home. A customer prefers it,
# Hindi or English; in the Hindi-speaking states the choice is Hindi or English.
STATE_LANGUAGE = {
    "Maharashtra": "Marathi", "Karnataka": "Kannada", "Tamil Nadu": "Tamil", "Telangana": "Telugu",
    "Andhra Pradesh": "Telugu", "West Bengal": "Bengali", "Gujarat": "Gujarati", "Kerala": "Malayalam",
    "Punjab": "Punjabi", "Odisha": "Odia", "Assam": "Assamese", "Goa": "Konkani",
    "Rajasthan": "Hindi", "Uttar Pradesh": "Hindi", "Madhya Pradesh": "Hindi", "Bihar": "Hindi", "Haryana": "Hindi",
    "Delhi": "Hindi", "Jharkhand": "Hindi", "Chhattisgarh": "Hindi", "Uttarakhand": "Hindi",
}
LANGUAGES = sorted({"English", "Hindi", *STATE_LANGUAGE.values()})
# membership_tier follows loyalty_points: the tier whose threshold was reached.
MEMBERSHIP_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
TIER_THRESHOLDS = {"Bronze": 0, "Silver": 5000, "Gold": 15000, "Platinum": 30000}
MEAN_LOYALTY_POINTS = 9000
MAX_LOYALTY_POINTS = 60000
# A paid membership: more common the more a customer buys.
PRIME_SHARE_BY_TIER = {"Bronze": 0.10, "Silver": 0.25, "Gold": 0.45, "Platinum": 0.65}
# The ages a segment is plausible for, and how common it is among them.
USER_SEGMENTS = ["Student", "Working Professional", "Small Business", "Enterprise"]
SEGMENT_AGES = {"Student": (18, 26), "Working Professional": (22, 60), "Small Business": (25, 65),
                "Enterprise": (30, 65)}
SEGMENT_WEIGHTS = {"Student": 4, "Working Professional": 6, "Small Business": 2, "Enterprise": 1}
# An account was opened between about four months and four years before the anchor date.
SIGNUP_DAYS_AGO = (130, 1500)

# --- Products -------------------------------------------------------------------
# The catalogue: what a category sells and the INR list-price range of each
# item. A product is one brand's version of one item, so name, category and
# brand agree and no two products share a name. Brand names are invented for
# this project (the Books brands are publishers).
PRODUCT_BRANDS = {
    "Electronics": ["Voltaro", "Quenzo", "Pixavi"],
    "Fashion": ["Threadora", "Kairovi", "Vastrio"],
    "Home": ["Hearthiq", "Casavio", "Nidora"],
    "Beauty": ["Auraeva", "Velvora", "Glimra"],
    "Fitness": ["Stridiq", "Corvanta", "Pulsivo"],
    "Books": ["Inkvale Press", "Pagemont Books", "Leafora Publishing"],
}
CATALOGUE = {
    "Electronics": [("Wireless Mouse", 499, 1999), ("Mechanical Keyboard", 1999, 6999),
                    ("Bluetooth Speaker", 999, 7999), ("Smart Watch", 1999, 14999), ("LED Monitor", 6999, 14999),
                    ("Laptop Stand", 699, 2499), ("Wireless Earbuds", 999, 9999), ("Power Bank", 799, 2999),
                    ("USB-C Hub", 899, 3999), ("Webcam", 1299, 5999), ("Gaming Headset", 1499, 7999),
                    ("Portable SSD", 3999, 12999)],
    "Fashion": [("Running Shoes", 1499, 5999), ("Backpack", 799, 3499), ("Denim Jacket", 1499, 3999),
                ("Cotton Kurta", 499, 1999), ("Leather Wallet", 399, 1499), ("Sunglasses", 499, 2999),
                ("Analog Watch", 999, 4999), ("Formal Shirt", 699, 2499), ("Sneakers", 1299, 4999),
                ("Silk Scarf", 399, 1499), ("Canvas Tote Bag", 399, 1299), ("Rain Jacket", 999, 3499)],
    "Home": [("Office Chair", 3999, 12999), ("Table Lamp", 599, 2999), ("Cotton Bedsheet Set", 699, 2999),
             ("Nonstick Cookware Set", 1499, 5999), ("Wall Clock", 399, 1999), ("Storage Organizer", 299, 1499),
             ("Air Purifier", 4999, 14999), ("Curtain Set", 799, 3499), ("Dinner Set", 1499, 6999),
             ("Steel Water Bottle", 299, 999), ("Memory Foam Pillow", 599, 2499), ("Study Table", 2999, 9999)],
    "Beauty": [("Face Serum", 349, 1499), ("Sunscreen SPF 50", 249, 899), ("Hair Oil", 149, 599),
               ("Lip Balm", 149, 399), ("Body Lotion", 199, 799), ("Face Wash", 149, 599), ("Shampoo", 199, 799),
               ("Eau de Parfum", 499, 2999), ("Kajal", 149, 499), ("Night Cream", 349, 1499),
               ("Beard Oil", 249, 799), ("Sheet Mask Pack", 199, 699)],
    "Fitness": [("Yoga Mat", 499, 2499), ("Dumbbell Set", 999, 4999), ("Resistance Bands", 299, 1299),
                ("Skipping Rope", 199, 799), ("Gym Bag", 599, 2499), ("Foam Roller", 499, 1999),
                ("Protein Shaker", 199, 699), ("Fitness Tracker", 1499, 4999), ("Exercise Ball", 499, 1499),
                ("Push-Up Bars", 399, 1299), ("Kettlebell", 799, 3499), ("Cycling Gloves", 299, 999)],
    "Books": [("Pocket English Dictionary", 199, 499), ("Atlas of India", 299, 899),
              ("Beginner's Guide to Python", 399, 899), ("Indian Vegetarian Cookbook", 299, 799),
              ("Sudoku Puzzle Collection", 149, 399), ("Children's Picture Encyclopedia", 399, 899),
              ("Personal Finance Handbook", 249, 699), ("Yoga for Beginners", 199, 599),
              ("Short Stories Anthology", 199, 499), ("World History Primer", 299, 799),
              ("Daily Planner Diary", 149, 499), ("Mathematics Practice Workbook", 149, 449)],
}
PRODUCT_CATEGORIES = list(CATALOGUE)
# The category's INR list-price range: that of its cheapest and dearest item.
PRICE_RANGES = {category: (min(low for _, low, _ in items), max(high for _, _, high in items))
                for category, items in CATALOGUE.items()}
DISCOUNT_PERCENTS = [0, 5, 10, 15, 20, 25, 30, 40]
DISCOUNT_WEIGHTS = [30, 12, 18, 12, 12, 7, 5, 4]
PRODUCT_STATUSES = ["active", "discontinued"]
DISCONTINUED_SHARE = 0.06
OUT_OF_STOCK_SHARE = 0.04
# Units per cart line: most lines are one unit.
QUANTITIES = [1, 2, 3]
QUANTITY_WEIGHTS = [0.6, 0.2, 0.2]

# --- Campaigns ------------------------------------------------------------------
# A campaign is paid marketing on one channel. Organic traffic is nobody's
# campaign: it has no budget and no ad spend, and an organic session or order
# has no campaign_id (the unknown member in the warehouse).
PAID_CHANNELS = ["google", "facebook", "instagram", "email", "affiliate"]
TRAFFIC_SOURCES = [*PAID_CHANNELS, "organic"]
CHANNEL_LABELS = {"google": "Google Search", "facebook": "Facebook Feed", "instagram": "Instagram Reels",
                  "email": "Email Newsletter", "affiliate": "Affiliate Partners"}
CAMPAIGN_TYPES = ["awareness", "consideration", "conversion", "retention"]
CAMPAIGN_THEMES = {
    "awareness": ["Brand Launch", "New Arrivals", "Store Introduction"],
    "consideration": ["Festive Lookbook", "Buying Guide", "Top Picks"],
    "conversion": ["Flash Sale", "Big Savings Days", "Weekend Offer"],
    "retention": ["Loyalty Rewards", "Win-Back Offer", "Members Week"],
}
CAMPAIGN_STATUSES = ["planned", "active", "paused", "completed"]
AB_TEST_GROUPS = ["A", "B", "control"]
# A campaign starts between 150 days before and 45 days after the anchor date
# and runs 15 to 90 days. Its lifetime budget is a daily amount in INR times
# the days it runs.
CAMPAIGN_START_DAYS = (-150, 45)
CAMPAIGN_RUN_DAYS = (15, 90)
CAMPAIGN_DAILY_BUDGET = (500.0, 2000.0)
CAMPAIGN_PLANNING_DAYS = (7, 30)
# Share of sessions that come without a campaign when campaigns are running.
ORGANIC_SHARE = 0.4

# --- Web events -----------------------------------------------------------------
JOURNEY_STAGES = ["awareness", "consideration", "cart", "checkout", "purchase", "retention"]
EVENT_TYPES = ["page_view", "product_view", "search", "add_to_cart", "checkout", "purchase"]
# Adding to the cart is the commonest step of a visit that buys.
EVENT_TYPE_WEIGHTS = [1.0, 1.2, 0.8, 1.4, 0.8, 1.0]
CART_EVENT_TYPES = ("add_to_cart", "checkout", "purchase")
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]
PAYMENT_WEIGHTS = [0.45, 0.15, 0.12, 0.08, 0.20]
# A device runs one of its own operating systems, and a browser that exists
# for it; a phone is on mobile data or WiFi, a desktop on WiFi or broadband.
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.65, 0.27, 0.08]
DEVICE_PROFILES = {
    "mobile": {"Android": ["Chrome", "Firefox"], "iOS": ["Safari", "Chrome"]},
    "tablet": {"Android": ["Chrome"], "iOS": ["Safari", "Chrome"]},
    "desktop": {"Windows": ["Chrome", "Edge", "Firefox"], "macOS": ["Safari", "Chrome", "Firefox"],
                "Ubuntu": ["Chrome", "Firefox"]},
}
DEVICE_NETWORKS = {"mobile": ["4G", "5G", "WiFi"], "tablet": ["WiFi", "4G", "5G"], "desktop": ["WiFi", "Broadband"]}
OPERATING_SYSTEMS = sorted({system for systems in DEVICE_PROFILES.values() for system in systems})
BROWSERS = sorted({browser for systems in DEVICE_PROFILES.values() for names in systems.values() for browser in names})
NETWORK_TYPES = sorted({network for networks in DEVICE_NETWORKS.values() for network in networks})
RECOMMENDATION_ALGORITHMS = ["collaborative_filtering", "content_based", "ranking_model", "popular_items"]
# Share of sessions from the customer's home city (the rest are from another city).
HOME_CITY_SHARE = 0.88

# --- Orders ---------------------------------------------------------------------
ORDER_STATUSES = ["completed", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.88, 0.07, 0.05]
# The life of an order. It is placed at order_timestamp. A cancelled order is
# cancelled within hours; a returned order comes back days later, with a full
# or a partial refund of its net value. Until that happens the source shows the
# order as completed: a batch delivers what was true at the end of its day, and
# the batch in which the cancellation or return falls delivers the order again
# with its new status, status_timestamp, refund_amount and a later updated_at.
CANCEL_AFTER_SECONDS = (5 * 60, 6 * 3600)
RETURN_AFTER_SECONDS = (2 * 86400, 20 * 86400)
FULL_REFUND_SHARE = 0.7
PARTIAL_REFUND = (0.3, 0.9)
# Fact IDs from this number up in a batch's ID block are the appended dirty fixtures.
FIXTURE_ID_START = ID_BLOCK - 100
TAX_RATE = 0.18                      # GST on the net order value
FREE_SHIPPING_THRESHOLD = 4999.00   # INR: orders from this net value ship free
SHIPPING_FEE = 99.00                # INR

# --- Ad spend -------------------------------------------------------------------
# The ad platform's daily feed is one row per campaign, day and region. Each
# campaign targets a few of these states. Spend comes from the campaign's
# budget; clicks, impressions, conversions and attributed revenue follow from
# the spend through the channel's cost per click and click-through rate and a
# return on ad spend, so the platform's figures agree with each other.
AD_REGIONS = ["Andhra Pradesh", "Bihar", "Gujarat", "Haryana", "Karnataka", "Kerala", "Madhya Pradesh",
              "Maharashtra", "Punjab", "Rajasthan", "Tamil Nadu", "Telangana", "Uttar Pradesh", "West Bengal"]
AD_REGIONS_PER_CAMPAIGN = (2, 5)
# traffic_source: (cost per click in INR, click-through rate), each a (low, high) range.
CHANNEL_BENCHMARKS = {
    "google": ((18.0, 45.0), (0.020, 0.060)),
    "facebook": ((8.0, 22.0), (0.008, 0.020)),
    "instagram": ((9.0, 25.0), (0.006, 0.018)),
    "email": ((6.0, 14.0), (0.020, 0.050)),
    "affiliate": ((12.0, 30.0), (0.010, 0.030)),
}
DEFAULT_BENCHMARK = ((10.0, 30.0), (0.010, 0.030))
# The return on ad spend the platform reports for a campaign-day-region
# (lognormal: median about 1.8, rarely above 5), and the value in INR of one conversion.
PLATFORM_ROAS = (0.6, 0.45)
PLATFORM_ROAS_CAP = 9.0
PLATFORM_CONVERSION_VALUE = (1200.0, 5000.0)


def stream(*parts: object) -> random.Random:
    """The random stream with this name: the same draws in every run and every batch."""
    return random.Random("|".join(str(part) for part in (MASTER_SEED, *parts)))


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


def weighted(rng: random.Random, values: list, weights: list):
    return rng.choices(values, weights)[0]


# ---------------------------------------------------------------------------
# Master data: the same base rows in every batch
# ---------------------------------------------------------------------------


def tier_for_points(points: int) -> str:
    return max((tier for tier, threshold in TIER_THRESHOLDS.items() if points >= threshold),
               key=TIER_THRESHOLDS.get)


def segment_for_age(rng: random.Random, age: int) -> str:
    plausible = [segment for segment, (low, high) in SEGMENT_AGES.items() if low <= age <= high]
    return weighted(rng, plausible, [SEGMENT_WEIGHTS[segment] for segment in plausible])


def language_for_state(rng: random.Random, state: str) -> str:
    regional = STATE_LANGUAGE[state]
    if regional == "Hindi":
        return weighted(rng, ["Hindi", "English"], [0.75, 0.25])
    return weighted(rng, [regional, "Hindi", "English"], [0.6, 0.15, 0.25])


def pick_city(rng: random.Random, exclude: object = None) -> tuple[str, str]:
    places = [(city, state, weight) for city, state, weight in CITIES if city != exclude]
    city, state, _ = weighted(rng, places, [weight for _, _, weight in places])
    return city, state


def email_part(name: str) -> str:
    """A name as it appears in an email address: its letters, in lower case."""
    return "".join(character for character in name.lower() if "a" <= character <= "z")


def generate_customers(count: int = 500) -> pd.DataFrame:
    rows = []

    for user_id in range(1, count + 1):
        rng = stream("customer", user_id)
        gender = weighted(rng, GENDERS, GENDER_WEIGHTS)
        # Faker's Indian names; the first name agrees with the gender ("Other"
        # has no list of its own and takes a name from either).
        fake.seed_instance(f"{MASTER_SEED}|customer|{user_id}")
        named_as = gender if gender in ("Male", "Female") else rng.choice(["Male", "Female"])
        first_name = fake.first_name_male() if named_as == "Male" else fake.first_name_female()
        surname = fake.last_name()
        age = rng.randint(18, 65)
        points = min(int(rng.expovariate(1 / MEAN_LOYALTY_POINTS)), MAX_LOYALTY_POINTS)
        tier = tier_for_points(points)
        home_city, home_state = pick_city(rng)

        rows.append(
            {
                "user_id": user_id,
                "user_name": f"{first_name} {surname}",
                # Derived from the name; the customer number keeps it unique.
                "email": f"{email_part(first_name)}.{email_part(surname)}{user_id}@{rng.choice(EMAIL_DOMAINS)}",
                "gender": gender,
                "age": age,
                "membership_tier": tier,
                "loyalty_points": points,
                "preferred_language": language_for_state(rng, home_state),
                "home_city": home_city,
                "home_state": home_state,
                "country": "India",
                "user_segment": segment_for_age(rng, age),
                "is_prime_user": rng.random() < PRIME_SHARE_BY_TIER[tier],
                # The day the account was opened: before every other date of the customer.
                "signup_date": (ANCHOR_DATE - timedelta(days=rng.randint(*SIGNUP_DAYS_AGO))).isoformat(),
                "schema_version": SCHEMA_VERSION,
                "source": "crm_customer_master",
                "updated_at": random_datetime(rng, ANCHOR_DATE, 120, 1).isoformat(),
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


def list_price(low: float, high: float, position: float) -> float:
    """The list price at position 0..1 of an INR range: log-uniform, ending in 9."""
    price = low * (high / low) ** position
    return float(min(max(low, int(price // 10) * 10 + 9), high))


def catalogue() -> list[tuple[str, str, str, int, int]]:
    """(category, brand, item, low, high) for every product ID, in order: no pair of brand and item twice."""
    per_category = {}
    for category, items in CATALOGUE.items():
        pairs = [(brand, item, low, high) for brand in PRODUCT_BRANDS[category] for item, low, high in items]
        stream("catalogue", category).shuffle(pairs)
        per_category[category] = pairs
    entries = []
    for position in range(max(len(pairs) for pairs in per_category.values())):
        for category in PRODUCT_CATEGORIES:
            if position < len(per_category[category]):
                entries.append((category, *per_category[category][position]))
    return entries


def generate_products(count: int = 150) -> pd.DataFrame:
    entries = catalogue()
    if count > len(entries):
        raise ValueError(f"The catalogue has {len(entries)} distinct products; {count} were asked for")
    rows = []

    for product_id in range(1, count + 1):
        rng = stream("product", product_id)
        category, brand, item, low, high = entries[product_id - 1]
        original_price = list_price(low, high, rng.random())
        discount_percent = weighted(rng, DISCOUNT_PERCENTS, DISCOUNT_WEIGHTS)
        discontinued = rng.random() < DISCONTINUED_SHARE

        rows.append(
            {
                "product_id": product_id,
                "product_name": f"{brand} {item}",
                "category": category,
                "brand": brand,
                "product_status": PRODUCT_STATUSES[discontinued],
                "original_price": original_price,
                "discount_percent": discount_percent,
                "discounted_price": round(original_price * (1 - discount_percent / 100), 2),
                "inventory_remaining": 0 if rng.random() < OUT_OF_STOCK_SHARE else rng.randint(5, 2000),
                "schema_version": SCHEMA_VERSION,
                "source": "product_catalog",
                "updated_at": random_datetime(rng, ANCHOR_DATE, 90, 1).isoformat(),
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


def campaign_status_on(start: date, end: date, day: date) -> str:
    """What the calendar alone says about a campaign on a day."""
    if day < start:
        return "planned"
    return "active" if day <= end else "completed"


def status_since(start: date, end: date, status: str, created: date) -> str:
    """The moment a campaign entered a calendar status."""
    began = {"planned": created, "active": start, "completed": end + timedelta(days=1)}[status]
    return f"{began.isoformat()}T00:00:00"


def generate_campaigns(count: int = 40) -> pd.DataFrame:
    rows = []

    for campaign_id in range(1, count + 1):
        rng = stream("campaign", campaign_id)
        channel = rng.choice(PAID_CHANNELS)
        campaign_type = rng.choice(CAMPAIGN_TYPES)
        start_date = ANCHOR_DATE + timedelta(days=rng.randint(*CAMPAIGN_START_DAYS))
        run_days = rng.randint(*CAMPAIGN_RUN_DAYS)
        if campaign_id == 4:
            # The anchor batch pauses campaign 4, so it is running on the anchor date.
            start_date = ANCHOR_DATE - timedelta(days=rng.randint(20, 40))
            run_days = rng.randint(60, 90)
        end_date = start_date + timedelta(days=run_days - 1)
        # Planned some days ahead, and always before the first batch.
        created = min(start_date - timedelta(days=rng.randint(*CAMPAIGN_PLANNING_DAYS)),
                      ANCHOR_DATE - timedelta(days=1))
        status = campaign_status_on(start_date, end_date, ANCHOR_DATE)

        rows.append(
            {
                "campaign_id": campaign_id,
                # The channel's own name, what the campaign promotes, and its number.
                "campaign_name": f"{CHANNEL_LABELS[channel]} {rng.choice(CAMPAIGN_THEMES[campaign_type])} {campaign_id}",
                "traffic_source": channel,
                "campaign_type": campaign_type,
                "ab_test_group": rng.choice(AB_TEST_GROUPS),
                "target_segment": rng.choice(USER_SEGMENTS),
                # A lifetime budget in INR: a daily amount for every day it runs.
                "budget": round(rng.uniform(*CAMPAIGN_DAILY_BUDGET) * run_days, 2),
                # The status on the anchor date, and since when it holds.
                "campaign_status": status,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "schema_version": SCHEMA_VERSION,
                "source": "marketing_campaign_master",
                "updated_at": status_since(start_date, end_date, status, created),
            }
        )

    df = pd.DataFrame(rows)

    # Dirty records
    df.loc[0, "campaign_id"] = None
    df.loc[1, "traffic_source"] = "unknown_channel"
    df.loc[2, "end_date"] = "1900-01-01"

    return df


# ---------------------------------------------------------------------------
# Changes: what happened to the master data on one batch day
# ---------------------------------------------------------------------------
# A batch file holds the current state of every record (the base rows with all
# changes of the earlier batches applied) and, appended, the versions that
# this batch's day brought. The earlier batches are the raw folders on disk,
# which is why batches are generated in date order.


def base_row(df: pd.DataFrame, key: str, key_value: int) -> pd.DataFrame:
    matches = df.index[df[key] == key_value]
    return df.loc[[matches[0]]].copy()


def changeable_keys(df: pd.DataFrame, key: str, entity: str) -> list[int]:
    return sorted(int(value) for value in df[key].dropna().unique() if int(value) not in NON_CHANGEABLE_IDS[entity])


def change_stamp(day: date, entity: str) -> str:
    return ANCHOR_CHANGE_TIMESTAMPS[entity] if day == ANCHOR_DATE else f"{day.isoformat()}T{CHANGE_TIME}"


def customer_changes(df: pd.DataFrame, day: date) -> list[pd.DataFrame]:
    """Customers who reached a higher tier by earning points, and one who moved."""
    rng = stream("changes", "customers", day)
    stamp = change_stamp(day, "customers")

    def earns_points(user_id: int, to_tier: str | None = None) -> pd.DataFrame:
        changed = base_row(df, "user_id", user_id)
        tier = changed["membership_tier"].iloc[0]
        target = to_tier or MEMBERSHIP_TIERS[MEMBERSHIP_TIERS.index(tier) + 1]
        # Enough points for the next tier: the tier changes because the points did.
        points = max(TIER_THRESHOLDS[target], int(changed["loyalty_points"].iloc[0])) + rng.randint(50, 2500)
        changed["loyalty_points"] = points
        changed["membership_tier"] = tier_for_points(points)
        changed["updated_at"] = stamp
        return changed

    if day == ANCHOR_DATE:
        changed = earns_points(5, "Platinum")
        changed["is_prime_user"] = True
        return [changed]

    keys = changeable_keys(df, "user_id", "customers")
    current = df.drop_duplicates("user_id").set_index("user_id")
    can_rise = [user_id for user_id in keys if current.loc[user_id, "membership_tier"] != MEMBERSHIP_TIERS[-1]]
    risers = sorted(rng.sample(can_rise, CHANGE_COUNTS["customers"] - 1))
    changes = [earns_points(user_id) for user_id in risers]
    # A move changes the city and its state together.
    mover = base_row(df, "user_id", rng.choice([user_id for user_id in keys if user_id not in risers]))
    mover["home_city"], mover["home_state"] = pick_city(rng, exclude=mover["home_city"].iloc[0])
    mover["updated_at"] = stamp

    # The three changes above are history (SCD Type 2). Two more show the other
    # types: a corrected email address is overwritten on every version of the
    # customer (Type 1), and a signup date that the source now states
    # differently is ignored, because the first one is kept (Type 0).
    touched = {*risers, int(mover["user_id"].iloc[0])}
    corrected_id, redated_id = rng.sample([user_id for user_id in keys if user_id not in touched], 2)
    corrected = base_row(df, "user_id", corrected_id)
    local, domain = corrected["email"].iloc[0].split("@")
    corrected["email"] = f"{local}@{rng.choice([name for name in EMAIL_DOMAINS if name != domain])}"
    corrected["updated_at"] = stamp
    redated = base_row(df, "user_id", redated_id)
    redated["signup_date"] = (date.fromisoformat(redated["signup_date"].iloc[0])
                              + timedelta(days=rng.randint(1, 30))).isoformat()
    redated["updated_at"] = stamp
    return [*changes, mover, corrected, redated]


def product_changes(df: pd.DataFrame, day: date) -> list[pd.DataFrame]:
    """Products whose discount (and so price) changed, and one whose status changed."""
    rng = stream("changes", "products", day)
    stamp = change_stamp(day, "products")

    if day == ANCHOR_DATE:
        changed = base_row(df, "product_id", 5)
        changed["discount_percent"] = 35
        changed["discounted_price"] = round(float(changed["original_price"].iloc[0]) * 0.65, 2)
        changed["inventory_remaining"] = 0
        # Sold out and withdrawn: a tracked status change in the same version.
        changed["product_status"] = "discontinued"
        changed["updated_at"] = stamp
        return [changed]

    keys = changeable_keys(df, "product_id", "products")
    changes = []
    for product_id in sorted(rng.sample(keys, CHANGE_COUNTS["products"])):
        changed = base_row(df, "product_id", product_id)
        current_discount = changed["discount_percent"].iloc[0]
        new_discount = rng.choice([d for d in DISCOUNT_PERCENTS if d != current_discount])
        # SCD2-tracked attribute: price (discounted_price). discount_percent
        # changes with it so the row stays internally consistent.
        changed["discount_percent"] = new_discount
        changed["discounted_price"] = round(float(changed["original_price"].iloc[0]) * (1 - new_discount / 100), 2)
        changed["updated_at"] = stamp
        changes.append(changed)

    # One more product changes only its status (SCD2-tracked).
    price_changed = {int(change["product_id"].iloc[0]) for change in changes}
    changed = base_row(df, "product_id", rng.choice([key for key in keys if key not in price_changed]))
    changed["product_status"] = "active" if changed["product_status"].iloc[0] == "discontinued" else "discontinued"
    changed["updated_at"] = stamp
    changes.append(changed)

    # And one is restocked: the stock level is overwritten, with no history (SCD Type 1).
    touched = {int(change["product_id"].iloc[0]) for change in changes}
    restocked = base_row(df, "product_id", rng.choice([key for key in keys if key not in touched]))
    restocked["inventory_remaining"] = int(restocked["inventory_remaining"].iloc[0]) + rng.randint(100, 500)
    restocked["updated_at"] = stamp
    changes.append(restocked)
    return changes


def campaign_changes(df: pd.DataFrame, day: date) -> list[pd.DataFrame]:
    """Campaigns that started or ended by this day, and one running campaign that was revised."""
    rng = stream("changes", "campaigns", day)
    stamp = change_stamp(day, "campaigns")

    if day == ANCHOR_DATE:
        changed = df.iloc[[3]].copy()
        changed["budget"] = round(float(changed["budget"].iloc[0]) * 1.25, 2)
        changed["campaign_status"] = "paused"
        changed["updated_at"] = stamp
        return [changed]

    changes, running = [], []
    for index, row in df.iterrows():
        if pd.isna(row["campaign_id"]):
            continue
        start, end = date.fromisoformat(row["start_date"]), date.fromisoformat(row["end_date"])
        if end < start:
            continue   # the dirty period: nothing can be said about it
        status = campaign_status_on(start, end, day)
        # The calendar moves a campaign on: planned to active on its start
        # date, anything to completed after its end date. A paused campaign
        # stays paused while it could still run.
        if status != row["campaign_status"] and not (row["campaign_status"] == "paused" and status == "active"):
            changed = df.loc[[index]].copy()
            changed["campaign_status"] = status
            changed["updated_at"] = status_since(start, end, status, start)
            changes.append(changed)
        elif status == "active" and row["campaign_status"] == "active":
            running.append(int(row["campaign_id"]))

    running = [campaign_id for campaign_id in sorted(set(running)) if campaign_id not in NON_CHANGEABLE_IDS["campaigns"]]
    for campaign_id in sorted(rng.sample(running, min(CHANGE_COUNTS["campaigns"], len(running)))):
        changed = base_row(df, "campaign_id", campaign_id)
        if rng.random() < 0.5:
            # More budget is authorized: the new amount replaces the old one.
            changed["budget"] = round(float(changed["budget"].iloc[0]) * rng.choice([1.1, 1.25, 1.5]), 2)
        else:
            changed["campaign_status"] = "paused"
        changed["updated_at"] = stamp
        changes.append(changed)
    return changes


MASTER_CHANGES = {
    "customers": ("user_id", customer_changes),
    "products": ("product_id", product_changes),
    "campaigns": ("campaign_id", campaign_changes),
}


def apply_changes(df: pd.DataFrame, key: str, changes: list[pd.DataFrame]) -> pd.DataFrame:
    """The state after the changes: each changed record replaces its row, in place."""
    df = df.copy()
    for changed in changes:
        index = df.index[df[key] == changed[key].iloc[0]][0]
        for column in df.columns:
            df.at[index, column] = changed[column].iloc[0]
    return df


def earlier_batch_days() -> list[date]:
    """The days of the complete raw batches before this one, oldest first."""
    days = []
    for folder in RAW_DIR.parent.glob("batch_date=*"):
        try:
            day = date.fromisoformat(folder.name.split("=", 1)[1])
        except ValueError:
            continue
        if ANCHOR_DATE <= day < BATCH_DAY and raw_dir_is_complete(folder):
            days.append(day)
    return sorted(days)


def master_data(entity: str, base: pd.DataFrame) -> pd.DataFrame:
    """The entity's file for this batch: its current state, then today's new versions."""
    key, changes_on = MASTER_CHANGES[entity]
    state = base
    for day in earlier_batch_days():
        state = apply_changes(state, key, changes_on(state, day))
    return pd.concat([state, *changes_on(state, BATCH_DAY)], ignore_index=True)


def campaign_in_force(versions: list[dict], day: date) -> dict:
    """The version of a campaign known at the start of a day (its first one before that)."""
    known = [version for version in versions if version["updated_at"] <= f"{day.isoformat()}T00:00:00"]
    return (known or versions)[-1 if known else 0]


def campaign_runs_on(versions: list[dict], day: date) -> dict | None:
    """The campaign's version in force on a day it is running: inside its period and not paused."""
    campaign = campaign_in_force(versions, day)
    try:
        start, end = date.fromisoformat(campaign["start_date"]), date.fromisoformat(campaign["end_date"])
    except (TypeError, ValueError):
        return None
    if start <= day <= end and campaign.get("campaign_status") != "paused":
        return campaign
    return None


def campaign_versions(campaigns: pd.DataFrame) -> dict[int, list[dict]]:
    return {
        int(campaign_id): versions.sort_values("updated_at", kind="stable").to_dict("records")
        for campaign_id, versions in campaigns.dropna(subset=["campaign_id"]).groupby("campaign_id")
    }


# ---------------------------------------------------------------------------
# Facts: per-batch increments with batch-unique IDs
# ---------------------------------------------------------------------------

DIRTY_EVENT = {
    "event_id": None, "user_id": 999999, "user_name": None, "email": "bad-email", "gender": "Unknown", "age": -10,
    "membership_tier": "Diamond", "loyalty_points": -999, "preferred_language": None, "home_city": None,
    "home_state": None, "event_time": "bad-timestamp", "event_type": "unknown_event",
    "user_journey_stage": "broken_stage", "user_segment": "ghost_segment", "is_prime_user": None,
    "product_id": 999999, "product_name": None, "category": None, "quantity": -5, "original_price": -100,
    "discount_percent": 500, "discounted_price": -50, "cart_value": -999, "inventory_remaining": -1,
    "search_query": None, "time_on_page_sec": -1, "scroll_depth_percent": 999, "hover_duration_ms": -1,
    "session_duration_sec": -1, "items_viewed_in_session": -1, "repeat_product_view_count": -1,
    "time_since_last_event_ms": -1, "recommendation_rank": -1, "recommendation_clicked": None,
    "recommendation_algorithm": "unknown_algo", "click_position": -1, "engagement_score": 9.9,
    "purchase_probability": 9.9, "cart_abandonment_probability": 9.9, "ab_test_group": "Z",
    "payment_method": "gold_coin", "device_type": "smart_fridge", "operating_system": "TempleOS",
    "browser": "UnknownBrowser", "network_type": "carrier_pigeon", "app_version": "bad-version",
    "traffic_source": "unknown_channel", "campaign_id": 999999, "api_latency_ms": -100, "page_load_time_ms": -100,
    "fraud_score": 9.9, "country": "India", "city": None, "ip_address": "999.999.999.999",
    "schema_version": SCHEMA_VERSION, "source": SOURCE_SYSTEM, "event_timestamp": "bad-timestamp", "state": None,
}
STAGE_OF_EVENT = {
    "page_view": "awareness", "product_view": "consideration", "search": "consideration",
    "add_to_cart": "cart", "checkout": "checkout", "purchase": "purchase",
}


def purchasable(product: dict) -> bool:
    """A product that can go into a cart: on sale, in stock and with a valid price."""
    return (product["product_status"] == "active" and int(product["inventory_remaining"]) > 0
            and float(product["original_price"]) > 0 and float(product["discounted_price"]) > 0)


def search_query(rng: random.Random, product: dict) -> str:
    """What a customer types to find a product: its item name, its category or its brand."""
    brand = product["brand"] if isinstance(product["brand"], str) else ""
    name = product["product_name"]
    item = name[len(brand):].strip() if brand and name.startswith(brand) else name
    choices = [item, brand, f"{brand} {item}".strip()]
    if isinstance(product["category"], str):
        choices.append(product["category"])
    return rng.choice([choice for choice in choices if choice]).lower()


def pick_campaign(rng: random.Random, versions: dict[int, list[dict]], day: date, user: dict,
                  state: object) -> dict | None:
    """The campaign that brought a session, or None for organic traffic.

    Only a campaign running on that day can bring a visitor. One that targets
    the customer's segment or advertises in the session's state is likelier.
    """
    running = [campaign for campaign in (campaign_runs_on(v, day) for _, v in sorted(versions.items())) if campaign]
    if not running or rng.random() < ORGANIC_SHARE:
        return None
    weights = [
        (3 if campaign["target_segment"] == user["user_segment"] else 1)
        * (3 if state in {region for region, _ in campaign_regions(int(campaign["campaign_id"]))} else 1)
        for campaign in running
    ]
    return weighted(rng, running, weights)


def generate_web_events(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    campaigns: pd.DataFrame,
    rng: random.Random,
    uuid_rng: random.Random,
    window_days: int,
    count: int,
) -> tuple[list[dict], dict[str, list[dict]]]:
    """Web events, visit by visit, and the cart bought by each purchase event.

    A visit belongs to one customer on one device, from one place and one
    traffic source; more than 30 minutes of inactivity would end it, and so
    does a purchase. Event timestamps are action times in source Asia/Kolkata
    time. Counters describe only what the visit had seen by that event. The
    cart is the visit's own: add_to_cart puts a product in, checkout and
    purchase act on it (a direct checkout or purchase of one product starts
    the cart with it), and cart_value is the cart's value in INR after the
    event. Browsing events have no quantity and no cart value.
    """
    # The state before today's changes: today's events mostly precede them.
    users = customers.dropna(subset=["user_id"]).drop_duplicates("user_id").to_dict("records")
    catalogue_rows = products.dropna(subset=["product_id"]).drop_duplicates("product_id").to_dict("records")
    on_sale = [product for product in catalogue_rows if purchasable(product)]
    versions = campaign_versions(campaigns)

    def new_uuid() -> str:
        return str(uuid.UUID(int=uuid_rng.getrandbits(128), version=4))

    fake.seed_instance(f"{MASTER_SEED}|visits|{BATCH_DATE}")
    events: list[dict] = []
    carts: dict[str, list[dict]] = {}
    intervals: dict[int, list[tuple[datetime, datetime]]] = {}
    window = window_days if IS_ANCHOR_BATCH else window_days - 1

    while len(events) < count:
        user = rng.choice(users)
        length = min(rng.choice([1, 2, 2, 3, 3, 4, 5, 8]), count - len(events))
        gaps = [0] + [rng.randint(1, 900) for _ in range(length - 1)]
        for _ in range(100):
            start = random_datetime(rng, BATCH_DAY, window)
            end = start + timedelta(seconds=sum(gaps))
            if end.date() > BATCH_DAY:
                continue
            if all(start > old_end + timedelta(seconds=SESSION_TIMEOUT_SECONDS)
                   or end < old_start - timedelta(seconds=SESSION_TIMEOUT_SECONDS)
                   for old_start, old_end in intervals.get(user["user_id"], [])):
                break
        else:
            raise ValueError("Could not place a non-overlapping customer session")
        intervals.setdefault(user["user_id"], []).append((start, end))

        # Where, on what and how the visit happens: one answer for the whole visit.
        home_city = user["home_city"] if isinstance(user["home_city"], str) else None
        if home_city and rng.random() < HOME_CITY_SHARE:
            city, state = home_city, user["home_state"]
        elif home_city:
            city, state = pick_city(rng, exclude=home_city)
        else:
            city, state = None, None
        campaign = pick_campaign(rng, versions, start.date(), user, state)
        device = weighted(rng, DEVICE_TYPES, DEVICE_WEIGHTS)
        system = rng.choice(sorted(DEVICE_PROFILES[device]))
        session = {
            "session_id": new_uuid(),
            "campaign_id": int(campaign["campaign_id"]) if campaign else None,
            "traffic_source": campaign["traffic_source"] if campaign else "organic",
            "ab_test_group": campaign["ab_test_group"] if campaign else rng.choice(AB_TEST_GROUPS),
            "device_type": device,
            "operating_system": system,
            "browser": rng.choice(DEVICE_PROFILES[device][system]),
            "network_type": rng.choice(DEVICE_NETWORKS[device]),
            "app_version": f"{rng.randint(1, 5)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}",
            "ip_address": fake.ipv4_public(),
            "city": city,
            "state": state,
        }
        payment_method = weighted(rng, PAYMENT_METHODS, PAYMENT_WEIGHTS)

        cart: dict[int, dict] = {}
        viewed: dict[int, int] = {}
        last_viewed: dict | None = None
        elapsed = 0
        for position, gap in enumerate(gaps):
            elapsed += gap
            event_type = weighted(rng, EVENT_TYPES, EVENT_TYPE_WEIGHTS)
            quantity = 0
            if event_type == "add_to_cart":
                # Usually the product just looked at, if it can be bought.
                product = last_viewed if last_viewed and purchasable(last_viewed) and rng.random() < 0.6 \
                    else rng.choice(on_sale)
                quantity = weighted(rng, QUANTITIES, QUANTITY_WEIGHTS)
                line = cart.setdefault(int(product["product_id"]), {"product": product, "quantity": 0})
                line["quantity"] += quantity
            elif event_type in ("checkout", "purchase"):
                if not cart:
                    # Straight to checkout: the cart is this one product.
                    product = last_viewed if last_viewed and purchasable(last_viewed) else rng.choice(on_sale)
                    cart[int(product["product_id"])] = {"product": product,
                                                        "quantity": weighted(rng, QUANTITIES, QUANTITY_WEIGHTS)}
                # The event shows the line added last, and the whole cart's value.
                line = list(cart.values())[-1]
                product, quantity = line["product"], line["quantity"]
            else:
                product = rng.choice(catalogue_rows)
                if event_type == "product_view":
                    last_viewed = product
                    viewed[int(product["product_id"])] = viewed.get(int(product["product_id"]), 0) + 1
            cart_value = round(sum(line["quantity"] * float(line["product"]["discounted_price"])
                                   for line in cart.values()), 2) if event_type in CART_EVENT_TYPES else 0.0
            timestamp = (start + timedelta(seconds=elapsed)).isoformat()
            # The time on a page ends when the next event of the visit happens.
            time_on_page = rng.randint(1, 900)
            if position + 1 < len(gaps):
                time_on_page = min(time_on_page, gaps[position + 1])

            event = {
                "event_id": new_uuid(),
                "session_id": session["session_id"],
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
                "event_time": timestamp,
                "event_type": event_type,
                "user_journey_stage": STAGE_OF_EVENT[event_type],
                "user_segment": user["user_segment"],
                "is_prime_user": bool(user["is_prime_user"]),
                "product_id": int(product["product_id"]),
                "product_name": product["product_name"],
                "category": product["category"],
                "quantity": quantity,
                "original_price": float(product["original_price"]),
                "discount_percent": float(product["discount_percent"]),
                "discounted_price": float(product["discounted_price"]),
                "cart_value": cart_value,
                "inventory_remaining": int(product["inventory_remaining"]),
                "search_query": search_query(rng, product) if event_type == "search" else None,
                "time_on_page_sec": time_on_page,
                "scroll_depth_percent": rng.randint(0, 100),
                "hover_duration_ms": rng.randint(0, min(30000, time_on_page * 1000)),
                "session_duration_sec": elapsed,
                "items_viewed_in_session": len(viewed),
                "repeat_product_view_count": max(viewed.get(int(product["product_id"]), 0) - 1, 0),
                "time_since_last_event_ms": gap * 1000,
                "recommendation_rank": rng.randint(1, 20),
                "recommendation_clicked": rng.choice([True, False]),
                "recommendation_algorithm": rng.choice(RECOMMENDATION_ALGORITHMS),
                "click_position": rng.randint(1, 50),
                "engagement_score": round(rng.uniform(0, 1), 4),
                "purchase_probability": round(rng.uniform(0, 1), 4),
                "cart_abandonment_probability": round(rng.uniform(0, 1), 4),
                "ab_test_group": session["ab_test_group"],
                # Only a checkout or a purchase has a payment method.
                "payment_method": payment_method if event_type in ("checkout", "purchase") else None,
                "device_type": session["device_type"],
                "operating_system": session["operating_system"],
                "browser": session["browser"],
                "network_type": session["network_type"],
                "app_version": session["app_version"],
                "traffic_source": session["traffic_source"],
                "campaign_id": session["campaign_id"],
                "api_latency_ms": rng.randint(20, 5000),
                "page_load_time_ms": rng.randint(100, 10000),
                "fraud_score": round(rng.uniform(0, 1), 4),
                "country": "India",
                "city": session["city"],
                "ip_address": session["ip_address"],
                "schema_version": SCHEMA_VERSION,
                "source": SOURCE_SYSTEM,
                "event_timestamp": timestamp,
                "state": session["state"],
            }
            events.append(event)
            if event_type == "purchase":
                carts[event["event_id"]] = [
                    {"product": line["product"], "quantity": line["quantity"]} for line in cart.values()]
                break   # a purchase ends the visit

    events.sort(key=lambda event: (event["event_timestamp"], event["event_id"]))

    # Dirty event for quarantine testing
    events.append({**DIRTY_EVENT, "session_id": new_uuid()})
    return events, carts


def generate_orders(web_events: list[dict], rng: random.Random, count: int) -> tuple[pd.DataFrame, list[str]]:
    """One order per purchase event, without amounts; and each order's purchase event.

    count is a ceiling, not a reason to fabricate transactions. The explicit
    duplicate-ID quarantine fixture is applied after sampling.
    """
    purchase_events = [event for event in web_events if event.get("event_type") == "purchase"]
    selected = purchase_events if len(purchase_events) <= count else sorted(
        rng.sample(purchase_events, count), key=lambda event: (event["event_timestamp"], event["event_id"]))
    if len(selected) < 12:
        raise ValueError("At least twelve purchase events are needed for order fixtures")

    rows = [
        {
            "order_id": fact_id(n),
            "user_id": event["user_id"],
            "campaign_id": event["campaign_id"],
            "order_timestamp": event["event_timestamp"],
            # Paid the way the purchase event says.
            "payment_method": event["payment_method"],
            "cart_value": None,
            "fraud_score": event["fraud_score"],
            "country": event["country"],
            "city": event["city"],
            "state": event["state"],
            "schema_version": SCHEMA_VERSION,
            "source": "orders_from_purchase_events",
        }
        for n, event in enumerate(selected, 1)
    ]
    df = pd.DataFrame(rows)

    # Dirty record: a duplicate order ID (both rows get that order's total).
    df.loc[0, "order_id"] = df.loc[1, "order_id"]

    return df, [event["event_id"] for event in selected]


def generate_order_items(orders: pd.DataFrame, purchases: list[str], carts: dict[str, list[dict]]) -> pd.DataFrame:
    """An order's items are the lines of the cart its purchase event bought."""
    rows = []
    for position, (order_id, event_id) in enumerate(zip(orders["order_id"], purchases)):
        if position == 0:
            continue   # the duplicate-ID fixture: its own order was never delivered
        for line in carts[event_id]:
            product = line["product"]
            discounted_price = float(product["discounted_price"])
            rows.append(
                {
                    "order_item_id": fact_id(len(rows) + 1),
                    "order_id": int(order_id),
                    "product_id": int(product["product_id"]),
                    "product_name": product["product_name"],
                    "category": product["category"],
                    "quantity": line["quantity"],
                    "original_price": float(product["original_price"]),
                    "discount_percent": float(product["discount_percent"]),
                    "discounted_price": discounted_price,
                    "line_amount": round(line["quantity"] * discounted_price, 2),
                }
            )
    return pd.DataFrame(rows)


def order_charges(net: float) -> tuple[float, float, float]:
    """(tax_amount, shipping_amount, total_amount) of an order with this net value."""
    # Exact decimal arithmetic, rounded half up to the cent, so the amounts are
    # the same in pandas, Spark and Postgres.
    cents = Decimal("0.01")
    net_value = Decimal(str(net))
    tax = (net_value * Decimal(str(TAX_RATE))).quantize(cents, rounding=ROUND_HALF_UP)
    shipping = Decimal("0.00") if net >= FREE_SHIPPING_THRESHOLD else Decimal(str(SHIPPING_FEE)).quantize(cents)
    return float(tax), float(shipping), float((net_value + tax + shipping).quantize(cents, rounding=ROUND_HALF_UP))


def order_lifecycle(order_id: int, order_timestamp: str, net: float) -> tuple[str, datetime, float]:
    """(final status, when it took effect, refund amount) of an order, from its own streams."""
    placed = datetime.fromisoformat(order_timestamp)
    status = random.Random(f"order_status|{order_id}").choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)[0]
    rng = random.Random(f"order_lifecycle|{order_id}")
    if status == "cancelled":
        return status, placed + timedelta(seconds=rng.randint(*CANCEL_AFTER_SECONDS)), 0.0
    if status == "returned":
        returned_at = placed + timedelta(seconds=rng.randint(*RETURN_AFTER_SECONDS))
        share = 1.0 if rng.random() < FULL_REFUND_SHARE else rng.uniform(*PARTIAL_REFUND)
        refund = (Decimal(str(net)) * Decimal(str(round(share, 4)))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return status, returned_at, float(min(refund, Decimal(str(net))))
    return status, placed, 0.0


def order_state(order_id: int, order_timestamp: str, net: float, as_of: date) -> tuple[str, str, float]:
    """(order_status, status_timestamp, refund_amount) as the source knows them at the end of as_of."""
    status, effective, refund = order_lifecycle(order_id, order_timestamp, net)
    if effective.date() > as_of:
        # Not cancelled or returned yet: the order is still a completed sale.
        return "completed", order_timestamp, 0.0
    return status, effective.isoformat(), refund


def redelivered_orders() -> pd.DataFrame | None:
    """Orders of earlier batches whose cancellation or return falls in this batch.

    The earlier batch delivered them as completed. A real order system sends
    the order again when its status changes: the same row with its new
    order_status, status_timestamp, refund_amount and updated_at. An order is
    re-delivered once, by the first batch on or after the day of the change,
    so the earlier raw batches on disk decide which batch that is.
    """
    earlier = sorted(
        (date.fromisoformat(folder.name.split("=", 1)[1]), folder)
        for folder in RAW_DIR.parent.glob("batch_date=*")
        if folder.name.split("=", 1)[1] < BATCH_DATE and raw_dir_is_complete(folder)
    )
    updates = []
    for position, (batch_day, folder) in enumerate(earlier):
        later_batches = [day for day, _ in earlier[position + 1:]]
        orders = pd.read_csv(folder / "orders.csv")
        if "status_timestamp" not in orders.columns:
            continue   # a batch written before orders had a lifecycle
        block_start = (batch_day - ANCHOR_DATE).days * ID_BLOCK
        # The batch's own orders: not its dirty rows (0, 2, 3, 4 and the appended
        # fixtures), and not orders that it re-delivered itself.
        own = orders[(orders.index > 4) & orders["order_id"].between(block_start + 1, block_start + FIXTURE_ID_START - 1)]
        for row in own.to_dict("records"):
            status, effective, refund = order_lifecycle(int(row["order_id"]), row["order_timestamp"], float(row["cart_value"]))
            changed_on = effective.date()
            if status == "completed" or changed_on <= batch_day or changed_on > BATCH_DAY:
                continue
            if any(changed_on <= day for day in later_batches):
                continue   # an earlier batch than this one has already re-delivered it
            updates.append({**row, "order_status": status, "status_timestamp": effective.isoformat(),
                            "refund_amount": refund, "updated_at": effective.isoformat()})
    return pd.DataFrame(updates) if updates else None


def add_order_status_and_charges(orders: pd.DataFrame) -> pd.DataFrame:
    """order_status, tax_amount, shipping_amount and total_amount, inserted after cart_value.

    cart_value stays the net value (the sum of the lines); total_amount is what
    the customer was charged: net + tax + shipping. It has that meaning in
    every layer (Silver, Gold, the warehouse, the marts and the exports). The status comes from its
    own stream per order ID, so a duplicated order ID has one status.
    """
    orders = orders.copy()
    charges = [order_charges(float(net)) for net in orders["cart_value"]]
    position = orders.columns.get_loc("cart_value") + 1
    orders.insert(position, "tax_amount", [tax for tax, _, _ in charges])
    orders.insert(position + 1, "shipping_amount", [shipping for _, shipping, _ in charges])
    orders.insert(position + 2, "total_amount", [total for _, _, total in charges])
    # What the source knows at the end of this batch's day (see the lifecycle note above).
    known = [order_state(int(order_id), timestamp, float(net), BATCH_DAY)
             for order_id, timestamp, net in zip(orders["order_id"], orders["order_timestamp"], orders["cart_value"])]
    orders.insert(orders.columns.get_loc("payment_method"), "order_status", [state[0] for state in known])
    position = orders.columns.get_loc("order_status") + 1
    orders.insert(position, "status_timestamp", [state[1] for state in known])
    orders.insert(position + 1, "refund_amount", [state[2] for state in known])
    orders["updated_at"] = orders["status_timestamp"]
    return orders


def generate_orders_and_items(
    web_events: list[dict],
    carts: dict[str, list[dict]],
    rng: random.Random,
    order_count: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Orders and their items: each order is the cart of its purchase event.

    1. one order per purchase event, with that event's customer, time, place,
       campaign and payment method
    2. its items are the cart's lines, so cart_value (the sum of the lines) is
       the cart value the purchase event shows
    3. the dirty records last, so they stay dirty
    """
    orders, purchases = generate_orders(web_events, rng, order_count)
    items = generate_order_items(orders, purchases, carts)

    totals = items.groupby("order_id")["line_amount"].sum().round(2)
    orders["cart_value"] = orders["order_id"].map(totals).astype(float)
    orders = add_order_status_and_charges(orders)

    # Dirty order records
    orders.loc[2, "cart_value"] = -500
    orders.loc[3, "user_id"] = 999999
    orders.loc[4, "order_timestamp"] = "not-a-date"

    # Orders of earlier batches that were cancelled or returned since: delivered again.
    updates = redelivered_orders()
    if updates is not None:
        orders = pd.concat([orders, updates[orders.columns]], ignore_index=True)

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
    for campaign_id, versions in sorted(campaign_versions(campaigns).items()):
        for ago in range(days):
            spend_day = BATCH_DAY - timedelta(days=ago)
            # The budget known at the start of the day; no spend on a day the
            # campaign is not running (outside its period, or paused).
            campaign = campaign_runs_on(versions, spend_day)
            if campaign is None:
                continue
            start, end = date.fromisoformat(campaign["start_date"]), date.fromisoformat(campaign["end_date"])
            candidates.append((campaign_id, spend_day, campaign, (end-start).days+1))
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
    # Impossible lifecycles: a return before the order, a refund above the
    # order's net value, and a refund on an order that was not returned.
    orders = outputs["orders.csv"]
    add("orders.csv", 9, "return_before_order", order_id=fact_id(ID_BLOCK - 5), order_status="returned",
        status_timestamp=(datetime.fromisoformat(orders.loc[9, "order_timestamp"]) - timedelta(days=1)).isoformat(),
        refund_amount=float(orders.loc[9, "cart_value"]))
    add("orders.csv", 10, "refund_above_net", order_id=fact_id(ID_BLOCK - 6), order_status="returned",
        status_timestamp=orders.loc[10, "order_timestamp"], refund_amount=round(float(orders.loc[10, "cart_value"]) + 1, 2))
    add("orders.csv", 11, "refund_on_non_returned_order", order_id=fact_id(ID_BLOCK - 7), order_status="completed",
        status_timestamp=orders.loc[11, "order_timestamp"], refund_amount=10.0)
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
    customers = master_data("customers", generate_customers())
    products = master_data("products", generate_products())
    campaigns = master_data("campaigns", generate_campaigns())

    window_days = ANCHOR_WINDOW_DAYS if IS_ANCHOR_BATCH else FACT_WINDOW_DAYS
    counts = ANCHOR_COUNTS if IS_ANCHOR_BATCH else INCREMENT_COUNTS
    validate_counts(counts)

    fact_rng = stream("facts", BATCH_DATE)
    web_events, carts = generate_web_events(
        customers, products, campaigns, fact_rng, stream("uuid", BATCH_DATE), window_days, counts["web_events"]
    )
    orders, order_items = generate_orders_and_items(web_events, carts, fact_rng, counts["orders"])
    ad_spend = generate_ad_spend(campaigns, stream("spend", BATCH_DATE), window_days, counts["ad_spend"])
    truth = [{"kind": "contract", "generator_version": GENERATOR_VERSION, "currency": CURRENCY,
              "monetary_fields": "every amount and price in every file is in Indian rupees (INR)",
              "budget_grain": "campaign lifetime authorization, latest known revision replaces prior cap",
              "budget_period": "start_date through end_date inclusive, Asia/Kolkata",
              "spend_grain": "observed campaign-day-region; every region of a sampled campaign-day is present; "
                             "sampled days are not a complete billing ledger",
              "session_timeout_seconds": SESSION_TIMEOUT_SECONDS,
              "session_counters": "observed prefix, not final duration or future events",
              "master_data": "current state after the changes of every earlier batch on disk, then this batch's new versions",
              "labels_are_model_inputs": False}]
    for row_index, row in ad_spend.iterrows():
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


def batch_generator_version(folder: Path) -> str:
    """The generator version a raw batch records in its truth file ('unrecorded' if it has none)."""
    truth = folder / "generation_truth.json"
    try:
        with truth.open(encoding="utf-8") as file:
            return str(json.loads(file.readline()).get("generator_version") or "unrecorded")
    except (OSError, ValueError, AttributeError):
        return "unrecorded"


def earlier_batches_of_another_version() -> list[tuple[str, str]]:
    """(batch date, its generator version) of every earlier raw batch this version did not write."""
    found = []
    for day in earlier_batch_days():
        version = batch_generator_version(RAW_DIR.parent / f"batch_date={day.isoformat()}")
        if version != GENERATOR_VERSION:
            found.append((day.isoformat(), version))
    return found


def newer_raw_batches() -> list[str]:
    """The raw batch folders dated after this batch, newest last."""
    newer = []
    for folder in RAW_DIR.parent.glob("batch_date=*"):
        name = folder.name.split("=", 1)[1]
        try:
            day = date.fromisoformat(name)
        except ValueError:
            continue
        if day > BATCH_DAY:
            newer.append(name)
    return sorted(newer)


def main() -> None:
    if BATCH_INDEX < 0:
        raise SystemExit(f"BATCH_DATE={BATCH_DATE} is before ANCHOR_DATE={ANCHOR_DATE.isoformat()}.")

    if raw_dir_is_complete(RAW_DIR):
        print(f"Raw data for {RAW_DIR.name} is already complete. Raw batches are immutable; nothing written.")
        return

    # A batch continues from the batches before it: the master data carries
    # their changes, and an order they delivered is delivered again when its
    # status changes. A batch written behind a newer one would contradict what
    # the newer one already says, so it is refused. Reprocessing existing raw
    # batches, in any order, never comes here: they are complete.
    other_versions = earlier_batches_of_another_version()
    if other_versions:
        listed = ", ".join(f"{name} ({version})" for name, version in other_versions)
        raise SystemExit(
            f"Cannot generate BATCH_DATE={BATCH_DATE} with generator version {GENERATOR_VERSION}: earlier raw batches "
            f"were written by a different version: {listed}. A batch continues from the batches before it, so all "
            f"raw batches must come from one generator version. Regenerate the raw data from the first batch "
            f"({ANCHOR_DATE.isoformat()}) in date order, or remove those batches."
        )

    newer = newer_raw_batches()
    if newer:
        raise SystemExit(
            f"Cannot generate BATCH_DATE={BATCH_DATE}: a newer raw batch already exists ({newer[-1]}). "
            f"Raw batches are generated in date order. Generate a date after {newer[-1]}, or remove the raw "
            f"batches after {BATCH_DATE} first."
        )

    RAW_DIR.parent.mkdir(parents=True, exist_ok=True)
    write_batch(RAW_DIR, build_batch())

    print(f"Raw data generated at: {RAW_DIR}")
    print("Files created:")
    for file_path in sorted(RAW_DIR.iterdir()):
        print(f"- {file_path.name}")


if __name__ == "__main__":
    main()
