"""Part K-A (p2-data-v2): the source is coherent.

Source fields used to be drawn independently and contradicted each other:
Nagpur in Telangana, "Instagram Campaign 6" on the organic channel, a Laptop
Stand in Books, a Bronze member with 48,598 points, an order whose items had
nothing to do with the cart that was bought. Each rule below is checked on
three generated batches; RULES is the complete list, and the last test scans
a batch for all of them at once.

The reference tables in this file (city to state, state to language, tier
thresholds, segment ages, device profiles, channel labels) are written out
here on purpose: they are what the generator is checked against.
"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
from datetime import date

import pandas as pd
import pytest

from conftest import CODE_ROOT

GENERATOR = "data-generator/generate_data.py"
BATCHES = ("2026-05-18", "2026-06-18", "2026-07-02")

CITY_STATE = {
    "Mumbai": "Maharashtra", "Pune": "Maharashtra", "Nagpur": "Maharashtra", "Nashik": "Maharashtra",
    "Thane": "Maharashtra", "Bengaluru": "Karnataka", "Mysuru": "Karnataka", "Mangaluru": "Karnataka",
    "Hubballi": "Karnataka", "Chennai": "Tamil Nadu", "Coimbatore": "Tamil Nadu", "Madurai": "Tamil Nadu",
    "Tiruchirappalli": "Tamil Nadu", "Hyderabad": "Telangana", "Warangal": "Telangana",
    "Visakhapatnam": "Andhra Pradesh", "Vijayawada": "Andhra Pradesh", "Guntur": "Andhra Pradesh",
    "Kolkata": "West Bengal", "Howrah": "West Bengal", "Siliguri": "West Bengal", "Ahmedabad": "Gujarat",
    "Surat": "Gujarat", "Vadodara": "Gujarat", "Rajkot": "Gujarat", "Jaipur": "Rajasthan", "Jodhpur": "Rajasthan",
    "Udaipur": "Rajasthan", "Kota": "Rajasthan", "Lucknow": "Uttar Pradesh", "Kanpur": "Uttar Pradesh",
    "Varanasi": "Uttar Pradesh", "Agra": "Uttar Pradesh", "Noida": "Uttar Pradesh", "Bhopal": "Madhya Pradesh",
    "Indore": "Madhya Pradesh", "Gwalior": "Madhya Pradesh", "Patna": "Bihar", "Gaya": "Bihar", "Kochi": "Kerala",
    "Thiruvananthapuram": "Kerala", "Kozhikode": "Kerala", "Ludhiana": "Punjab", "Amritsar": "Punjab",
    "Jalandhar": "Punjab", "Gurugram": "Haryana", "Faridabad": "Haryana", "New Delhi": "Delhi",
    "Bhubaneswar": "Odisha", "Cuttack": "Odisha", "Guwahati": "Assam", "Ranchi": "Jharkhand",
    "Jamshedpur": "Jharkhand", "Raipur": "Chhattisgarh", "Dehradun": "Uttarakhand", "Panaji": "Goa",
}
STATE_LANGUAGE = {
    "Maharashtra": "Marathi", "Karnataka": "Kannada", "Tamil Nadu": "Tamil", "Telangana": "Telugu",
    "Andhra Pradesh": "Telugu", "West Bengal": "Bengali", "Gujarat": "Gujarati", "Kerala": "Malayalam",
    "Punjab": "Punjabi", "Odisha": "Odia", "Assam": "Assamese", "Goa": "Konkani", "Rajasthan": "Hindi",
    "Uttar Pradesh": "Hindi", "Madhya Pradesh": "Hindi", "Bihar": "Hindi", "Haryana": "Hindi", "Delhi": "Hindi",
    "Jharkhand": "Hindi", "Chhattisgarh": "Hindi", "Uttarakhand": "Hindi",
}
TIER_FROM = {"Bronze": 0, "Silver": 5000, "Gold": 15000, "Platinum": 30000}
SEGMENT_AGES = {"Student": (18, 26), "Working Professional": (22, 60), "Small Business": (25, 65), "Enterprise": (30, 65)}
CHANNEL_LABELS = {"google": "Google Search", "facebook": "Facebook Feed", "instagram": "Instagram Reels",
                  "email": "Email Newsletter", "affiliate": "Affiliate Partners"}
DEVICES = {
    "mobile": ({"Android": {"Chrome", "Firefox"}, "iOS": {"Safari", "Chrome"}}, {"4G", "5G", "WiFi"}),
    "tablet": ({"Android": {"Chrome"}, "iOS": {"Safari", "Chrome"}}, {"WiFi", "4G", "5G"}),
    "desktop": ({"Windows": {"Chrome", "Edge", "Firefox"}, "macOS": {"Safari", "Chrome", "Firefox"},
                 "Ubuntu": {"Chrome", "Firefox"}}, {"WiFi", "Broadband"}),
}
BROWSING, CART_EVENTS = {"page_view", "product_view", "search"}, {"add_to_cart", "checkout", "purchase"}


def tier_of(points: int) -> str:
    return max((tier for tier, start in TIER_FROM.items() if points >= start), key=TIER_FROM.get)


@pytest.fixture(scope="module")
def source(tmp_path_factory):
    """Three batches generated in date order into one raw folder: {batch: Batch}, and the generator module."""
    root = tmp_path_factory.mktemp("coherent")
    target = root / GENERATOR
    target.parent.mkdir(parents=True)
    shutil.copy(CODE_ROOT / GENERATOR, target)
    saved = os.environ.get("BATCH_DATE")
    batches, module = {}, None
    try:
        for batch in BATCHES:
            os.environ["BATCH_DATE"] = batch
            spec = importlib.util.spec_from_file_location(f"p2_coherent_generator_{batch.replace('-', '')}", target)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.main()
            batches[batch] = Batch(root / "data" / "raw" / f"batch_date={batch}", batch)
    finally:
        if saved is None:
            os.environ.pop("BATCH_DATE", None)
        else:
            os.environ["BATCH_DATE"] = saved
    return batches, module


class Batch:
    """One raw batch without its deliberately dirty rows (the truth file lists them)."""

    def __init__(self, folder, batch: str):
        self.day = date.fromisoformat(batch)
        truth = [json.loads(line) for line in (folder / "generation_truth.json").read_text(encoding="utf-8").splitlines()]
        dirty = {}
        for row in truth:
            if row["kind"] == "quality_fixture":
                dirty.setdefault(row["table"], set()).add(row["row_index"])
        self.raw = {name: pd.read_csv(folder / f"{name}.csv") for name in
                    ("customers", "products", "campaigns", "orders", "order_items", "ad_spend")}
        events = [json.loads(line) for line in (folder / "web_events.json").read_text(encoding="utf-8").splitlines()]
        self.raw["web_events"] = pd.DataFrame(events)
        clean = {name: frame.drop(index=sorted(dirty.get(f"{name}.csv" if name != "web_events" else "web_events.json", ())))
                 for name, frame in self.raw.items()}
        # A dirty master record stays dirty in every version of it.
        for name, key in (("customers", "user_id"), ("products", "product_id"), ("campaigns", "campaign_id")):
            dirty_keys = set(self.raw[name].loc[sorted(dirty[f"{name}.csv"]), key].dropna())
            clean[name] = clean[name][~clean[name][key].isin(dirty_keys)]
        self.customers, self.products, self.campaigns = clean["customers"], clean["products"], clean["campaigns"]
        self.events, self.spend = clean["web_events"], clean["ad_spend"]
        # The duplicate-ID order row is dirty, and so are the orders of the dirty item rows' and the dirty customers.
        orders = clean["orders"]
        self.own_orders = orders[orders["order_id"] // 100_000 == (self.day - date(2026, 5, 18)).days]
        self.items = clean["order_items"]
        self.dirty_item_orders = set(self.raw["order_items"].loc[sorted(dirty["order_items.csv"]), "order_id"])


def customer_rules(batch: Batch, first_state: dict) -> dict[str, list]:
    c = batch.customers
    # A customer who moved keeps the language of where they come from.
    origin = c["user_id"].map(first_state)
    # Names are Faker's Indian ones: a man's first name is on its list of male names, a woman's on the female one.
    from faker.providers.person.en_IN import Provider as IndianNames

    first = c["user_name"].str.split(" ", n=1).str[0]
    last = c["user_name"].str.split(" ", n=1).str[1]
    local = (first.str.lower().str.replace("[^a-z]", "", regex=True) + "."
             + last.str.lower().str.replace("[^a-z]", "", regex=True) + c["user_id"].astype(str))
    wrong_gender = c[((c["gender"] == "Male") & ~first.isin(IndianNames.first_names_male))
                     | ((c["gender"] == "Female") & ~first.isin(IndianNames.first_names_female))
                     | ~last.isin(IndianNames.last_names)]
    prime = c.groupby("membership_tier")["is_prime_user"].mean()
    share = c["is_prime_user"].mean()
    age_range = c["user_segment"].map(SEGMENT_AGES)
    return {
        "customer city is in its state": c[c["home_state"] != c["home_city"].map(CITY_STATE)]["user_id"].tolist(),
        "customer language is plausible for the state": c[[
            language not in (STATE_LANGUAGE.get(state), "Hindi", "English")
            for language, state in zip(c["preferred_language"], origin)]]["user_id"].tolist(),
        "customer name agrees with gender": wrong_gender["user_name"].tolist(),
        "customer signed up before anything else happened": c[
            (c["signup_date"] >= c["updated_at"].str[:10]) | (c["signup_date"] < "2022-01-01")]["user_id"].tolist(),
        "customer email is derived from the name": c[
            (c["email"].str.split("@").str[0] != local)
            | ~c["email"].str.split("@").str[1].isin(["example.com", "example.org", "example.net"])]["user_id"].tolist(),
        "membership tier follows loyalty points": c[c["membership_tier"] != c["loyalty_points"].map(tier_of)]["user_id"].tolist(),
        "user segment is plausible for age": c[[not low <= age <= high for age, (low, high) in zip(c["age"], age_range)]]["user_id"].tolist(),
        "prime membership has a realistic share": [] if 0.10 < share < 0.40 and prime["Platinum"] > prime["Bronze"]
        else [f"share {share:.2f}, by tier {prime.round(2).to_dict()}"],
    }


def product_rules(batch: Batch, module) -> dict[str, list]:
    p = batch.products
    catalogue = getattr(module, "CATALOGUE", {})
    brands = getattr(module, "PRODUCT_BRANDS", {})
    items = {item: (category, low, high) for category, rows in catalogue.items() for item, low, high in rows}
    wrong, out_of_range = [], []
    for row in p.to_dict("records"):
        brand = row["brand"] if isinstance(row["brand"], str) else ""
        item = row["product_name"][len(brand):].strip()
        category, low, high = items.get(item, (None, 0, 0))
        if not row["product_name"].startswith(brand + " ") or category != row["category"] \
                or brand not in brands.get(row["category"], []):
            wrong.append((row["product_id"], row["product_name"], row["category"], row["brand"]))
        if not low <= row["original_price"] <= high \
                or round(row["original_price"] * (1 - row["discount_percent"] / 100), 2) != row["discounted_price"]:
            out_of_range.append((row["product_id"], row["product_name"], row["original_price"]))
    names = p.drop_duplicates("product_id")["product_name"]
    return {
        "product name, category and brand agree": wrong,
        "the catalogue has distinct products": sorted(names[names.duplicated()].unique()),
        "product price is in its item's INR range": out_of_range,
    }


def campaign_rules(batch: Batch) -> dict[str, list]:
    k, spend, day = batch.campaigns, batch.spend, batch.day.isoformat()
    latest = k.sort_values("updated_at", kind="stable").drop_duplicates("campaign_id", keep="last")
    run_days = (pd.to_datetime(latest["end_date"]) - pd.to_datetime(latest["start_date"])).dt.days + 1
    daily = latest["budget"] / run_days
    calendar = pd.Series("active", index=latest.index).mask(latest["start_date"] > day, "planned").mask(latest["end_date"] < day, "completed")
    status_ok = (latest["campaign_status"] == calendar) | ((latest["campaign_status"] == "paused") & (calendar == "active"))
    periods = latest.set_index("campaign_id")
    known = spend[spend["campaign_id"].isin(periods.index)]
    outside = known[(known["spend_date"] < known["campaign_id"].map(periods["start_date"]))
                    | (known["spend_date"] > known["campaign_id"].map(periods["end_date"]))]
    paused = k[k["campaign_status"] == "paused"].set_index("campaign_id")["updated_at"].str[:10]
    after_pause = known[known["spend_date"] > known["campaign_id"].map(paused).fillna("9999")]
    regions = spend.groupby("campaign_id")["state"].nunique()
    return {
        "campaign name agrees with its channel": k[[
            not str(name).startswith(CHANNEL_LABELS.get(channel, "?") + " ")
            for name, channel in zip(k["campaign_name"], k["traffic_source"])]]["campaign_name"].tolist(),
        "organic is not a paid campaign": k[k["traffic_source"] == "organic"]["campaign_name"].tolist()
        + spend[spend["traffic_source"] == "organic"]["spend_id"].tolist(),
        "campaign status agrees with its dates": latest[~status_ok][["campaign_id", "campaign_status", "start_date", "end_date"]].values.tolist(),
        "campaign budget agrees with its period": latest[(run_days < 15) | (run_days > 90) | (daily < 500) | (daily > 2000 * 1.5 ** 3)]["campaign_id"].tolist(),
        "ad spend only while the campaign runs": outside["spend_id"].tolist() + after_pause["spend_id"].tolist(),
        "a campaign advertises in the same few states": regions[(regions < 2) | (regions > 5)].index.tolist(),
    }


def event_rules(batch: Batch) -> dict[str, list]:
    e = batch.events
    sessions = e.groupby("session_id")
    context = ["user_id", "device_type", "operating_system", "browser", "network_type", "app_version", "ip_address",
               "city", "state", "campaign_id", "traffic_source", "ab_test_group"]
    mixed = [column for column in context if sessions[column].nunique(dropna=False).max() != 1]
    bad_device = e[[
        system not in DEVICES[device][0] or browser not in DEVICES[device][0][system] or network not in DEVICES[device][1]
        for device, system, browser, network in zip(e["device_type"], e["operating_system"], e["browser"], e["network_type"])]]
    campaigns = batch.campaigns.drop_duplicates("campaign_id").set_index("campaign_id")
    with_campaign = e[e["campaign_id"].notna()]
    period = with_campaign["campaign_id"].map(campaigns["start_date"]), with_campaign["campaign_id"].map(campaigns["end_date"])
    day = with_campaign["event_timestamp"].str[:10]
    # The product as the batch knew it before its own changes: the first row per product.
    products = batch.raw["products"].drop_duplicates("product_id").set_index("product_id")
    price_differs = e[(e["original_price"] != e["product_id"].map(products["original_price"]))
                      | (e["discounted_price"] != e["product_id"].map(products["discounted_price"]))
                      | (e["product_name"] != e["product_id"].map(products["product_name"]))]
    in_cart = e[e["event_type"].isin(CART_EVENTS)]
    unsellable = in_cart[(in_cart["product_id"].map(products["product_status"]) != "active")
                         | (in_cart["product_id"].map(products["inventory_remaining"]) <= 0)]
    home = e[e["home_city"].notna()]
    return {
        "a session has one device, place and traffic source": mixed,
        "device, operating system, browser and network agree": bad_device[["device_type", "operating_system", "browser", "network_type"]].drop_duplicates().values.tolist(),
        "organic traffic has no campaign": e[e["campaign_id"].isna() != (e["traffic_source"] == "organic")]["event_id"].tolist()
        + with_campaign[with_campaign["campaign_id"].isin(campaigns.index)
                        & (with_campaign["traffic_source"] != with_campaign["campaign_id"].map(campaigns["traffic_source"]))]["event_id"].tolist(),
        "a session comes from a campaign that is running": with_campaign[(day < period[0]) | (day > period[1])]["event_id"].tolist(),
        "an event shows the product's own name and price": price_differs["event_id"].tolist(),
        "only a product on sale and in stock is put in a cart": unsellable["event_id"].tolist(),
        "quantity and cart value only on cart events": e[(e["event_type"].isin(BROWSING) != (e["quantity"] == 0))
                                                         | (e["event_type"].isin(BROWSING) & (e["cart_value"] != 0))
                                                         | (e["event_type"].isin(CART_EVENTS) & (e["cart_value"] <= 0))]["event_id"].tolist(),
        "payment method only at checkout and purchase": e[e["payment_method"].notna() != e["event_type"].isin(["checkout", "purchase"])]["event_id"].tolist()
        + [session for session, methods in sessions["payment_method"].nunique().items() if methods > 1],
        "search query only on a search": e[e["search_query"].notna() != (e["event_type"] == "search")]["event_id"].tolist(),
        "an event's city is in its state": home[home["state"] != home["city"].map(CITY_STATE)]["event_id"].tolist()
        + ([] if (home["city"] == home["home_city"]).mean() > 0.7 else ["most sessions should come from the home city"]),
    }


def order_rules(batch: Batch) -> dict[str, list]:
    o = batch.own_orders
    o = o[~o["order_id"].isin(batch.dirty_item_orders)]
    purchases = batch.events[batch.events["event_type"] == "purchase"]
    joined = o.merge(purchases, left_on=["user_id", "order_timestamp"], right_on=["user_id", "event_timestamp"],
                     how="left", suffixes=("", "_event"))
    lines = batch.items.groupby("order_id")["line_amount"].sum().round(2)
    differs = joined[joined["event_id"].isna()
                     | ((joined["cart_value"] - joined["cart_value_event"]).abs() > 0.005)
                     | ((joined["cart_value"] - joined["order_id"].map(lines)).abs() > 0.005)
                     | (joined["payment_method"] != joined["payment_method_event"])
                     | (joined["city"].fillna("") != joined["city_event"].fillna(""))
                     | (joined["state"].fillna("") != joined["state_event"].fillna(""))
                     | (joined["campaign_id"].fillna(-1) != joined["campaign_id_event"].fillna(-1))]
    products = batch.raw["products"].drop_duplicates("product_id").set_index("product_id")
    items = batch.items[batch.items["order_id"].isin(o["order_id"])]
    located = o[o["city"].notna()]
    return {
        "an order is the cart of its purchase event": differs["order_id"].tolist(),
        "an order item shows the product's own name and price": items[
            (items["discounted_price"] != items["product_id"].map(products["discounted_price"]))
            | (items["product_name"] != items["product_id"].map(products["product_name"]))]["order_item_id"].tolist(),
        "an order's city is in its state": located[located["state"] != located["city"].map(CITY_STATE)]["order_id"].tolist(),
        "an order has a payment method and a refund only when returned": o[
            o["payment_method"].isna() | ((o["order_status"] == "returned") != (o["refund_amount"] > 0))]["order_id"].tolist(),
    }


def incoherences(batch: Batch, module, first_state: dict) -> dict[str, list]:
    """Every rule and the rows that break it."""
    return {**customer_rules(batch, first_state), **product_rules(batch, module), **campaign_rules(batch), **event_rules(batch),
            **order_rules(batch)}


RULES = [
    "customer city is in its state", "customer language is plausible for the state", "customer name agrees with gender",
    "customer email is derived from the name", "customer signed up before anything else happened",
    "membership tier follows loyalty points",
    "user segment is plausible for age", "prime membership has a realistic share",
    "product name, category and brand agree", "the catalogue has distinct products",
    "product price is in its item's INR range",
    "campaign name agrees with its channel", "organic is not a paid campaign", "campaign status agrees with its dates",
    "campaign budget agrees with its period", "ad spend only while the campaign runs",
    "a campaign advertises in the same few states",
    "a session has one device, place and traffic source", "device, operating system, browser and network agree",
    "organic traffic has no campaign", "a session comes from a campaign that is running",
    "an event shows the product's own name and price", "only a product on sale and in stock is put in a cart",
    "quantity and cart value only on cart events", "payment method only at checkout and purchase",
    "search query only on a search", "an event's city is in its state",
    "an order is the cart of its purchase event", "an order item shows the product's own name and price",
    "an order's city is in its state", "an order has a payment method and a refund only when returned",
]


@pytest.fixture(scope="module")
def found(source):
    batches, module = source
    first = batches[BATCHES[0]].raw["customers"].drop_duplicates("user_id")
    first_state = dict(zip(first["user_id"], first["home_state"]))
    return {name: incoherences(batch, module, first_state) for name, batch in batches.items()}


@pytest.mark.parametrize("rule", RULES)
def test_rule(found, rule):
    assert {batch: rows[rule][:5] for batch, rows in found.items() if rows[rule]} == {}


def test_a_generated_batch_has_no_incoherence_of_any_kind(found, source):
    batches, _ = source
    for name, rows in found.items():
        assert sorted(rows) == sorted(RULES), "every rule is in the list above"
        assert {rule: broken[:3] for rule, broken in rows.items() if broken} == {}, name
    # The scan is not vacuous: every kind of row it judges is there.
    first = batches[BATCHES[0]]
    assert len(first.customers) > 490 and len(first.products) > 140 and len(first.campaigns) > 35
    assert len(first.events) == 8000 and len(first.own_orders) > 800 and len(first.spend) > 500
    assert first.events["campaign_id"].isna().mean() > 0.2 and first.events["campaign_id"].notna().mean() > 0.3
    assert set(first.events["device_type"]) == set(DEVICES) and first.events["state"].nunique() > 15
    assert set(first.customers["membership_tier"]) == set(TIER_FROM) and set(first.customers["user_segment"]) == set(SEGMENT_AGES)
    assert {"planned", "active", "completed", "paused"} == set(first.campaigns["campaign_status"])
    assert first.customers["preferred_language"].nunique() >= 8
    # The reference city table is the generator's.
    assert dict(source[1].CITY_STATE) == CITY_STATE


def test_the_dirty_rows_are_still_there(source):
    batches, _ = source
    for batch in batches.values():
        customers, products, campaigns = (batch.raw[name] for name in ("customers", "products", "campaigns"))
        assert customers.loc[0, "email"] == "invalid-email" and pd.isna(customers.loc[1, "home_city"]) and customers.loc[2, "age"] == -5
        assert customers.loc[500, "user_id"] == customers.loc[3, "user_id"]
        assert pd.isna(products.loc[0, "category"]) and products.loc[1, "original_price"] == -1000 and products.loc[2, "discount_percent"] == 150
        assert pd.isna(campaigns.loc[0, "campaign_id"]) and campaigns.loc[1, "traffic_source"] == "unknown_channel"
        assert campaigns.loc[2, "end_date"] == "1900-01-01"
        orders, items = batch.raw["orders"], batch.raw["order_items"]
        assert orders.loc[0, "order_id"] == orders.loc[1, "order_id"] and orders.loc[2, "cart_value"] == -500
        assert orders.loc[3, "user_id"] == 999999 and orders.loc[4, "order_timestamp"] == "not-a-date"
        assert items.loc[0, "product_id"] == 999999 and items.loc[1, "quantity"] == -3 and items.loc[2, "line_amount"] == 1
        assert batch.raw["web_events"].iloc[-1]["event_type"] == "unknown_event"


def test_master_data_changes_between_batches_are_plausible_and_carried_forward(source):
    batches, _ = source
    previous = None
    for name, batch in batches.items():
        customers = batch.raw["customers"]
        state, changes = customers.iloc[:501], customers.iloc[501:]
        current = state.drop_duplicates("user_id").set_index("user_id")
        assert len(changes) == (1 if name == BATCHES[0] else 5), name
        kinds = []
        for row in changes.to_dict("records"):
            before = current.loc[row["user_id"]]
            assert row["updated_at"] > before["updated_at"]
            assert (row["user_name"], row["gender"]) == (before["user_name"], before["gender"])
            changed = {column for column in ("membership_tier", "loyalty_points", "home_city", "home_state", "email",
                                             "signup_date", "is_prime_user") if row[column] != before[column]}
            if "membership_tier" in changed:
                # SCD Type 2: the tier changed because the points did.
                kinds.append("tier")
                assert changed <= {"membership_tier", "loyalty_points", "is_prime_user"}
                assert row["loyalty_points"] > before["loyalty_points"] and row["membership_tier"] == tier_of(row["loyalty_points"])
                assert TIER_FROM[row["membership_tier"]] > TIER_FROM[before["membership_tier"]]
            elif "home_city" in changed:
                # SCD Type 2: a move changes the city and the state together.
                kinds.append("move")
                assert changed <= {"home_city", "home_state"} and row["home_state"] == CITY_STATE[row["home_city"]]
            elif changed == {"email"}:
                # SCD Type 1: a corrected address, still the customer's own.
                kinds.append("email")
                assert row["email"].split("@")[0] == before["email"].split("@")[0]
            else:
                # SCD Type 0: the source states another signup date.
                kinds.append("signup")
                assert changed == {"signup_date"}
        assert sorted(kinds) == (["tier"] if name == BATCHES[0] else ["email", "move", "signup", "tier", "tier"]), name
        if name != BATCHES[0]:
            # Products: two price changes and one status change (Type 2), one restock (Type 1).
            products = batch.raw["products"]
            state_p = products.iloc[:151].drop_duplicates("product_id").set_index("product_id")
            later = products.iloc[151:]
            product_kinds = []
            for row in later[later["product_id"] < 900_000].to_dict("records"):
                before = state_p.loc[row["product_id"]]
                product_kinds.append("price" if row["discounted_price"] != before["discounted_price"]
                                     else "status" if row["product_status"] != before["product_status"] else "stock")
                if product_kinds[-1] == "stock":
                    assert row["inventory_remaining"] > before["inventory_remaining"]
            assert sorted(product_kinds) == ["price", "price", "status", "stock"], name
        # The next batch starts from this one's latest versions: nothing reverts.
        for table, key, rows in (("customers", "user_id", 501), ("products", "product_id", 151), ("campaigns", "campaign_id", 40)):
            frame = batch.raw[table]
            frame = frame[frame[key].fillna(0) < 900_000]
            if previous is not None:
                expected = previous[table].sort_values("updated_at", kind="stable").drop_duplicates(key, keep="last")
                expected = expected.dropna(subset=[key]).set_index(key).sort_index()
                actual = frame.iloc[:rows].dropna(subset=[key]).drop_duplicates(key).set_index(key).sort_index()
                pd.testing.assert_frame_equal(actual, expected, check_dtype=False, obj=f"{table} of {name}")
        previous = {table: frame[frame[key].fillna(0) < 900_000] for table, key in
                    (("customers", "user_id"), ("products", "product_id"), ("campaigns", "campaign_id"))
                    for frame in [batch.raw[table]]}
    # A campaign's status history follows the calendar across the batches.
    last = batches[BATCHES[-1]].raw["campaigns"]
    first = batches[BATCHES[0]].raw["campaigns"]
    started = set(first[first["campaign_status"] == "planned"]["campaign_id"]) \
        & set(last[last["campaign_status"].isin(["active", "completed"])]["campaign_id"])
    assert len(started) >= 3, "campaigns planned at the first batch have started by the last"


def test_a_batch_older_than_the_newest_raw_batch_is_refused(load_job, project_root, capsys):
    for batch in ("2026-05-18", "2026-06-18"):
        load_job(GENERATOR, BATCH_DATE=batch).main()
    raw = project_root / "data" / "raw"
    with pytest.raises(SystemExit) as refused:
        load_job(GENERATOR, BATCH_DATE="2026-06-01").main()
    message = str(refused.value)
    assert "Cannot generate BATCH_DATE=2026-06-01" in message and "a newer raw batch already exists (2026-06-18)" in message
    assert "Raw batches are generated in date order" in message
    assert sorted(path.name for path in raw.iterdir()) == ["batch_date=2026-05-18", "batch_date=2026-06-18"], "nothing was written"
    # Reprocessing an existing batch, the older one included, still works: it is complete and untouched.
    before = {path.name: path.read_bytes() for path in (raw / "batch_date=2026-05-18").iterdir()}
    capsys.readouterr()
    load_job(GENERATOR, BATCH_DATE="2026-05-18").main()
    assert "already complete" in capsys.readouterr().out
    assert {path.name: path.read_bytes() for path in (raw / "batch_date=2026-05-18").iterdir()} == before
    # A later date is generated as before.
    load_job(GENERATOR, BATCH_DATE="2026-06-19").main()
    assert (raw / "batch_date=2026-06-19" / "orders.csv").is_file()


def test_currency_is_stated_wherever_money_is_described(source):
    readme = (CODE_ROOT / "README.md").read_text(encoding="utf-8")
    dictionary = (CODE_ROOT / "docs" / "warehouse" / "power_bi_data_dictionary.md").read_text(encoding="utf-8")
    refresh = (CODE_ROOT / "docs" / "warehouse" / "power_bi_refresh_flow.md").read_text(encoding="utf-8")
    assert "**Currency.** Every monetary value in this project is in Indian rupees (INR)" in readme
    assert "**Currency.** Every monetary field in every dataset is in Indian rupees (INR)" in dictionary
    assert "Every monetary field in the exports of both routes is in Indian rupees (INR)" in refresh

    # dbt: the description of every monetary column says so.
    models: dict[str, dict[str, str]] = {}
    schema = (CODE_ROOT / "dbt" / "models" / "schema.yml").read_text(encoding="utf-8")
    model = column = None
    for line in schema[schema.index("\nmodels:"):].splitlines():
        if line.startswith("  - name: "):
            model, column = line.split(": ", 1)[1], None
            models[model] = {}
        elif line.startswith("      - name: "):
            column = line.split(": ", 1)[1]
            models[model][column] = ""
        elif line.startswith("        description: ") and column:
            models[model][column] = line.split(": ", 1)[1]
    monetary = {
        "dim_product": ["original_price", "current_price"], "dim_campaign": ["budget"],
        "fact_orders": ["gross_amount", "discount_amount", "net_revenue", "tax_amount", "shipping_amount", "total_amount",
                        "refund_amount"],
        "fact_order_items": ["unit_price", "gross_amount", "discount_amount", "line_revenue", "refund_amount"],
        "fact_campaign_spend": ["spend_amount", "attributed_revenue"], "fact_web_events": ["event_value"],
        "mart_customer_360": ["lifetime_value", "net_sales", "refund_amount", "average_order_value"],
        "mart_campaign_performance": ["campaign_budget", "total_spend", "attributed_revenue", "platform_attributed_revenue"],
        "mart_product_sales": ["product_revenue", "original_price", "current_price"],
        "mart_revenue_daily": ["gross_revenue", "discount_amount", "net_revenue", "refund_amount",
                               "net_revenue_after_refunds", "total_amount"],
    }
    missing = [f"{model}.{column}" for model, columns in monetary.items() for column in columns
               if "Indian rupees (INR)" not in models[model].get(column, "")]
    assert missing == []

    # The source says it once, in its contract, and has no currency column.
    batches, module = source
    assert module.CURRENCY == "INR"
    first = batches[BATCHES[0]]
    assert not [column for frame in first.raw.values() for column in frame.columns if "currency" in column.lower()]


def test_the_generator_refuses_to_build_on_raw_batches_of_another_version(load_job, project_root):
    generator = load_job(GENERATOR, BATCH_DATE="2026-05-18")
    generator.main()
    raw = project_root / "data" / "raw"
    truth = raw / "batch_date=2026-05-18" / "generation_truth.json"
    lines = truth.read_text(encoding="utf-8").splitlines()
    contract = json.loads(lines[0])
    # Each raw batch records the version that wrote it.
    assert contract["generator_version"] == generator.GENERATOR_VERSION

    # The same batch as an older generator would have left it.
    truth.write_text("\n".join([json.dumps({**contract, "generator_version": "session_budget_v4"}), *lines[1:]]) + "\n",
                     encoding="utf-8")
    with pytest.raises(SystemExit) as refused:
        load_job(GENERATOR, BATCH_DATE="2026-06-18").main()
    message = str(refused.value)
    assert "Cannot generate BATCH_DATE=2026-06-18" in message and f"generator version {generator.GENERATOR_VERSION}" in message
    assert "2026-05-18 (session_budget_v4)" in message and "Regenerate the raw data from the first batch" in message
    assert not (raw / "batch_date=2026-06-18").exists(), "nothing was written"

    # A batch with no recorded version at all is refused in the same way.
    truth.unlink()
    with pytest.raises(SystemExit, match=r"2026-05-18 \(unrecorded\)"):
        load_job(GENERATOR, BATCH_DATE="2026-06-18").main()
    # With the version it was written by, the next batch is generated.
    truth.write_text("\n".join(lines) + "\n", encoding="utf-8")
    load_job(GENERATOR, BATCH_DATE="2026-06-18").main()
    assert (raw / "batch_date=2026-06-18" / "orders.csv").is_file()
