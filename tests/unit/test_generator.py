"""The data generator is deterministic, incremental and atomic.

Fixed in e2d35c1: a batch is reproduced byte for byte from its BATCH_DATE, a
batch folder only ever appears complete (written to a temporary folder, then
renamed), and dates before the anchor batch are refused.

p2-data-v2: every order has at least one item, and its cart_value is the sum
of its own items' line_amount (it used to be copied from an unrelated
purchase event). The dirty records stay dirty.
"""
from __future__ import annotations

import pandas as pd
import pytest

GENERATOR = "data-generator/generate_data.py"
RAW_FILES = ["customers.csv", "products.csv", "campaigns.csv", "orders.csv",
             "order_items.csv", "ad_spend.csv", "web_events.json", "generation_truth.json"]


def raw_dir(root, batch_date):
    return root / "data" / "raw" / f"batch_date={batch_date}"


def test_a_batch_is_byte_identical_across_runs(load_job, tmp_path):
    batch = "2026-06-18"
    for name in ("first", "second"):
        root = tmp_path / name
        load_job(GENERATOR, root=root, BATCH_DATE=batch).main()

    first, second = raw_dir(tmp_path / "first", batch), raw_dir(tmp_path / "second", batch)
    assert sorted(p.name for p in first.iterdir()) == sorted(RAW_FILES)
    for name in RAW_FILES:
        assert (first / name).read_bytes() == (second / name).read_bytes(), f"{name} differs between two runs"


def test_a_crash_leaves_no_partial_batch_and_a_rerun_completes(load_job, project_root, monkeypatch):
    batch = "2026-06-20"
    real_to_csv = pd.DataFrame.to_csv
    calls = {"n": 0}

    def crash_on_fourth_file(self, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 4:
            raise RuntimeError("simulated crash while writing")
        return real_to_csv(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", crash_on_fourth_file)
    with pytest.raises(RuntimeError, match="simulated crash"):
        load_job(GENERATOR, BATCH_DATE=batch).main()
    assert not raw_dir(project_root, batch).exists(), "a crashed run left a batch folder behind"

    monkeypatch.setattr(pd.DataFrame, "to_csv", real_to_csv)
    load_job(GENERATOR, BATCH_DATE=batch).main()
    files = raw_dir(project_root, batch)
    assert sorted(p.name for p in files.iterdir()) == sorted(RAW_FILES)
    assert all((files / name).stat().st_size > 0 for name in RAW_FILES)


def test_a_date_before_the_anchor_is_refused(load_job, project_root):
    with pytest.raises(SystemExit):
        load_job(GENERATOR, BATCH_DATE="2026-05-17").main()
    assert not (project_root / "data" / "raw" / "batch_date=2026-05-17").exists()


@pytest.fixture
def increment_batch(load_job, project_root):
    """One generated increment batch: (orders, order_items)."""
    load_job(GENERATOR, BATCH_DATE="2026-06-18").main()
    folder = raw_dir(project_root, "2026-06-18")
    return pd.read_csv(folder / "orders.csv"), pd.read_csv(folder / "order_items.csv")


def test_every_order_has_at_least_one_item(increment_batch):
    orders, items = increment_batch
    # The appended dirty fixtures (IDs at the top of the batch's ID block) are
    # copies of real orders with one bad value and no items of their own;
    # Silver quarantines them.
    orders = orders[orders["order_id"] % 100_000 < 99_900]
    empty = sorted(set(orders["order_id"]) - set(items["order_id"]))
    assert empty == [], f"{len(empty)} orders have no items, e.g. {empty[:5]}"


def test_order_totals_equal_the_sum_of_their_lines(increment_batch):
    orders, items = increment_batch
    # Totals are built from the clean line amounts. Of the dirty item rows only
    # row 2 has a dirty line_amount (1); its clean amount is quantity x price.
    # Row 1's quantity is dirty (-3) but its line_amount is still the clean one.
    clean_line = items["line_amount"].copy()
    clean_line[2] = round(items.loc[2, "quantity"] * items.loc[2, "discounted_price"], 2)
    sums = clean_line.groupby(items["order_id"]).sum().round(2)
    totals = orders.assign(line_sum=orders["order_id"].map(sums))

    dirty = totals["cart_value"] <= 0
    assert dirty.sum() == 1, "the one dirty order (cart_value -500) must stay dirty"
    mismatched = totals[~dirty & ((totals["cart_value"] - totals["line_sum"]).abs() > 0.005)]
    assert mismatched.empty, mismatched[["order_id", "cart_value", "line_sum"]].head().to_string()

    # The dirty item rows are still dirty.
    assert items.loc[0, "product_id"] == 999999
    assert items.loc[1, "quantity"] == -3
    assert items.loc[2, "line_amount"] == 1


def test_legacy_complete_batch_without_truth_is_not_replaced(load_job, project_root):
    module = load_job(GENERATOR, BATCH_DATE='2026-06-18')
    folder = raw_dir(project_root, '2026-06-18')
    folder.mkdir(parents=True)
    for name in RAW_FILES:
        if name != 'generation_truth.json':
            (folder / name).write_text('existing raw data\n')
    module.main()
    assert not (folder / 'generation_truth.json').exists()
    assert all(p.read_text() == 'existing raw data\n' for p in folder.iterdir())


# --- Part J: real attributes from their own random streams ------------------------

@pytest.fixture
def anchor_and_increment(load_job, tmp_path):
    """The raw folders of the anchor batch and of one later batch."""
    for batch in ("2026-05-18", "2026-06-18"):
        load_job(GENERATOR, root=tmp_path, BATCH_DATE=batch).main()
    return {batch: raw_dir(tmp_path, batch) for batch in ("2026-05-18", "2026-06-18")}


def without_fixtures(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    # The appended dirty rows have IDs at the top of the batch's ID block (facts:
    # orders, ad spend) or 900001 and up (master data: products, campaigns).
    if key in ("product_id", "campaign_id"):
        return frame[frame[key] < 900_000]
    return frame[frame[key] % 100_000 < 99_900]


def test_order_status_tax_shipping_and_total_are_generated(anchor_and_increment):
    generator_text = (anchor_and_increment["2026-05-18"].parents[2] / GENERATOR).read_text(encoding="utf-8")
    assert 'random.Random(f"order_status|{int(order_id)}")' in generator_text, "its own stream per order"
    for batch, folder in anchor_and_increment.items():
        orders = pd.read_csv(folder / "orders.csv")
        clean = without_fixtures(orders, "order_id").drop(index=[0, 2, 3, 4], errors="ignore")
        shares = clean["order_status"].value_counts(normalize=True)
        assert set(shares.index) == {"completed", "cancelled", "returned"}, batch
        assert 0.80 < shares["completed"] < 0.95 and shares["cancelled"] > 0.02 and shares["returned"] > 0.01, (batch, shares)
        # Tax is 18% of the net value; shipping is INR 99 under INR 4,999; the total is their sum.
        from decimal import ROUND_HALF_UP, Decimal

        tax = [float((Decimal(str(net)) * Decimal("0.18")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
               for net in clean["cart_value"]]
        assert clean["tax_amount"].tolist() == tax, batch
        assert ((clean["cart_value"] >= 4999) == (clean["shipping_amount"] == 0)).all(), batch
        assert set(clean["shipping_amount"]) == {0.0, 99.0}, batch
        # J3: realistic order values, so the shipping rule applies to a meaningful share.
        assert 0.2 < (clean["shipping_amount"] > 0).mean() < 0.7, batch
        assert 3000 < clean["cart_value"].mean() < 15000, batch
        assert ((clean["cart_value"] + clean["tax_amount"] + clean["shipping_amount"]).round(2)
                == clean["total_amount"]).all(), batch
        # cart_value is still the sum of the lines: tax and shipping are not in it.
        items = pd.read_csv(folder / "order_items.csv").iloc[3:]
        lines = items.groupby("order_id")["line_amount"].sum().round(2)
        complete = clean[~clean["order_id"].isin(pd.read_csv(folder / "order_items.csv").iloc[:3]["order_id"])]
        assert (complete["order_id"].map(lines) == complete["cart_value"]).all(), batch


def test_the_ad_feed_is_per_campaign_day_and_region_and_internally_coherent(anchor_and_increment):
    feed = pd.concat(without_fixtures(pd.read_csv(folder / "ad_spend.csv"), "spend_id")
                     for folder in anchor_and_increment.values())
    # Grain: campaign, day, region. Each campaign targets 2 to 5 states, the same ones every day.
    assert not feed.duplicated(["campaign_id", "spend_date", "state"]).any()
    targeted = feed.groupby("campaign_id")["state"].nunique()
    per_day = feed.groupby(["campaign_id", "spend_date"])["state"].nunique()
    assert targeted.between(2, 5).all() and (per_day == per_day.index.get_level_values(0).map(targeted)).all()
    assert (feed["country"] == "India").all() and feed["state"].nunique() >= 10
    # The regional rows of a campaign-day sum to the platform's total for it, to the paisa.
    days = feed.groupby(["campaign_id", "spend_date"]).agg(regional=("spend_amount", "sum"),
                                                           total=("campaign_day_spend", "max"),
                                                           totals=("campaign_day_spend", "nunique"))
    assert ((days["regional"] - days["total"]).abs() < 0.005).all() and (days["totals"] == 1).all()

    # Clicks, impressions, conversions and revenue follow from the spend.
    assert (feed["clicks"] <= feed["impressions"]).all() and (feed["conversions"] <= feed["clicks"]).all()
    assert ((feed["conversions"] == 0) == (feed["attributed_revenue"] == 0)).all()
    ctr = feed["clicks"].sum() / feed["impressions"].sum()
    cpc = feed["spend_amount"].sum() / feed["clicks"].sum()
    conversion_rate = feed["conversions"].sum() / feed["clicks"].sum()
    platform_roas = feed["attributed_revenue"].sum() / feed["spend_amount"].sum()
    assert 0.008 < ctr < 0.05 and 6 < cpc < 40 and 0.003 < conversion_rate < 0.03, (ctr, cpc, conversion_rate)
    assert 2 < platform_roas < 5, platform_roas
    by_channel = feed.groupby("traffic_source").agg(spend=("spend_amount", "sum"), clicks=("clicks", "sum"),
                                                    revenue=("attributed_revenue", "sum"))
    assert (by_channel["revenue"] / by_channel["spend"]).between(1, 9).all(), "single-digit ROAS on every channel"
    assert (by_channel.loc["google", "spend"] / by_channel.loc["google", "clicks"]
            > by_channel.loc["facebook", "spend"] / by_channel.loc["facebook", "clicks"]), "search clicks cost more than social"

    # Comparable with what the shop sells: completed revenue over the same spend is single digits too.
    orders = pd.concat(without_fixtures(pd.read_csv(folder / "orders.csv"), "order_id")
                       for folder in anchor_and_increment.values())
    warehouse_roas = orders[orders["order_status"] == "completed"]["cart_value"].sum() / feed["spend_amount"].sum()
    assert 1 < warehouse_roas < 9 and 0.33 < platform_roas / warehouse_roas < 3, (platform_roas, warehouse_roas)


def test_prices_and_quantities_are_realistic(anchor_and_increment):
    products = without_fixtures(pd.read_csv(anchor_and_increment["2026-05-18"] / "products.csv"), "product_id").iloc[3:150]
    ranges = {"Electronics": (799, 14999), "Fashion": (399, 3999), "Home": (299, 7999), "Beauty": (149, 1499),
              "Fitness": (299, 4999), "Books": (149, 899)}
    for category, (low, high) in ranges.items():
        prices = products[products["category"] == category]["original_price"]
        assert len(prices) > 5 and prices.between(low, high).all(), (category, prices.min(), prices.max())
        assert (prices % 10 == 9).all(), "prices end in 9"
    assert products.groupby("category")["original_price"].median()["Electronics"] \
        > 3 * products.groupby("category")["original_price"].median()["Books"]
    items = pd.read_csv(anchor_and_increment["2026-05-18"] / "order_items.csv").iloc[3:]
    assert set(items["quantity"]) == {1, 2, 3} and (items["quantity"] == 1).mean() > 0.5
    campaigns = without_fixtures(pd.read_csv(anchor_and_increment["2026-05-18"] / "campaigns.csv").dropna(subset=["campaign_id"]),
                                 "campaign_id")
    assert campaigns["budget"].between(25000, 300000 * 1.5).all()


def test_product_brand_and_status_and_campaign_type_are_generated(anchor_and_increment):
    first = pd.read_csv(anchor_and_increment["2026-05-18"] / "products.csv")
    later = pd.read_csv(anchor_and_increment["2026-06-18"] / "products.csv")
    base, base_later = without_fixtures(first, "product_id").iloc[:150], without_fixtures(later, "product_id").iloc[:150]
    # The same product has the same brand and base status in every batch.
    assert base[["product_id", "brand", "product_status"]].equals(base_later[["product_id", "brand", "product_status"]])
    assert base["brand"].nunique() >= 12 and base["brand"].notna().all()
    assert set(base["product_status"]) == {"active", "discontinued"}
    assert 0.02 < (base["product_status"] == "discontinued").mean() < 0.15
    # The anchor change withdraws product 5; a later batch flips one product's status and nothing else.
    assert first[first["product_id"] == 5]["product_status"].tolist() == ["active", "discontinued"]
    changes = without_fixtures(later, "product_id").iloc[151:]
    merged = changes.merge(base_later, on="product_id", suffixes=("", "_base"))
    status_only = merged[(merged["product_status"] != merged["product_status_base"])
                         & (merged["discounted_price"] == merged["discounted_price_base"])]
    assert len(status_only) == 1 and status_only["updated_at"].iloc[0] == "2026-06-18T08:00:00"

    campaigns = without_fixtures(pd.read_csv(anchor_and_increment["2026-06-18"] / "campaigns.csv").dropna(subset=["campaign_id"]),
                                 "campaign_id")
    assert set(campaigns["campaign_type"]) == {"awareness", "consideration", "conversion", "retention"}
    assert campaigns.groupby("campaign_id")["campaign_type"].nunique().max() == 1, "a campaign keeps its objective"


def test_each_new_attribute_has_one_dirty_row_recorded_in_the_truth_file(anchor_and_increment):
    import json

    expected = {
        "products.csv": {"invalid_product_status": ("product_status", "recalled"), "missing_brand": ("brand", None)},
        "campaigns.csv": {"invalid_campaign_type": ("campaign_type", "brand_lift")},
        "orders.csv": {"invalid_order_status": ("order_status", "shipped"), "negative_tax": ("tax_amount", -1.0),
                       "negative_shipping": ("shipping_amount", -99.0), "total_amount_mismatch": ("total_amount", None)},
        "ad_spend.csv": {"negative_spend": ("spend_amount", -100), "clicks_exceed_impressions": ("clicks", None),
                         "missing_campaign": ("campaign_id", 999999), "missing_date": ("spend_date", None),
                         "conversions_exceed_clicks": ("conversions", None),
                         "negative_attributed_revenue": ("attributed_revenue", -1.0), "missing_region": ("state", None)},
    }
    for batch, folder in anchor_and_increment.items():
        truth = [json.loads(line) for line in (folder / "generation_truth.json").read_text(encoding="utf-8").splitlines()]
        assert truth[0]["generator_version"] == "session_budget_v3"
        fixtures = {(row["table"], row["reason"]): row["row_index"] for row in truth if row["kind"] == "quality_fixture"}
        for table, reasons in expected.items():
            frame = pd.read_csv(folder / table)
            for reason, (column, value) in reasons.items():
                row = frame.loc[fixtures[(table, reason)]]
                if value is None:
                    assert pd.isna(row[column]) or reason in ("total_amount_mismatch", "conversions_exceed_clicks",
                                                              "clicks_exceed_impressions"), (batch, reason)
                else:
                    assert row[column] == value, (batch, reason, row[column])
        mismatch = pd.read_csv(folder / "orders.csv").loc[fixtures[("orders.csv", "total_amount_mismatch")]]
        assert round(mismatch["total_amount"] - mismatch["cart_value"] - mismatch["tax_amount"] - mismatch["shipping_amount"], 2) == 10
        over = pd.read_csv(folder / "ad_spend.csv").loc[fixtures[("ad_spend.csv", "conversions_exceed_clicks")]]
        assert over["conversions"] == over["clicks"] + 1


def test_the_draws_that_were_not_meant_to_change_are_unchanged(anchor_and_increment):
    # Part J added attributes from separate random streams; J3 changed prices,
    # quantities, campaign budgets and the ad feed on purpose. Every other
    # value is the one the generator wrote before Part J: these digests are of
    # its files (the old rows; for web events every field but the price ones).
    import hashlib
    import json

    kept = {
        "customers.csv": None,
        "orders.csv": ["order_id", "user_id", "campaign_id", "order_timestamp", "payment_method", "fraud_score",
                       "country", "city", "schema_version", "source"],
        "order_items.csv": ["order_item_id", "order_id", "product_id", "product_name", "category", "discount_percent"],
        "products.csv": ["product_id", "product_name", "category", "discount_percent", "inventory_remaining",
                         "schema_version", "source", "updated_at"],
        "campaigns.csv": ["campaign_id", "campaign_name", "traffic_source", "ab_test_group", "target_segment",
                          "campaign_status", "start_date", "end_date", "schema_version", "source", "updated_at"],
    }
    old_rows = {"2026-05-18": {"customers.csv": 502, "orders.csv": 1311, "order_items.csv": 5000, "products.csv": 152,
                               "campaigns.csv": 41},
                "2026-06-18": {"customers.csv": 504, "orders.csv": 150, "order_items.csv": 625, "products.csv": 153,
                               "campaigns.csv": 41}}
    # (state is a new field of each event, derived from its city)
    price_fields = ("original_price", "discounted_price", "cart_value", "quantity", "state")
    digests = {}
    for batch, folder in anchor_and_increment.items():
        for name, columns in kept.items():
            frame = pd.read_csv(folder / name, dtype=str, keep_default_na=False).iloc[:old_rows[batch][name]]
            frame = frame if columns is None else frame[columns]
            digests[f"{batch}/{name}"] = hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()[:16]
        events = [json.loads(line) for line in (folder / "web_events.json").read_text(encoding="utf-8").splitlines()]
        events = [{key: value for key, value in event.items() if key not in price_fields} for event in events]
        digests[f"{batch}/web_events.json"] = hashlib.sha256(json.dumps(events, sort_keys=True).encode()).hexdigest()[:16]
    assert digests == EXPECTED_UNCHANGED_DIGESTS


# sha256 (first 16 hex digits), computed from the files the generator wrote before Part J.
EXPECTED_UNCHANGED_DIGESTS = {
    "2026-05-18/customers.csv": "8b0ececa3490fd56", "2026-05-18/orders.csv": "86cb92e37d112ec1",
    "2026-05-18/order_items.csv": "69ab2208ea11e3f8", "2026-05-18/products.csv": "1a540b680437060b",
    "2026-05-18/campaigns.csv": "978dfe8e064cf547", "2026-05-18/web_events.json": "f42b7e013225bd3b",
    "2026-06-18/customers.csv": "0106d3cecf5057c3", "2026-06-18/orders.csv": "34f135e230e77dcc",
    "2026-06-18/order_items.csv": "2e09ab3c28b2af4e", "2026-06-18/products.csv": "2111fed3cbddc9c4",
    "2026-06-18/campaigns.csv": "cacdc7e1088ce2ad", "2026-06-18/web_events.json": "4379e2aa6cfc7490",
}
