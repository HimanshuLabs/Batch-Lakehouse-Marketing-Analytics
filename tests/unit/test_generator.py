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
             "order_items.csv", "ad_spend.csv", "web_events.json"]


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
