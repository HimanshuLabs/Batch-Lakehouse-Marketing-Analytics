"""Budgets, stock and the customer changes per batch follow the shop's size.

At the default size (500 customers) each helper returns its argument itself
and draws nothing, so the default batches stay byte for byte what they were
(tests/unit/test_scale.py pins their sha256). Above it the rule is the one web
events and the order ceiling already follow: the default's value times
P2_NUM_CUSTOMERS / 500, rounded down where a whole number is needed.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from test_coherent_source import CITY_STATE, RULES, TIER_FROM, Batch, incoherences, tier_of

GENERATOR = "data-generator/generate_data.py"
BATCH_DATES = ["2026-05-18", "2026-06-04", "2026-06-18"]


def generate(load_job, root: Path, batch_date: str, customers: int):
    module = load_job(GENERATOR, root=root, BATCH_DATE=batch_date, P2_NUM_CUSTOMERS=str(customers))
    module.main()
    return module


def raw_folder(root: Path, batch_date: str) -> Path:
    return root / "data" / "raw" / f"batch_date={batch_date}"


def test_size_scaling_is_the_identity_at_the_default_size(load_job):
    module = load_job(GENERATOR, BATCH_DATE="2026-05-18")
    amount = float("1234.5678") * 3   # a float object made at run time
    assert module.size_scaled(amount) is amount, "the default's budget is the very same float"
    assert module.size_scaled_count(1937) == 1937
    assert (module.CUSTOMER_RISES, module.CUSTOMER_MOVES) == (2, 1)
    assert module.CHANGE_COUNTS == {"customers": 3, "products": 2, "campaigns": 1}


@pytest.mark.parametrize("customers, amount, count, rises, moves", [
    (750, 150.0, 7, 3, 1),          # not a multiple of 500: the ratio, rounded down for whole numbers
    (1000, 200.0, 10, 4, 2),
    (400_000, 80_000.0, 4000, 1600, 800),
])
def test_one_rule_for_every_size(load_job, customers, amount, count, rises, moves):
    module = load_job(GENERATOR, BATCH_DATE="2026-05-18", P2_NUM_CUSTOMERS=str(customers))
    assert module.size_scaled(100.0) == amount
    assert module.size_scaled_count(5) == count
    assert (module.CUSTOMER_RISES, module.CUSTOMER_MOVES) == (rises, moves)
    assert module.CHANGE_COUNTS == {"customers": rises + moves, "products": 2, "campaigns": 1}


def test_budgets_and_stock_follow_the_shop_and_nothing_else_moves(load_job, tmp_path):
    modules, frames = {}, {}
    for customers in (500, 1000):
        root = tmp_path / str(customers)
        modules[customers] = generate(load_job, root, "2026-05-18", customers)
        folder = raw_folder(root, "2026-05-18")
        frames[customers] = {name: pd.read_csv(folder / f"{name}.csv") for name in ("campaigns", "products", "ad_spend")}
    small, large = frames[500], frames[1000]

    # Budgets: twice the default's, to the paisa; every other campaign field is the same.
    pd.testing.assert_frame_equal(large["campaigns"].drop(columns="budget"), small["campaigns"].drop(columns="budget"))
    assert ((large["campaigns"]["budget"] - 2 * small["campaigns"]["budget"]).abs() <= 0.011).all()

    # Stock: exactly twice, so zero stays zero; every other product field is the same.
    pd.testing.assert_frame_equal(large["products"].drop(columns="inventory_remaining"),
                                  small["products"].drop(columns="inventory_remaining"))
    assert (large["products"]["inventory_remaining"] == 2 * small["products"]["inventory_remaining"]).all()
    # So the products that can go into a cart are the same.
    assert [modules[1000].purchasable(p) for p in large["products"].to_dict("records")] \
        == [modules[500].purchasable(p) for p in small["products"].to_dict("records")]

    # Ad spend: the same rows (campaign-days and regions); amounts about twice. The
    # appended fixtures copy or fix values of their own and are left out.
    spend = {size: frame["ad_spend"][frame["ad_spend"]["spend_id"] < modules[size].FIXTURE_ID_START].reset_index(drop=True)
             for size, frame in frames.items()}
    keys = ["spend_id", "campaign_id", "traffic_source", "spend_date", "state"]
    pd.testing.assert_frame_equal(spend[1000][keys], spend[500][keys])
    assert ((spend[1000]["campaign_day_spend"] - 2 * spend[500]["campaign_day_spend"]).abs() <= 0.02).all()
    assert ((spend[1000]["spend_amount"] - 2 * spend[500]["spend_amount"]).abs() <= 0.10).all()


def test_customer_changes_follow_the_shop(load_job, tmp_path):
    customers, factor = 1000, 2
    root = tmp_path / "shop"
    module = None
    for batch_date in BATCH_DATES:
        module = generate(load_job, root, batch_date, customers)
    batches = {batch_date: Batch(raw_folder(root, batch_date), batch_date) for batch_date in BATCH_DATES}

    # Every coherence rule of the default shop holds; the budget rule is read per 500 customers.
    first = batches[BATCH_DATES[0]].raw["customers"].drop_duplicates("user_id")
    first_state = dict(zip(first["user_id"], first["home_state"]))
    for name, batch in batches.items():
        batch.campaigns = batch.campaigns.assign(budget=batch.campaigns["budget"] / factor)
        found = incoherences(batch, module, first_state)
        assert sorted(found) == sorted(RULES)
        assert {rule: rows[:3] for rule, rows in found.items() if rows} == {}, name

    # Per later batch: 2 rises and 1 move per 500 customers, one corrected email and
    # one re-dated signup, each customer at most once; and nothing reverts.
    state_rows = customers + 1   # the base rows and the duplicate customer
    previous = None
    for name, batch in batches.items():
        frame = batch.raw["customers"]
        state, changes = frame.iloc[:state_rows], frame.iloc[state_rows:]
        current = state.drop_duplicates("user_id").set_index("user_id")
        if name == BATCH_DATES[0]:
            assert len(changes) == 1, "the anchor batch's fixed change set"
        else:
            assert changes["user_id"].nunique() == len(changes) == 8, name
            kinds = []
            for row in changes.to_dict("records"):
                before = current.loc[row["user_id"]]
                assert row["updated_at"] > before["updated_at"]
                changed = {column for column in ("membership_tier", "loyalty_points", "home_city", "home_state", "email",
                                                 "signup_date", "is_prime_user") if row[column] != before[column]}
                if "membership_tier" in changed:
                    kinds.append("tier")
                    assert changed <= {"membership_tier", "loyalty_points", "is_prime_user"}
                    assert row["loyalty_points"] > before["loyalty_points"] and row["membership_tier"] == tier_of(row["loyalty_points"])
                    assert TIER_FROM[row["membership_tier"]] > TIER_FROM[before["membership_tier"]]
                elif "home_city" in changed:
                    kinds.append("move")
                    assert changed <= {"home_city", "home_state"} and row["home_state"] == CITY_STATE[row["home_city"]]
                elif changed == {"email"}:
                    kinds.append("email")
                    assert row["email"].split("@")[0] == before["email"].split("@")[0]
                else:
                    kinds.append("signup")
                    assert changed == {"signup_date"}
            assert sorted(kinds) == ["email", "move", "move", "signup", "tier", "tier", "tier", "tier"], name
        if previous is not None:
            expected = previous.sort_values("updated_at", kind="stable").drop_duplicates("user_id", keep="last")
            expected = expected.set_index("user_id").sort_index()
            actual = state.drop_duplicates("user_id").set_index("user_id").sort_index()
            pd.testing.assert_frame_equal(actual, expected, check_dtype=False, obj=f"customers of {name}")
        previous = frame
