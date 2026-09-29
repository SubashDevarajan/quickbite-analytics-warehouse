"""The simulated source must be deterministic and contain the quirks the
warehouse is designed to handle. Pure Python, runs in about a second."""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

from quickbite import generator as g

D = date(2026, 9, 10)


def test_same_date_produces_identical_extracts():
    assert g.extract_day(D) == g.extract_day(D)


def test_extracts_have_expected_shape():
    ex = g.extract_day(D)
    assert set(ex) == {"restaurants", "customers", "orders"}
    assert len(ex["restaurants"]) > 100
    assert len(ex["customers"]) > 3000
    assert 1000 < len(ex["orders"]) < 3000
    assert all(r["_extract_date"] == D for rows in ex.values() for r in rows)


def test_orders_extract_contains_exact_duplicates():
    rows = g.orders_extract(D)
    keys = Counter((r["order_id"], r["updated_at"]) for r in rows)
    assert any(c > 1 for c in keys.values())


def test_late_status_changes_appear_in_later_extracts():
    placed_day = D - timedelta(days=1)
    refunded = [r for r in g.orders_extract(D) if r["status"] == "REFUNDED"]
    assert refunded, "refunds of earlier orders should appear in today's extract"
    assert any(r["order_ts"].date() < D for r in refunded)
    # and an order placed just before midnight is delivered in the next extract
    next_day = [r for r in g.orders_extract(placed_day + timedelta(days=1))
                if r["order_ts"].date() == placed_day and r["status"] == "DELIVERED"]
    assert next_day


def test_restaurants_can_arrive_after_their_first_orders():
    late = 0
    for i in range(20):
        d = D + timedelta(days=i)
        listed = {r["restaurant_id"] for r in g.restaurants_extract(d)}
        late += sum(1 for r in g.orders_extract(d) if r["restaurant_id"] not in listed)
    assert late > 0


def test_dimensions_change_over_time():
    before = {r["restaurant_id"]: r for r in g.restaurants_extract(D)}
    after = {r["restaurant_id"]: r for r in g.restaurants_extract(D + timedelta(days=7))}
    changed = [rid for rid in before if before[rid]["commission_pct"] != after[rid]["commission_pct"]]
    assert changed
    assert all(after[rid]["updated_at"] > before[rid]["updated_at"] for rid in changed)


def test_order_versions_move_forward_in_time():
    for o in g.orders_placed_on(D)[:300]:
        times = [v.updated_at for v in o.versions]
        assert times == sorted(times)
        assert o.versions[0].status == "PLACED"
        assert o.versions[-1].status in g.FINAL_STATUSES
