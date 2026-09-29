"""Simulated source system: the daily exports a food-delivery backend would
hand to the data team.

Every day ``D`` the "source" drops three extracts:

* ``restaurants``  full snapshot of the restaurant catalogue as of end of D
* ``customers``    full snapshot of customers as of end of D
* ``orders``       incremental: every order whose state *changed* on D

The data is realistic on purpose, because each quirk is something the
warehouse has to handle:

* **Slowly changing dimensions**: restaurants change commission rate and
  cuisine, customers move zone and get loyalty upgrades. Reports must use the
  value that was true *when the order happened* (SCD Type 2).
* **Late-arriving dimension**: about half of new restaurants appear in the
  catalogue extract one day *after* their first orders.
* **Late updates to facts**: an order placed at 23:50 is still
  OUT_FOR_DELIVERY in that day's extract and becomes DELIVERED in the next
  one; about 2% of orders are REFUNDED one or two days later.
* **Duplicates**: about 0.3% of order rows appear twice in a file.

Generation is fully deterministic: the same date always produces the same
files, so re-running or back-filling a day is reproducible (idempotent).
Pure Python; Parquet writing lives in ``landing.py``.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal

EPOCH = date(2026, 1, 1)  # the source system "went live" here

ZONES = [
    "KORAMANGALA", "INDIRANAGAR", "HSR_LAYOUT", "WHITEFIELD",
    "JAYANAGAR", "MARATHAHALLI", "ELECTRONIC_CITY", "HEBBAL",
]
CUISINES = ["NORTH_INDIAN", "SOUTH_INDIAN", "CHINESE", "BIRYANI", "PIZZA", "BURGERS", "DESSERTS", "HEALTHY"]
TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
PAYMENT_METHODS = ["UPI", "CARD", "COD", "WALLET"]
PAYMENT_WEIGHTS = [55, 20, 15, 10]
# Orders per hour of day: lunch and dinner peaks.
HOUR_WEIGHTS = [1, 0.5, 0.3, 0.2, 0.2, 0.3, 0.6, 1.2, 2.0, 2.2, 2.4, 3.5,
                6.0, 6.5, 4.0, 2.5, 2.2, 2.8, 4.0, 6.5, 7.5, 6.5, 4.0, 2.0]
FINAL_STATUSES = ("DELIVERED", "CANCELLED", "REFUNDED")

INITIAL_RESTAURANTS = 100
INITIAL_CUSTOMERS = 3000
BASE_ORDERS_PER_DAY = 1500


def _rng(tag: str, d: date) -> random.Random:
    # Seeding with a string is stable across Python processes and versions.
    return random.Random(f"quickbite:{tag}:{d.isoformat()}")


def _at(d: date, seconds: float) -> datetime:
    return datetime.combine(d, time()) + timedelta(seconds=int(seconds))


def _random_ts(rng: random.Random, d: date, start_h: float = 0, end_h: float = 24) -> datetime:
    return _at(d, rng.uniform(start_h * 3600, end_h * 3600 - 1))


def _money(paise: int) -> Decimal:
    return (Decimal(paise) / 100).quantize(Decimal("0.01"))


# ----------------------------------------------------------------- dimensions
@dataclass
class Restaurant:
    restaurant_id: str
    name: str
    zone: str
    cuisine: str
    commission_pct: Decimal
    is_active: bool
    created_at: datetime
    updated_at: datetime
    listed_on: date  # first extract date the restaurant appears in

    def row(self) -> dict:
        return {
            "restaurant_id": self.restaurant_id,
            "name": self.name,
            "zone": self.zone,
            "cuisine": self.cuisine,
            "commission_pct": self.commission_pct.quantize(Decimal("0.01")),
            "is_active": self.is_active,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass
class Customer:
    customer_id: str
    zone: str
    loyalty_tier: str
    signup_ts: datetime
    updated_at: datetime

    def row(self) -> dict:
        return {
            "customer_id": self.customer_id,
            "zone": self.zone,
            "loyalty_tier": self.loyalty_tier,
            "signup_ts": self.signup_ts,
            "updated_at": self.updated_at,
        }


@dataclass
class World:
    """State of the source system at the end of a given day."""

    as_of: date
    restaurants: dict[str, Restaurant] = field(default_factory=dict)
    customers: dict[str, Customer] = field(default_factory=dict)

    @classmethod
    def build(cls, d: date) -> World:
        if d < EPOCH:
            raise ValueError(f"no data before {EPOCH}")
        w = cls(as_of=EPOCH)
        rng = _rng("world", EPOCH)
        start = _at(EPOCH, 0)
        for i in range(INITIAL_RESTAURANTS):
            w._add_restaurant(rng, i, start, EPOCH)
        for i in range(INITIAL_CUSTOMERS):
            w._add_customer(rng, i, start)
        day = EPOCH
        while day < d:
            day += timedelta(days=1)
            w._evolve(day)
        w.as_of = d
        return w

    def _add_restaurant(self, rng: random.Random, idx: int, created: datetime, listed_on: date) -> None:
        rid = f"R{idx:04d}"
        self.restaurants[rid] = Restaurant(
            restaurant_id=rid,
            name=f"Kitchen {idx:04d}",
            zone=ZONES[idx % len(ZONES)],
            cuisine=rng.choice(CUISINES),
            commission_pct=Decimal(rng.randint(15, 25)),
            is_active=True,
            created_at=created,
            updated_at=created,
            listed_on=listed_on,
        )

    def _add_customer(self, rng: random.Random, idx: int, signup: datetime) -> None:
        cid = f"C{idx:06d}"
        self.customers[cid] = Customer(cid, rng.choice(ZONES), "BRONZE", signup, signup)

    def _evolve(self, day: date) -> None:
        rng = _rng("world", day)
        day_start = _at(day, 0)

        # New restaurant, sometimes listed in the catalogue a day late.
        if rng.random() < 0.35:
            created = _random_ts(rng, day, 7, 11)
            listed_on = day + timedelta(days=1) if rng.random() < 0.5 else day
            self._add_restaurant(rng, len(self.restaurants), created, listed_on)

        # Restaurant attribute changes (early morning, before most orders).
        existing = [r for r in self.restaurants.values() if r.created_at < day_start]
        for _ in range(rng.randint(1, 3)):
            r = rng.choice(existing)
            roll = rng.random()
            if roll < 0.6:
                r.commission_pct = Decimal(min(30, max(12, int(r.commission_pct) + rng.choice([-3, -2, -1, 1, 2, 3]))))
            elif roll < 0.85:
                r.cuisine = rng.choice([c for c in CUISINES if c != r.cuisine])
            else:
                r.is_active = not r.is_active
            r.updated_at = _random_ts(rng, day, 0, 6)

        # New customers.
        base = len(self.customers)
        for i in range(rng.randint(15, 30)):
            self._add_customer(rng, base + i, _random_ts(rng, day))

        # Customer changes: loyalty upgrades and moving zone.
        existing_c = [c for c in self.customers.values() if c.signup_ts < day_start]
        for _ in range(rng.randint(8, 20)):
            c = rng.choice(existing_c)
            if rng.random() < 0.7 and c.loyalty_tier != TIERS[-1]:
                c.loyalty_tier = TIERS[TIERS.index(c.loyalty_tier) + 1]
            else:
                c.zone = rng.choice([z for z in ZONES if z != c.zone])
            c.updated_at = _random_ts(rng, day)


# ---------------------------------------------------------------------- facts
@dataclass
class OrderVersion:
    status: str
    updated_at: datetime
    delivered_ts: datetime | None


@dataclass
class Order:
    order_id: str
    customer_id: str
    restaurant_id: str
    order_ts: datetime
    items_count: int
    gross_amount: Decimal
    discount_amount: Decimal
    delivery_fee: Decimal
    payment_method: str | None
    versions: list[OrderVersion]

    def row(self, v: OrderVersion) -> dict:
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "restaurant_id": self.restaurant_id,
            "order_ts": self.order_ts,
            "status": v.status,
            "items_count": self.items_count,
            "gross_amount": self.gross_amount,
            "discount_amount": self.discount_amount,
            "delivery_fee": self.delivery_fee,
            "payment_method": self.payment_method,
            "delivered_ts": v.delivered_ts,
            "updated_at": v.updated_at,
        }


def orders_placed_on(x: date, world: World | None = None) -> list[Order]:
    """All orders placed on day ``x`` with their full version history."""
    world = world or World.build(x)
    rng = _rng("orders", x)
    day_start = _at(x, 0)

    customers = [c.customer_id for c in world.customers.values() if c.signup_ts < day_start]
    customer_zone = {c.customer_id: c.zone for c in world.customers.values()}
    active = [r for r in world.restaurants.values() if r.is_active]

    weekend_boost = 1.3 if x.weekday() >= 4 else 1.0  # Fri-Sun
    n = int(BASE_ORDERS_PER_DAY * weekend_boost * rng.uniform(0.9, 1.1))
    hours = rng.choices(range(24), weights=HOUR_WEIGHTS, k=n)
    order_times = sorted(_at(x, h * 3600 + rng.uniform(0, 3599)) for h in hours)

    orders = []
    for i, ts in enumerate(order_times):
        cid = rng.choice(customers)
        open_now = [r for r in active if r.created_at < ts]
        same_zone = [r for r in open_now if r.zone == customer_zone[cid]]
        rest = rng.choice(same_zone if same_zone and rng.random() < 0.7 else open_now)

        items = rng.randint(1, 5)
        gross = sum(rng.randint(9_000, 42_000) for _ in range(items))
        discount = int(gross * rng.uniform(0.10, 0.20)) if rng.random() < 0.3 else 0
        payment = rng.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
        if rng.random() < 0.002:
            payment = None  # missing in source

        versions = [OrderVersion("PLACED", ts, None)]
        if rng.random() < 0.04:
            versions.append(OrderVersion("CANCELLED", ts + timedelta(minutes=rng.randint(2, 10)), None))
        else:
            dispatched = ts + timedelta(minutes=rng.randint(8, 15))
            delivered = dispatched + timedelta(minutes=rng.randint(10, 45))
            versions.append(OrderVersion("OUT_FOR_DELIVERY", dispatched, None))
            versions.append(OrderVersion("DELIVERED", delivered, delivered))
            if rng.random() < 0.02:
                refund_day = x + timedelta(days=rng.choice([1, 2]))
                versions.append(OrderVersion("REFUNDED", _random_ts(rng, refund_day, 9, 21), delivered))

        orders.append(Order(
            order_id=f"O{x:%Y%m%d}{i:05d}",
            customer_id=cid,
            restaurant_id=rest.restaurant_id,
            order_ts=ts,
            items_count=items,
            gross_amount=_money(gross),
            discount_amount=_money(discount),
            delivery_fee=Decimal(rng.choice([0, 25, 35, 49])).quantize(Decimal("0.01")),
            payment_method=payment,
            versions=versions,
        ))
    return orders


# ------------------------------------------------------------------- extracts
def restaurants_extract(d: date, world: World | None = None) -> list[dict]:
    world = world or World.build(d)
    return [r.row() for r in world.restaurants.values() if r.listed_on <= d]


def customers_extract(d: date, world: World | None = None) -> list[dict]:
    world = world or World.build(d)
    return [c.row() for c in world.customers.values()]


def orders_extract(d: date) -> list[dict]:
    """Incremental extract: for every order that changed on ``d``, its state at
    the end of ``d``. Looks back two days for late updates (refunds)."""
    rows = []
    for back in (2, 1, 0):
        x = d - timedelta(days=back)
        if x < EPOCH:
            continue
        for o in orders_placed_on(x):
            changed_today = [v for v in o.versions if v.updated_at.date() == d]
            if changed_today:
                rows.append(o.row(changed_today[-1]))

    dup_rng = _rng("duplicates", d)
    with_dupes = []
    for r in rows:
        with_dupes.append(r)
        if dup_rng.random() < 0.003:
            with_dupes.append(dict(r))  # exact duplicate, as a flaky export would produce
    return with_dupes


def extract_day(d: date) -> dict[str, list[dict]]:
    """All three extracts for day ``d``; each row carries ``_extract_date``."""
    world = World.build(d)
    out = {
        "restaurants": restaurants_extract(d, world),
        "customers": customers_extract(d, world),
        "orders": orders_extract(d),
    }
    for rows in out.values():
        for r in rows:
            r["_extract_date"] = d
    return out
