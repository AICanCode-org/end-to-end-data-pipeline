"""Seeded generator for BazaarBox's source data.

The whole world (customers, orders with their status history, Prime subscriptions, marketing spend, the product
catalogue) is generated day by day with one independent random stream per entity, then cut at the release horizon.
That is why the v1.1 data for June-August is identical to the v1.0 data: extending the horizon only adds days.

Output layout (``--out source-data``)::

    api/orders.jsonl         one line per order with its full status history (served by the API stub)
    api/customers.jsonl      one line per customer with its profile versions
    drops/payments/payments_YYYY-MM-DD.jsonl
    drops/marketing_spend/spend_YYYY-MM-DD.csv
    drops/products/products_YYYY-MM-DD.csv
    late/...                 files that "arrive late" (v1.1 drill only)
    MANIFEST.json            sha256 of every file above (``--check`` compares against contract/source-manifest-*.json)
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import random
import sys
from pathlib import Path

SEED = 20260601
LAUNCH = dt.date(2026, 6, 1)
HORIZONS = {"1.0": dt.date(2026, 8, 31), "1.1": dt.date(2026, 9, 30)}
WORLD_END = dt.date(2026, 9, 30)
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
UTC = dt.UTC
SCHEMA_CHANGE = dt.date(2026, 9, 15)  # orders gain coupon_code/discount_paise, spend CSV gains campaign_id
RTO_FROM = dt.date(2026, 9, 1)  # new order status "rto" (return to origin)
INFLUENCER_FROM = dt.date(2026, 9, 1)  # new acquisition/marketing channel
LATE_SPEND_FILE = dt.date(2026, 9, 20)  # this spend file arrives one day late (v1.1 drill)

# raw spellings per city (what customers type), canonical city, region
CITIES = [
    ("Bengaluru", "South", ["Bengaluru", "Bangalore", "bengaluru ", "BLR"], 0.24),
    ("Pune", "West", ["Pune", "pune", "Poona"], 0.14),
    ("Hyderabad", "South", ["Hyderabad", "Hyd", "hyderabad"], 0.16),
    ("Delhi", "North", ["New Delhi", "Delhi", "delhi "], 0.20),
    ("Jaipur", "North", ["Jaipur", "JAIPUR"], 0.08),
    ("Kolkata", "East", ["Kolkata", "Calcutta"], 0.10),
    ("Kochi", "South", ["Kochi", "Cochin"], 0.08),
]
UNKNOWN_CITY = "Ooty"  # a spelling the city map does not know (rare)
CHANNELS = ["google_ads", "meta_ads", "affiliates", "referral", "organic"]
CHANNEL_WEIGHTS = [0.30, 0.25, 0.10, 0.10, 0.25]
CATEGORIES = [
    ("kitchen", ["Kitchen", "kitchen", " Kitchen"]),
    ("cookware", ["Cookware", "cookware"]),
    ("storage", ["Storage", "storage "]),
    ("cleaning", ["Cleaning", "cleaning"]),
    ("decor", ["Decor", "decor", "Décor"]),
    ("appliances", ["Appliances", "appliances"]),
]
PLANS = {"prime_monthly": 14900, "prime_annual": 142800}
UNKNOWN_SKU = "SKU-9999"


def rng(*parts: object) -> random.Random:
    """An independent, reproducible random stream for one entity (string seeds hash with SHA-512 in CPython)."""
    return random.Random("|".join(str(p) for p in (SEED, *parts)))


def days(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def ist(day: dt.date, seconds: int) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(), IST) + dt.timedelta(seconds=seconds)


def iso(ts: dt.datetime, *key: object) -> str:
    """Mixed offsets on purpose: about 30 % of timestamps are written in UTC ('Z'), the rest in IST (+05:30).
    The choice depends only on ``key`` (entity + field), so it never shifts when the horizon changes."""
    if hashlib.sha256("|".join(map(str, key)).encode()).digest()[0] < 77:
        return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return ts.astimezone(IST).strftime("%Y-%m-%dT%H:%M:%S+05:30")


def ist_date(ts: dt.datetime) -> dt.date:
    return ts.astimezone(IST).date()


def end_of(day: dt.date) -> dt.datetime:
    return dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), IST) - dt.timedelta(seconds=1)


def add_months(day: dt.date, n: int) -> dt.date:
    m = day.month - 1 + n
    y, m = day.year + m // 12, m % 12 + 1
    last = [31, 29 if y % 4 == 0 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]
    return dt.date(y, m, min(day.day, last))


# ---------------------------------------------------------------------------------------------------------------
# products
def build_products() -> list[dict]:
    r = rng("products")
    names = {
        "kitchen": ["Spice Box", "Chopping Board", "Tiffin Set", "Masala Grinder Jar", "Tea Strainer", "Knife Set"],
        "cookware": ["Kadai", "Tawa", "Pressure Cooker", "Idli Stand", "Saucepan", "Appam Patra"],
        "storage": ["Steel Dabba Set", "Glass Jar", "Rice Bin", "Fridge Organiser", "Lunch Bag", "Vacuum Flask"],
        "cleaning": ["Microfibre Cloth", "Mop Set", "Dish Rack", "Scrub Pack", "Broom", "Bucket"],
        "decor": ["Brass Diya", "Cushion Cover", "Wall Clock", "Table Runner", "Planter", "Photo Frame"],
        "appliances": ["Mixer Grinder", "Electric Kettle", "Induction Cooktop", "Hand Blender", "Toaster", "Iron"],
    }
    products = []
    n = 0
    for cat, spellings in CATEGORIES:
        for base in names[cat]:
            for variant in ("", " Pro", " Mini"):
                n += 1
                price = r.choice([199, 249, 299, 349, 399, 499, 599, 699, 799, 999, 1299, 1499, 1999, 2499, 2999])
                if cat == "appliances":
                    price += 1000
                launch = LAUNCH if n % 9 else LAUNCH + dt.timedelta(days=r.randint(20, 100))
                changes = []
                if r.random() < 0.12:
                    when = LAUNCH + dt.timedelta(days=r.randint(15, 110))
                    changes.append((when, price + r.choice([-100, -50, 50, 100, 200])))
                products.append(
                    {
                        "sku": f"SKU-{n:04d}",
                        "name": f"{base}{variant}",
                        "category_raw": r.choice(spellings),
                        "category": cat,
                        "price": price,
                        "launch": launch,
                        "changes": changes,
                        "weight": 1 / (1 + (n * 7919 % 108)) ** 0.6,
                    }
                )
    return products


def price_on(p: dict, day: dt.date) -> int:
    price = p["price"]
    for when, new in p["changes"]:
        if day >= when:
            price = new
    return price


# ---------------------------------------------------------------------------------------------------------------
# customers, orders, subscriptions
def signups_on(day: dt.date) -> int:
    r = rng("signups", day)
    t = (day - LAUNCH).days
    base = 38 + 0.25 * t + (8 if day.weekday() >= 5 else 0)
    return max(5, int(r.gauss(base, 6)))


def build_world():
    products = build_products()
    customers, orders, subs = [], [], []
    cid = 0
    for day in days(LAUNCH, WORLD_END):
        for _ in range(signups_on(day)):
            cid += 1
            c = make_customer(cid, day)
            customers.append(c)
            c_orders, c_sub = make_orders(c, products)
            orders.extend(c_orders)
            if c_sub:
                subs.append(c_sub)
    orders.sort(key=lambda o: (o["created"], o["customer_id"], o["n"]))
    for i, o in enumerate(orders, 1):  # ids in creation order, independent of the horizon
        o["order_id"] = f"O-{i:07d}"
    return products, customers, orders, subs


def make_customer(cid: int, day: dt.date) -> dict:
    r = rng("customer", cid)
    city = r.choices(CITIES, weights=[c[3] for c in CITIES])[0]
    channels, weights = list(CHANNELS), list(CHANNEL_WEIGHTS)
    if day >= INFLUENCER_FROM:
        channels.append("influencer")
        weights.append(0.12)
    signup = ist(day, r.randint(7 * 3600, 23 * 3600))
    versions = [{"city": r.choice(city[2]), "at": signup}]
    if r.random() < 0.05:  # moved city later: the profile reappears in a later window
        other = r.choice(CITIES)
        versions.append({"city": r.choice(other[2]), "at": signup + dt.timedelta(days=r.randint(3, 60), seconds=r.randint(0, 80000))})
    return {
        "customer_id": f"C-{cid:06d}",
        "signup": signup,
        "channel": r.choices(channels, weights=weights)[0],
        "is_test": r.random() < 0.006,
        "versions": versions,
        "repeat_gap": r.choice([12, 18, 25, 35, 50, 80]),
        "city": city,
    }


def order_time(r: random.Random, day: dt.date) -> dt.datetime:
    if r.random() < 0.06:  # night owls: after midnight IST is still "yesterday" in UTC
        return ist(day, r.randint(0, 2 * 3600))
    return ist(day, r.randint(8 * 3600, 23 * 3600 + 3599))


def make_orders(c: dict, products: list[dict]):
    r = rng("orders", c["customer_id"])
    out, sub = [], None
    if r.random() > 0.85:
        return out, sub
    first = c["signup"].date() + dt.timedelta(days=r.choice([0, 0, 0, 1, 2, 3, 5, 7]))
    when = order_time(r, first)
    if when < c["signup"]:
        when = c["signup"] + dt.timedelta(minutes=r.randint(5, 90))
    n = 0
    while ist_date(when) <= WORLD_END:
        n += 1
        out.append(make_order(c, n, when, products))
        if n == 1 and r.random() < 0.09:
            sub = make_subscription(c, when)
        gap = max(1, int(r.expovariate(1 / c["repeat_gap"])))
        if r.random() < 0.35 and n == 1:
            gap = r.randint(3, 30)  # many second orders come within a month
        when = order_time(r, ist_date(when) + dt.timedelta(days=gap))
        if n >= 12:
            break
    return out, sub


def make_order(c: dict, n: int, created: dt.datetime, products: list[dict]) -> dict:
    r = rng("order", c["customer_id"], n)
    day = ist_date(created)
    live = [p for p in products if p["launch"] <= day]
    items = []
    for _ in range(r.choices([1, 2, 3, 4], weights=[0.45, 0.30, 0.17, 0.08])[0]):
        if r.random() < 0.005:
            items.append({"sku": UNKNOWN_SKU, "qty": 1, "unit_price_paise": 39900})
            continue
        p = r.choices(live, weights=[q["weight"] for q in live])[0]
        qty = r.choices([1, 2, 3], weights=[0.75, 0.2, 0.05])[0]
        if r.random() < 0.003:
            qty = r.choice([0, -1])
        items.append({"sku": p["sku"], "qty": qty, "unit_price_paise": price_on(p, day) * 100})
    city = c["city"] if n == 1 or r.random() > 0.05 else r.choice(CITIES)
    ship_city = UNKNOWN_CITY if r.random() < 0.004 else r.choice(city[2])
    gross = sum(max(i["qty"], 0) * i["unit_price_paise"] for i in items)
    order = {
        "customer_id": c["customer_id"],
        "n": n,
        "created": created,
        "ship_city": ship_city,
        "payment_method": "prepaid" if r.random() < 0.62 else "cod",
        "items": items,
        "is_test": c["is_test"],
        "events": status_events(r, created),
        "coupon": None,
        "discount": 0,
    }
    if day >= SCHEMA_CHANGE:
        coupon = r.choices([None, "FEST10", "WELCOME50"], weights=[0.7, 0.2, 0.1])[0]
        order["coupon"] = coupon
        order["discount"] = {None: 0, "FEST10": gross // 10, "WELCOME50": min(5000, gross)}[coupon]
    order["gross"] = gross
    return order


def status_events(r: random.Random, created: dt.datetime) -> list[tuple[str, dt.datetime]]:
    ev = [("placed", created)]
    if r.random() < 0.06:
        ev.append(("cancelled", created + dt.timedelta(minutes=r.randint(10, 360))))
        return ev
    shipped = created + dt.timedelta(hours=r.randint(12, 36), minutes=r.randint(0, 59))
    ev.append(("shipped", shipped))
    if ist_date(created) >= RTO_FROM and r.random() < 0.05:
        ev.append(("rto", shipped + dt.timedelta(days=r.randint(3, 6), minutes=r.randint(0, 600))))
        return ev
    delivered = shipped + dt.timedelta(days=r.randint(1, 4), minutes=r.randint(0, 600))
    ev.append(("delivered", delivered))
    if r.random() < 0.05:
        ev.append(("returned", delivered + dt.timedelta(days=r.randint(2, 6), minutes=r.randint(0, 600))))
    return ev


def make_subscription(c: dict, start: dt.datetime) -> dict:
    r = rng("subscription", c["customer_id"])
    plan = "prime_monthly" if r.random() < 0.7 else "prime_annual"
    charges, cancel = [], None
    period_start = ist_date(start)
    at = start + dt.timedelta(minutes=2)
    while period_start <= WORLD_END:
        period_end = add_months(period_start, 1 if plan == "prime_monthly" else 12)
        charges.append((at, period_start, period_end))
        if plan == "prime_monthly" and r.random() < 0.18:
            cancel = ist(period_start + dt.timedelta(days=r.randint(1, 25)), r.randint(9 * 3600, 22 * 3600))
            break
        if plan == "prime_annual":
            break
        period_start = period_end
        at = ist(period_start, r.randint(3600, 5 * 3600))
    return {"subscription_id": "S-" + c["customer_id"][2:], "customer_id": c["customer_id"], "plan": plan, "charges": charges, "cancel": cancel}


# ---------------------------------------------------------------------------------------------------------------
# writers
def event_id(*parts: object) -> str:
    return "evt_" + hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:20]


def payment_events(orders: list[dict], subs: list[dict], horizon_end: dt.datetime) -> list[dict]:
    events = []
    for o in orders:
        if o["is_test"] or o["payment_method"] != "prepaid":
            continue
        k = o["order_id"]
        amount = o["gross"] - o["discount"]
        cap = o["created"] + dt.timedelta(seconds=40)
        events.append(
            {"event_id": event_id(o["order_id"], "cap"), "type": "payment.captured", "customer_id": o["customer_id"],
             "order_id": o["order_id"], "subscription_id": None, "plan": None, "amount_paise": amount, "currency": "INR",
             "occurred_at": iso(cap, k, "cap"), "period_start": None, "period_end": None, "_at": cap}
        )
        for status, at in o["events"]:
            if status in ("cancelled", "returned", "rto") and at <= horizon_end:
                at = at + dt.timedelta(minutes=30)
                events.append(
                    {"event_id": event_id(o["order_id"], "ref"), "type": "refund.succeeded",
                     "customer_id": o["customer_id"], "order_id": o["order_id"], "subscription_id": None, "plan": None,
                     "amount_paise": amount, "currency": "INR", "occurred_at": iso(at, k, "ref"), "period_start": None,
                     "period_end": None, "_at": at}
                )
    for s in subs:
        k = s["subscription_id"]
        for at, ps, pe in s["charges"]:
            events.append(
                {"event_id": event_id(s["subscription_id"], ps), "type": "subscription.charged",
                 "customer_id": s["customer_id"], "order_id": None, "subscription_id": s["subscription_id"],
                 "plan": s["plan"], "amount_paise": PLANS[s["plan"]], "currency": "INR", "occurred_at": iso(at, k, ps),
                 "period_start": ps.isoformat(), "period_end": pe.isoformat(), "_at": at}
            )
        if s["cancel"]:
            events.append(
                {"event_id": event_id(s["subscription_id"], "cancel"), "type": "subscription.cancelled",
                 "customer_id": s["customer_id"], "order_id": None, "subscription_id": s["subscription_id"],
                 "plan": s["plan"], "amount_paise": 0, "currency": "INR", "occurred_at": iso(s["cancel"], k, "cancel"),
                 "period_start": None, "period_end": None, "_at": s["cancel"]}
            )
    return [e for e in events if e["_at"] <= horizon_end]


def spend_text(paise: int, r: random.Random) -> str:
    rupees, p = divmod(paise, 100)
    style = r.random()
    if style < 0.5:
        return f"{rupees}.{p:02d}"
    if style < 0.8:
        return f"₹{rupees:,}.{p:02d}"
    frac = f"{p:02d}".rstrip("0")  # "12,345.5" or "12,345"
    return f"{rupees:,}" + (f".{frac}" if frac else "")


def spend_rows(day: dt.date, new_by_channel: dict[str, int]) -> list[tuple[str, int]]:
    r = rng("spend", day)
    cpa = {"google_ads": 42000, "meta_ads": 36000, "affiliates": 30000, "referral": 15000, "influencer": 52000}
    rows = []
    for ch in CHANNELS + (["influencer"] if day >= INFLUENCER_FROM else []):
        if ch == "organic":
            rows.append((ch, 0))
            continue
        n = new_by_channel.get(ch, 0)
        rows.append((ch, max(0, int(n * cpa[ch] * r.uniform(0.7, 1.3) + r.randint(0, 99999)))))
    return rows


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def generate(out: Path, version: str = "1.0", start: dt.date = LAUNCH, end: dt.date | None = None) -> dict:
    """Write the source data for a release (``version``) into ``out``; ``start``/``end`` narrow the drop files and
    the API horizon (used for the tiny CI fixture). Returns the manifest {relative path: sha256}."""
    horizon = min(end or HORIZONS[version], HORIZONS[version])
    horizon_end = end_of(horizon)
    products, customers, orders, subs = build_world()
    first = [o for o in orders if ist_date(o["created"]) >= start]
    orders = [o for o in first if o["created"] <= horizon_end]
    customers = [c for c in customers if start <= c["signup"].date() <= horizon]
    keep = {c["customer_id"] for c in customers}
    orders = [o for o in orders if o["customer_id"] in keep]
    subs = [s for s in subs if s["customer_id"] in keep]
    out.mkdir(parents=True, exist_ok=True)

    api_orders = []
    for o in orders:
        events = [(s, at) for s, at in o["events"] if at <= horizon_end]
        k = o["order_id"]
        rec = {
            "order_id": o["order_id"],
            "customer_id": o["customer_id"],
            "created_at": iso(o["created"], k, "created"),
            "ship_city": o["ship_city"],
            "payment_method": o["payment_method"],
            "is_test": o["is_test"],
            "items": o["items"],
            "history": [{"status": s, "at": iso(at, k, s)} for s, at in events],
        }
        if ist_date(o["created"]) >= SCHEMA_CHANGE:
            rec["coupon_code"] = o["coupon"]
            rec["discount_paise"] = o["discount"]
        api_orders.append(rec)
    write_jsonl(out / "api" / "orders.jsonl", api_orders)

    api_customers = []
    for c in customers:
        k = c["customer_id"]
        api_customers.append(
            {
                "customer_id": c["customer_id"],
                "signup_at": iso(c["signup"], k, "signup"),
                "acquisition_channel": c["channel"],
                "is_test": c["is_test"],
                "email": f"user{c['customer_id'][2:]}@example.{'test' if c['is_test'] else 'com'}",
                "phone": f"+91-9{int(c['customer_id'][2:]) % 1000000000:09d}",
                "versions": [{"city": v["city"], "at": iso(v["at"], k, i)} for i, v in enumerate(c["versions"]) if v["at"] <= horizon_end],
            }
        )
    write_jsonl(out / "api" / "customers.jsonl", api_customers)

    # payments: one file per IST day; ~1.5 % duplicated in the same file, ~0.5 % re-delivered in the next file
    pay = sorted(payment_events(orders, subs, horizon_end), key=lambda e: (e["_at"], e["event_id"]))
    by_day: dict[dt.date, list[dict]] = {d: [] for d in days(start, horizon)}
    for e in pay:
        d = ist_date(e.pop("_at"))
        r = rng("dup", e["event_id"])
        by_day.setdefault(d, []).append(e)
        if r.random() < 0.015:
            by_day[d].append(dict(e))
        elif r.random() < 0.005 and d + dt.timedelta(days=1) <= horizon:
            by_day.setdefault(d + dt.timedelta(days=1), []).append(dict(e))
    for d, rows in by_day.items():
        write_jsonl(out / "drops" / "payments" / f"payments_{d}.jsonl", rows)

    # marketing spend: one CSV per day; campaign_id column from the schema change; one file arrives late
    new_by_day: dict[dt.date, dict[str, int]] = {}
    for c in customers:
        d = c["signup"].date()
        new_by_day.setdefault(d, {}).setdefault(c["channel"], 0)
        new_by_day[d][c["channel"]] += 1
    for d in days(start, horizon):
        r = rng("spendfmt", d)
        header = "date,channel,spend,currency" + (",campaign_id" if d >= SCHEMA_CHANGE else "")
        lines = [header]
        for ch, paise in spend_rows(d, new_by_day.get(d, {})):
            text = spend_text(paise, r)
            text = f'"{text}"' if "," in text else text
            row = f"{d},{ch},{text},INR" + (f",{ch[:3].upper()}-{d:%m%d}" if d >= SCHEMA_CHANGE else "")
            lines.append(row)
        folder = "late" if d == LATE_SPEND_FILE else "drops"
        write_text(out / folder / "marketing_spend" / f"spend_{d}.csv", "\n".join(lines) + "\n")

    # product catalogue: a full snapshot every day
    for d in days(start, horizon):
        lines = ["sku,name,category,price_inr,active"]
        for p in products:
            if p["launch"] <= d:
                lines.append(f"{p['sku']},{p['name']},{p['category_raw']},{price_on(p, d)}.00,true")
        write_text(out / "drops" / "products" / f"products_{d}.csv", "\n".join(lines) + "\n")

    manifest = {
        str(p.relative_to(out)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(out.rglob("*"))
        if p.is_file() and p.name != "MANIFEST.json"
    }
    meta = {"generator": "bazaarbox_source", "seed": SEED, "version": version, "start": str(start), "end": str(horizon)}
    (out / "MANIFEST.json").write_text(json.dumps({**meta, "files": manifest}, indent=1, sort_keys=True) + "\n")
    return {**meta, "files": manifest}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m bazaarbox_source generate", description=__doc__.split("\n")[0])
    ap.add_argument("--out", default="source-data", type=Path)
    ap.add_argument("--version", default="1.0", choices=sorted(HORIZONS))
    ap.add_argument("--start", type=dt.date.fromisoformat, default=LAUNCH)
    ap.add_argument("--end", type=dt.date.fromisoformat, default=None)
    ap.add_argument("--check", type=Path, metavar="MANIFEST", help="exit 1 unless the output matches this manifest")
    args = ap.parse_args(argv)
    got = generate(args.out, args.version, args.start, args.end)
    if args.check:
        want = json.loads(args.check.read_text())
        if want != got:
            diff = sorted(k for k in set(want["files"]) | set(got["files"]) if want["files"].get(k) != got["files"].get(k))
            print(f"MISMATCH: {len(diff)} file(s) differ from {args.check}: {diff[:5]}", file=sys.stderr)
            return 1
        print(f"OK: {len(got['files'])} files identical to {args.check}")
    else:
        print(f"wrote {len(got['files'])} files to {args.out} (version {args.version}, {got['start']}..{got['end']})")
    return 0
