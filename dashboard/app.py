"""Pipewright dashboard: BazaarBox's five CEO reports, plus a Data health tab with the AI incident explainer.

    PIPEWRIGHT_WAREHOUSE=warehouse/pipewright.duckdb streamlit run dashboard/app.py --server.port 18142
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import data

from pipewright import config, explainer

st.set_page_config(page_title="BazaarBox · Pipewright", page_icon="📦", layout="wide")
settings = config.load()

st.title("BazaarBox — daily business dashboard")
if not settings.warehouse.exists():
    st.warning(f"No published data yet ({settings.warehouse.name} not found). Run `pipewright run --date YYYY-MM-DD` first.")
    st.stop()

con = data.connect(settings.warehouse)
s = data.status(con, settings.reports_dir)
age = s.age_days()
st.caption(f"Data as of **{s.run_date}** (IST business day) · built by the {s.track} track · contract {s.contract_version}"
           + (f" · {age} day(s) old" if age is not None else ""))
if s.last_run_blocked:
    st.error(f"The latest run ({s.latest_report['run_date']}) failed its data-quality checks, so it was NOT published. "
             f"You are looking at the last good data ({s.run_date}). See the Data health tab.")

revenue_tab, cac_tab, mrr_tab, top_tab, cohort_tab, health_tab = st.tabs(
    ["Revenue", "CAC by channel", "Prime MRR", "Top 10 products (30d)", "30-day retention", "Data health"]
)

with revenue_tab:
    c1, c2 = st.columns(2)
    regions = c1.multiselect("Region", data.dimension_values(con, "region"))
    categories = c2.multiselect("Category", data.dimension_values(con, "category"))
    rev = data.revenue_daily(con, regions, categories)
    m1, m2, m3 = st.columns(3)
    m1.metric("Revenue", data.rupees(int(rev["revenue_paise"].sum())))
    m2.metric("Orders", f"{int(rev['orders'].sum()):,}")
    m3.metric("Units", f"{int(rev['units'].sum()):,}")
    disc = data.discounts(con)  # 1.1: None on a 1.0 warehouse
    if disc is not None and int(disc["discount_paise"].sum()) > 0:
        d1, d2, d3 = st.columns(3)
        d1.metric("Coupon discounts", data.rupees(int(disc["discount_paise"].sum())))
        d2.metric("Net revenue (after coupons)", data.rupees(int(disc["net_paise"].sum())))
        d3.metric("Orders with a coupon", f"{int(disc['orders_with_coupon'].sum()):,}")
        st.caption("Revenue above is gross (list price x quantity); coupons started on 15 Sep 2026.")
    daily = rev.groupby("order_date", as_index=False)["revenue_paise"].sum()
    st.line_chart(daily.assign(revenue_inr=daily["revenue_paise"] / 100), x="order_date", y="revenue_inr")
    split = rev.groupby(["region", "category"], as_index=False)["revenue_paise"].sum()
    st.bar_chart(split.assign(revenue_inr=split["revenue_paise"] / 100), x="category", y="revenue_inr", color="region")

with cac_tab:
    cac = data.cac_monthly(con)
    st.dataframe(cac.assign(spend=cac["spend_paise"].map(data.rupees), cac=cac["cac_paise"].map(data.rupees))
                 [["month", "channel", "spend", "new_customers", "cac"]], hide_index=True, width="stretch")
    st.caption("CAC = marketing spend ÷ new customers in the month of their first valid order (rounded down). "
               "Channels with no new customers show —.")

with mrr_tab:
    mrr = data.mrr_monthly(con)
    if len(mrr):
        latest = mrr.iloc[-1]
        k1, k2, k3 = st.columns(3)
        k1.metric("MRR", data.rupees(int(latest["mrr_paise"])))
        k2.metric("Active members", int(latest["active_subscriptions"]))
        k3.metric("Churned MRR (month)", data.rupees(int(latest["churned_mrr_paise"])))
        st.line_chart(mrr.assign(mrr_inr=mrr["mrr_paise"] / 100), x="month", y="mrr_inr")
    st.dataframe(mrr, hide_index=True, width="stretch")

with top_tab:
    top = data.top_products(con)
    st.dataframe(top.assign(revenue=top["revenue_paise"].map(data.rupees))[["rank", "sku", "product_name", "category", "units", "revenue"]],
                 hide_index=True, width="stretch")

with cohort_tab:
    coh = data.cohorts(con)
    st.dataframe(coh.assign(retention=coh["retention_rate"].map(lambda r: "—" if r != r else f"{r:.1%}")),  # noqa: PLR0124 (NaN check)
                 hide_index=True, width="stretch")
    st.caption("Share of mature customers (first order 30+ days ago) who ordered again within 30 days.")

with health_tab:
    h = data.health(settings.warehouse.parent / "ops.duckdb")  # 1.1 monitoring; None before the first monitored run
    if h is not None:
        failing = h.checks[h.checks["status"] == "fail"]
        st.subheader("Freshness, volume and SLA" + (f" for {h.checks['run_date'].iloc[0]:%Y-%m-%d}" if len(h.checks) else ""))
        if len(failing):
            st.warning(f"{len(failing)} monitoring check(s) failing: " + ", ".join(failing["check_id"]))
        st.dataframe(h.checks, hide_index=True, width="stretch")
        if len(h.dq_trend):
            st.subheader("Data-quality trend")
            st.line_chart(h.dq_trend, x="run_date", y=["passed", "blocking", "warnings"])
        st.subheader("Recent runs")
        st.dataframe(h.runs, hide_index=True, width="stretch")
        if len(h.alerts):
            st.subheader("Recent alerts")
            st.dataframe(h.alerts, hide_index=True, width="stretch")
    st.subheader("Data-quality checks of the published build")
    st.dataframe(s.checks, hide_index=True, width="stretch")
    if s.latest_report:
        st.subheader(f"Latest run: {s.latest_report['run_date']}")
        if st.button("Explain the latest run", key="explain"):
            e = explainer.explain(s.latest_report)
            st.markdown(e.text)
            st.caption(f"Source: {e.source} ({e.reason}). Only the numbered facts below were used.")
            st.markdown("\n".join(f"{i}. {f}" for i, f in enumerate(e.facts, 1)))
con.close()
