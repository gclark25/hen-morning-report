#!/usr/bin/env python3
"""Build dashboard/brief_feed.json -- the small, market-data-only feed for the morning brief.

    python3 build_brief_feed.py                      latest.json -> dashboard/brief_feed.json
    python3 build_brief_feed.py --in dashboard/latest.json --history dashboard/history.json \
                                --out /tmp/brief_feed.json

Why it exists: latest.json is too large for the brief's fetch tool, so it arrives cut off.
This file carries only what the brief needs and stays under 50,000 characters.

Sections (existing keys first, in this order, then the additive ones):
    schema_version .. as_spreads   freshness block, hourly rows, daily rows, AS spreads
    constraints   top 8 binding constraints by avg shadow price, with the 3 most-affected nodes
    weather       next 4 days (after data_date) of high temp and precip % per ERCOT metro
    nodes         per settlement point: Sharpe signal, mean DART, MCC, RT avg/max/min, DART, then
                  top_constraints (the 2 constraints with the largest |MCC| AT THAT NODE, from every
                  constraint in latest.json, not just the 8 above), hen_name, region
                  (and hen_name_confirmed:false for the one name that is an unconfirmed assumption)
    hubs          per ERCOT hub / load zone: RT avg/max/min and DA avg
    data_flags    machine-readable "is this section empty / degraded" switches
    freshness     expected_data_date, is_fresh, stale_reasons: one answer to "can I use this today?"
    constraints[].peak_hour_ending is the explicit hour-ending (1-24) of peak_hour (see build_constraints)

Not included: system RT lambda. The pipeline does not pull it (ERCOT publishes it as
np6-322-cd/sced_system_lambda); adding it means a new collector, so it is left out rather than faked.

MARKET DATA ONLY. The script reads a fixed whitelist of latest.json sections and never
copies anything else, so HEN revenue, dispatch actuals, state of charge and performance
cannot reach the feed:
    data_date, gross_load_hourly, wind_hourly, solar_hourly   (yesterday's actuals, GW)
    ercot_forecasts, bid_close_forecast                       (system forecasts, GW)
    as_prices                                                 (system AS clearing prices)
    constraints, node_mcc_summary                             (ERCOT shadow prices; MCC per node)
    weather                                                   (AG2 metro forecasts)
    rt, da, dart, sharpe_ratios                               (settlement point prices / spreads)
    hub_zone_rt, hub_zone_da                                  (hub and load zone prices)
    regions                                                   (the dashboard's group names -> node lists)
    history.json: only whether the newest entry's regional block is empty (a boolean)
Nothing from modo, asset_status or history's battery / fleet blocks is ever read.

Refresh: rewritten every time the pipeline runs (daily at 6:00 AM Central via the Cloudflare
trigger, plus any manual run). It does not update intraday on its own.

Rules: GW fields rounded to 1 decimal; AS spreads, prices, shadow prices and MCC to 2; Sharpe
to 3 (it is a ratio, not a price); null, never 0 / NaN / Infinity, for missing values; same shape
every day. Size: if the file would pass 50,000 characters (measured as the file AND as
json.dumps of it, which is how the brief measures) weather is cut to 3 days first, then
constraints to 6, then (last resort, so size can never stop the daily feed) each node's
top_constraints to 1. A failure while building any additive section is logged as a warning and
that section is left empty; it never stops the existing sections from being written. Atomic
write: on any failure the previous file is left untouched and the script exits 1, so a stale file is detectably stale (generated_at,
data_date) instead of half-written.

Missing-value rules (the pipeline stores 0 for "no data" in several places):
  - gross load or wind <= 0 is treated as missing; solar <= 0 between HE10 and HE17 is missing
    (night-time zeros are real); a day whose solar is zero all day is missing.
  - a series flagged in ercot_forecasts.missing_sources is null for every forecast row.
  - net_load is null unless gross, wind and solar are all present, and is computed from the
    rounded parts so gross - wind - solar always equals net exactly.

Known limit: the collectors collapse the repeated hour on the 25-hour November day into one
row. ts_ct carries the right offset for every hour present; fixing the repeated hour needs a
DSTFlag change in the collectors (next fall-back day: Nov 7, 2027).
"""
import argparse
import json
import math
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from datetime import time as dtime
from zoneinfo import ZoneInfo

CT  = ZoneInfo("America/Chicago")
UTC = timezone.utc

SCHEMA_VERSION = 1
N_DAYS         = 4                       # data_date (actuals) + today + the next two days
AS_DAYS        = 5                       # AS spread lookback shown in the feed
AS_PRODUCTS    = ["REGUP", "REGDN", "RRS", "NSPIN", "ECRS"]
DAYLIGHT_HE    = range(10, 18)           # HE10-HE17: solar is always > 0 here
MAX_BYTES      = 50_000                  # also the character limit (see fits())
CONSTRAINTS_N  = 8                       # additive sections: how much to include
CONSTRAINTS_N_TRIMMED = 6                # ... and the fallback if the file would be too big
WEATHER_DAYS  = 4
WEATHER_DAYS_TRIMMED = 3
TOP_NODES_N   = 3
TOP_CONSTRAINTS_PER_NODE = 2             # nodes[].top_constraints
TOP_CONSTRAINTS_TRIMMED  = 1             # last-resort size fallback (see render())
FRESH_AFTER = dtime(6, 0)                # freshness: the feed must be generated at/after 06:00 CT

# HEN's name for each settlement point (nodes[].hen_name). One place to edit.
HEN_NAMES = {
    "CATARINA_B1": "Catarina", "CEDRVALE_RN": "Cedarvale", "CISC_RN": "Cisco",
    "COYOTSPR_RN": "Coyote Springs", "DIBOL_RN": "Diboll", "FALFUR_RN": "Falfurrias",
    "FAULKNER_RN": "Faulkner", "FRMRSVLW_RN": "Farmersville", "FTDUNCAN_RN": "Fort Duncan",
    "GOMZ_RN": "Gomez", "GRDNE_ESR_RN": "Garden City East", "HAMI_BESS_RN": "Hamilton",
    "HOLCOMB_RN1": "Holcomb", "JDKNS_RN": "Judkins", "JUNCTION_RN": "Junction",
    "LFSTH_RN": "Lufkin South", "LONESTAR_RN": "Lonestar", "MAINLAND_RN": "Mainland",
    "MNWL_BESS_RN": "Mineral Wells East", "MV_VALV4_RN": "Val Verde", "OLNEYTN_RN": "Olney",
    "PAULN_RN": "Pauline", "PAVLOV_BT_RN": "Pavlov", "POTEETS_RN": "Poteets",
    "RTLSNAKE_BT": "Rattlesnake", "RUSSEKST_RN": "Russek Street", "SADLBACK_RN": "Saddleback",
    "SANDLAKE_RN": "Sandlake", "SBEAN_BESS": "Screwbean",
    "TOYAH_RN": "Cherry Creek",          # ASSUMPTION, NOT CONFIRMED: supplied as a guess in the request that
                                         # added this field. Confirm it, then delete TOYAH_RN from
                                         # UNCONFIRMED_HEN_NAMES below.
    "TYNAN_RN": "Tynan", "WLTC_ESR_RN": "Weil Tract",
}
UNCONFIRMED_HEN_NAMES = {"TOYAH_RN"}     # these get nodes[].hen_name_confirmed = false (and no one else does)

# nodes[].region is the dashboard's own grouping (latest.json "regions", written from REGIONS in
# hen_morning_report.py). FALLBACK_REGIONS is only used if that key is ever missing. Fort Duncan is
# reported as its own region in this feed even though the dashboard groups it under Premium.
REGION_OVERRIDES = {"FTDUNCAN_RN": "South (Fort Duncan)"}
FALLBACK_REGIONS = {
    "West Texas": ["TOYAH_RN", "SADLBACK_RN", "FAULKNER_RN", "COYOTSPR_RN", "LONESTAR_RN", "RTLSNAKE_BT",
                   "CEDRVALE_RN", "SBEAN_BESS", "GOMZ_RN", "GRDNE_ESR_RN", "JDKNS_RN", "SANDLAKE_RN"],
    "North Texas": ["OLNEYTN_RN", "DIBOL_RN", "FRMRSVLW_RN", "MNWL_BESS_RN", "LFSTH_RN", "PAULN_RN", "CISC_RN"],
    "Coastal": ["MV_VALV4_RN", "WLTC_ESR_RN", "MAINLAND_RN", "FALFUR_RN", "PAVLOV_BT_RN", "POTEETS_RN", "TYNAN_RN"],
    "Premium": ["CATARINA_B1", "HOLCOMB_RN1", "HAMI_BESS_RN", "JUNCTION_RN", "RUSSEKST_RN", "FTDUNCAN_RN"],
}
PLAUSIBLE      = {"gross_load": (10.0, 120.0), "wind": (0.0, 50.0), "solar": (0.0, 50.0)}  # GW

ROW_KEYS = ("date", "he", "ts_ct", "status", "gross_load", "wind", "solar", "net_load",
            "bc_gross_load", "bc_wind", "bc_solar", "bc_net_load")


# ── small helpers ────────────────────────────────────────────────────────────
def num(x):
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None         # NaN -> None


def r1(x):
    return None if x is None else round(x, 1) + 0.0     # + 0.0 turns -0.0 into 0.0


def r2(x):
    return None if x is None else round(x, 2) + 0.0


def hour_exists(d, hour):
    """False for the local hour that does not exist on the spring-forward day."""
    dt = datetime(d.year, d.month, d.day, hour, tzinfo=CT)
    return dt.astimezone(UTC).astimezone(CT).hour == hour


def he_map(obj):
    """{'1': 12.3, ...} -> {1: 12.3, ...} (hour-ending keyed actuals from latest.json)."""
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            try:
                out[int(k)] = num(v)
            except (TypeError, ValueError):
                pass
    return out


def clean(kind, he, v, day_max=None):
    """Apply the missing-value rules to one value. kind: gross_load | wind | solar."""
    if v is None:
        return None
    if kind in ("gross_load", "wind"):
        return v if v > 0 else None
    if day_max is not None and day_max <= 0:               # solar zero all day = missing, not night
        return None
    if v <= 0 and he in DAYLIGHT_HE:
        return None
    return max(v, 0.0)


def net(g, w, s):
    return None if None in (g, w, s) else r1(g - w - s)


def parse_date(s):
    return date.fromisoformat(str(s)[:10])


# ── the feed ─────────────────────────────────────────────────────────────────
def build_core(latest, now_ct=None):
    """The original feed: freshness block, hourly, daily, as_spreads. Unchanged from the first
    release; the additive sections are layered on top by build()."""
    now_ct = now_ct or datetime.now(CT)
    if not isinstance(latest, dict) or not latest.get("data_date"):
        raise ValueError("latest.json has no data_date")
    data_date = parse_date(latest["data_date"])
    dates = [data_date + timedelta(days=i) for i in range(N_DAYS)]

    ef  = latest.get("ercot_forecasts") or {}
    fc  = ef.get("hourly_24hr") or {}
    bc  = (latest.get("bid_close_forecast") or {}).get("hourly") or {}
    ap  = latest.get("as_prices") or {}

    # actuals (yesterday): {he: GW}
    gl_act, wind_act, sol_act = (he_map(latest.get(k)) for k in
                                 ("gross_load_hourly", "wind_hourly", "solar_hourly"))
    sol_act_max = max((v for v in sol_act.values() if v is not None), default=None)

    # forecast lookup by "YYYY-MM-DD HH" (hour-BEGINNING 0-23, Central wall clock)
    ts = fc.get("timestamps") or []
    fidx = {t: i for i, t in enumerate(ts)}
    def fseries(name):
        arr = fc.get(name) or []
        return lambda key: num(arr[fidx[key]]) if key in fidx and fidx[key] < len(arr) else None
    f_gl, f_wind, f_sol = fseries("gross_load"), fseries("wind"), fseries("solar")

    # which forecast series are unusable this run
    fc_missing = set(ef.get("missing_sources") or [])
    for src, name in (("load", "gross_load"), ("wind", "wind"), ("solar", "solar")):
        arr = fc.get(name) or []
        if not arr or not any(arr):
            fc_missing.add(src)                              # also covers older files without the flag

    # per-day solar maxima, to tell "night" from "missing all day"
    def day_max(getter, d):
        vals = [getter(f"{d.isoformat()} {h:02d}") for h in range(24)]
        vals = [v for v in vals if v is not None]
        return max(vals) if vals else None
    bc_get = lambda field: (lambda key: num((bc.get(key) or {}).get(field)))
    b_gl, b_wind, b_sol = bc_get("gross_load"), bc_get("wind"), bc_get("solar")

    rows, bc_dates_seen = [], set()
    for d in dates:
        is_actual = (d == data_date)
        f_sol_max = day_max(f_sol, d)
        b_sol_max = day_max(b_sol, d)
        for he in range(1, 25):
            hb = he - 1
            if not hour_exists(d, hb):
                continue
            key = f"{d.isoformat()} {hb:02d}"
            ts_ct = datetime(d.year, d.month, d.day, hb, tzinfo=CT).isoformat()

            if is_actual:
                g = clean("gross_load", he, gl_act.get(he))
                w = clean("wind", he, wind_act.get(he))
                sv = sol_act.get(he)
                if sv is None and sol_act and he not in DAYLIGHT_HE:
                    sv = 0.0                                  # absent night hour = zero generation
                s = clean("solar", he, sv, sol_act_max)
                status = "actual"
            else:
                g = None if "load"  in fc_missing else clean("gross_load", he, f_gl(key))
                w = None if "wind"  in fc_missing else clean("wind", he, f_wind(key))
                s = None if "solar" in fc_missing else clean("solar", he, f_sol(key), f_sol_max)
                status = "forecast"

            bg = clean("gross_load", he, b_gl(key))
            bw = clean("wind", he, b_wind(key))
            bs = clean("solar", he, b_sol(key), b_sol_max)
            if key in bc:
                bc_dates_seen.add(d)

            g, w, s, bg, bw, bs = (r1(x) for x in (g, w, s, bg, bw, bs))
            rows.append(dict(zip(ROW_KEYS, (
                d.isoformat(), he, ts_ct, status, g, w, s, net(g, w, s),
                bg, bw, bs, net(bg, bw, bs)))))

    # plausibility guard: refuse to publish wrong units or a corrupted series
    for r in rows:
        for field, (lo, hi) in PLAUSIBLE.items():
            for pfx in ("", "bc_"):
                v = r[pfx + field]
                if v is not None and not (lo <= v <= hi):
                    raise ValueError(f"implausible {pfx}{field}={v} GW on {r['date']} HE{r['he']} "
                                     f"(expected {lo}-{hi}); wrong units or corrupt input")

    # daily rows, from the hourly rows
    daily = []
    for d in dates:
        day = [r for r in rows if r["date"] == d.isoformat()]
        def vals(field): return [r[field] for r in day if r[field] is not None]
        nets = [(r["net_load"], r["he"]) for r in day if r["net_load"] is not None]
        peak_net, peak_he = (max(nets, key=lambda t: (t[0], -t[1])) if nets else (None, None))
        def avg(xs): return r1(sum(xs) / len(xs)) if xs else None
        def top(xs): return max(xs) if xs else None
        daily.append({"date": d.isoformat(), "peak_gross": top(vals("gross_load")),
                      "peak_net": peak_net, "peak_net_he": peak_he,
                      "wind_avg": avg(vals("wind")), "solar_peak": top(vals("solar")),
                      "bc_peak_net": top(vals("bc_net_load")), "bc_wind_avg": avg(vals("bc_wind"))})

    # AS spreads: compact per (date, product) arrays, position 0 = HE1 ... 23 = HE24
    series, as_start, as_end = [], None, ap.get("end_date")
    if as_end:
        end_d = parse_date(as_end)
        as_start = (end_d - timedelta(days=AS_DAYS - 1)).isoformat()
        by_key = {}
        for prod, recs in (ap.get("series") or {}).items():
            for rec in recs or []:
                try:
                    by_key[(str(rec["date"])[:10], prod, int(rec["he"]))] = num(rec.get("spread"))
                except (KeyError, TypeError, ValueError):
                    pass
        for i in range(AS_DAYS):
            ds = (parse_date(as_start) + timedelta(days=i)).isoformat()
            for prod in AS_PRODUCTS:
                series.append({"date": ds, "product": prod,
                               "da_minus_rt": [r2(by_key.get((ds, prod, he))) for he in range(1, 25)]})

    # missing_sources: forecast flags + actual gaps + bid-close + AS
    missing = set(ef.get("missing_sources") or []) | fc_missing
    if not gl_act:   missing.add("gross_load_actual")
    if not wind_act: missing.add("wind_actual")
    if not sol_act:  missing.add("solar_actual")
    for d in dates:                                          # bid-close is expected once its cutoff passed
        cutoff = datetime(d.year, d.month, d.day, 10, tzinfo=CT) - timedelta(days=1)
        if cutoff <= now_ct and d not in bc_dates_seen:
            missing.add("bid_close")
    if not ap or ap.get("error") or not (ap.get("series") or {}):
        missing.add("as_prices")
    elif all(rec.get("rt") is None for recs in ap["series"].values() for rec in (recs or [])):
        missing.add("as_prices_rt")

    run_at = None
    if ef.get("generated_at"):
        try:
            run_at = (datetime.fromisoformat(str(ef["generated_at"]).replace("Z", "+00:00"))
                      .astimezone(CT).replace(microsecond=0).isoformat())
        except ValueError:
            pass

    return {
        "schema_version":     SCHEMA_VERSION,
        "generated_at":       now_ct.astimezone(CT).replace(microsecond=0).isoformat(),
        "data_date":          data_date.isoformat(),
        "forecast_run_at":    run_at,
        "as_prices_end_date": str(as_end)[:10] if as_end else None,
        "missing_sources":    sorted(missing),
        "hourly":             rows,
        "daily":              daily,
        "as_spreads":         {"start": as_start, "end": str(as_end)[:10] if as_end else None,
                               "unit": "$/MW", "he_index": "position 0 = HE1 ... 23 = HE24",
                               "series": series},
    }


# ── additive sections ────────────────────────────────────────────────────────────────────────
EXTRA_KEYS = ("constraints", "weather", "nodes", "hubs", "data_flags", "freshness")
_WARNED = set()


def fin(x):
    """A finite float, or None. Rejects bools, strings, NaN and +/-Infinity."""
    if x is None or isinstance(x, bool) or not isinstance(x, (int, float)):
        return None
    v = float(x)
    return v if math.isfinite(v) else None


def p2(x):
    """Price / shadow price / MCC: 2 decimals, or None."""
    v = fin(x)
    return None if v is None else round(v, 2) + 0.0


def p3(x):
    """Ratios (Sharpe): 3 decimals, or None."""
    v = fin(x)
    return None if v is None else round(v, 3) + 0.0


def _text(x):
    return x if isinstance(x, str) and x else None


def _dict(x):
    return x if isinstance(x, dict) else {}


def build_constraints(latest, limit=CONSTRAINTS_N):
    """Top `limit` constraints by avg shadow price ($/MWh) from latest.json 'constraints'.
    peak_hour is the hour ENDING (1-24) with the highest avg shadow price; top_nodes are the
    (up to 3) nodes with the largest |MCC| from that constraint's node_mcc, sign kept."""
    ranked = []
    for c in latest.get("constraints") or []:
        if not isinstance(c, dict):
            continue
        avg = fin(c.get("avg_shadow"))
        if avg is None:
            continue
        peak_hour, best = None, None
        for he, rec in _dict(c.get("hourly")).items():
            v = fin(rec.get("avg_shadow") if isinstance(rec, dict) else rec)
            try:
                h = int(he)
            except (TypeError, ValueError):
                continue
            if v is not None and (best is None or v > best or (v == best and h < peak_hour)):
                best, peak_hour = v, h
        mcc = [(str(n), fin(v)) for n, v in _dict(c.get("node_mcc")).items()]
        mcc = sorted(((n, v) for n, v in mcc if v is not None), key=lambda t: (-abs(t[1]), t[0]))
        hb = fin(c.get("hours_binding"))
        impact = fin(c.get("hen_impact_score"))
        row = {"name": _text(c.get("name")), "from_station": _text(c.get("from_station")),
               "to_station": _text(c.get("to_station")), "avg_shadow": p2(avg),
               "peak_shadow": p2(c.get("peak_shadow")),
               "hours_binding": None if hb is None else int(hb),
               "hen_impact_score": p2(impact), "peak_hour": peak_hour,
               "top_nodes": [{"node": n, "mcc": p2(v)} for n, v in mcc[:TOP_NODES_N]],
               # peak_hour comes from the keys of each constraint's "hourly" dict. Those are produced in
               # hen_integrations.collect_ercot_constraints as  he = int(SCEDTimestamp[11:13]) + 1  on the
               # Central-time SCED timestamp, i.e. HOUR-ENDING 1-24 (the 13:05 interval is HE14), so no
               # conversion is needed and peak_hour_ending == peak_hour. Kept as its own explicit field.
               "peak_hour_ending": peak_hour if peak_hour is not None and 1 <= peak_hour <= 24 else None}
        ranked.append((-avg, -(impact or 0.0), str(c.get("name")), row))
    ranked.sort(key=lambda t: t[:3])
    return [t[3] for t in ranked[:limit]]


def build_weather(latest, days=WEATHER_DAYS):
    """The next `days` days AFTER data_date for each city: [{date, high, precip_pct}].
    The pipeline writes 0 where a high temperature is missing, which no Texas metro ever reaches,
    so a high of 0 is reported as null. Cities with no day after data_date are left out."""
    cutoff = str(latest["data_date"])[:10]
    cities = _dict(_dict(latest.get("weather")).get("cities"))
    out = {}
    for city in sorted(cities):
        rows = []
        for d in _dict(cities[city]).get("days") or []:
            if not isinstance(d, dict):
                continue
            ds = str(d.get("date"))[:10]
            if ds <= cutoff:
                continue
            hi, pp = fin(d.get("high")), fin(d.get("precip_pct"))
            rows.append({"date": ds,
                         "high": None if hi in (None, 0) else int(round(hi)),
                         "precip_pct": None if pp is None else (int(pp) if pp == int(pp) else round(pp, 1))})
        rows.sort(key=lambda r: r["date"])
        if rows:
            out[str(city)] = rows[:days]
    return out


def _node_constraint_index(latest):
    """node -> [(constraint name, mcc)] over ALL constraints in latest.json (the pipeline keeps its 20
    highest HEN-impact constraints there), biggest |mcc| first, ties by name. MCC that rounds to 0.00
    is dropped: a constraint with no real effect on the node is not worth listing."""
    idx = {}
    for c in latest.get("constraints") or []:
        if not isinstance(c, dict) or _text(c.get("name")) is None:
            continue
        for node, v in _dict(c.get("node_mcc")).items():
            f = fin(v)
            if f is not None and abs(f) >= 0.005:
                idx.setdefault(str(node), []).append((c["name"], f))
    for node in idx:
        idx[node].sort(key=lambda t: (-abs(t[1]), t[0]))
    return idx


def _node_region(node, regions):
    if node in REGION_OVERRIDES:
        return REGION_OVERRIDES[node]
    for groups in (regions, FALLBACK_REGIONS):
        for name, members in groups.items():
            if isinstance(members, list) and node in members:
                return str(name)
    return None


def build_nodes(latest, top_constraints_n=TOP_CONSTRAINTS_PER_NODE):
    """One entry per node in sharpe_ratios (sorted by name). The first eight fields are the original
    ones; top_constraints, hen_name, region (and hen_name_confirmed for unconfirmed names) follow."""
    sharpe, rt, dart = (_dict(latest.get(k)) for k in ("sharpe_ratios", "rt", "dart"))
    mcc = _dict(latest.get("node_mcc_summary"))
    by_node, regions = _node_constraint_index(latest), _dict(latest.get("regions"))
    out = {}
    for node in sorted(sharpe):
        s_, r_ = _dict(sharpe[node]), _dict(rt.get(node))
        entry = {"signal": _text(s_.get("signal")), "sharpe": p3(s_.get("sharpe")),
                 "mean_dart": p2(s_.get("mean_dart")), "mcc": p2(mcc.get(node)),
                 "rt_avg": p2(r_.get("avg")), "rt_max": p2(r_.get("max")),
                 "rt_min": p2(r_.get("min")), "dart": p2(dart.get(node))}
        entry["top_constraints"] = [{"name": n, "mcc": p2(v)} for n, v in by_node.get(str(node), [])[:top_constraints_n]]
        entry["hen_name"] = HEN_NAMES.get(node)
        entry["region"] = _node_region(node, regions)
        if node in UNCONFIRMED_HEN_NAMES:
            entry["hen_name_confirmed"] = False
        out[str(node)] = entry
    return out


def build_hubs(latest):
    """One entry per ERCOT hub / load zone: RT avg/max/min and DA avg."""
    rt, da = _dict(latest.get("hub_zone_rt")), _dict(latest.get("hub_zone_da"))
    out = {}
    for hub in sorted(set(rt) | set(da)):
        r_, d_ = _dict(rt.get(hub)), _dict(da.get(hub))
        out[str(hub)] = {"rt_avg": p2(r_.get("avg")), "rt_max": p2(r_.get("max")),
                         "rt_min": p2(r_.get("min")), "da_avg": p2(d_.get("avg"))}
    return out


def _history_regional_empty(history, data_date):
    """True when the newest history entry has no regional block, or is not for data_date at all
    (so the regional view has nothing for that day). None when history is unavailable."""
    entries = [e for e in history or [] if isinstance(e, dict) and e.get("date")]
    if not entries:
        return None
    last = max(entries, key=lambda e: str(e["date"]))
    return str(last["date"])[:10] != str(data_date)[:10] or not last.get("regional")


FLAG_KEYS = ("da_empty", "dart_empty", "rt_empty", "history_regional_empty_for_data_date",
             "forecast_missing_sources", "constraints_count", "weather_city_count", "node_count")


def build_flags(latest, history, constraints, weather, nodes):
    ms = _dict(latest.get("ercot_forecasts")).get("missing_sources")
    return {"da_empty":   not latest.get("da"),             # true when the dict has zero keys
            "dart_empty": not latest.get("dart"),
            "rt_empty":   not latest.get("rt"),
            "history_regional_empty_for_data_date": _history_regional_empty(history, latest.get("data_date")),
            "forecast_missing_sources": [str(x) for x in ms] if isinstance(ms, list) else [],
            "constraints_count":  len(constraints),         # entries actually in this file
            "weather_city_count": len(weather),
            "node_count":         len(nodes)}


def _section(name, fn, default):
    """Build one additive section; on any error warn once and leave it empty. The existing
    sections and the file as a whole are never put at risk by a surprise in these."""
    try:
        return fn()
    except Exception as exc:
        if name not in _WARNED:
            _WARNED.add(name)
            print(f"::warning::brief_feed: section '{name}' skipped ({exc!r}); the rest of the feed is unaffected")
        return default


def build_extras(latest, history=None, weather_days=WEATHER_DAYS, constraints_n=CONSTRAINTS_N,
                 top_constraints_n=TOP_CONSTRAINTS_PER_NODE):
    constraints = _section("constraints", lambda: build_constraints(latest, constraints_n), [])
    weather     = _section("weather",     lambda: build_weather(latest, weather_days), {})
    nodes       = _section("nodes",       lambda: build_nodes(latest, top_constraints_n), {})
    hubs        = _section("hubs",        lambda: build_hubs(latest), {})
    flags       = _section("data_flags",  lambda: build_flags(latest, history, constraints, weather, nodes),
                           dict.fromkeys(FLAG_KEYS))
    return {"constraints": constraints, "weather": weather, "nodes": nodes, "hubs": hubs,
            "data_flags": flags}


def build_freshness(feed):
    """Is this feed safe for the brief to use as today's? Computed from the feed's own fields, so it
    always describes exactly what is in the file. All five conditions must hold:
      1. data_date is yesterday in Central, relative to generated_at
      2. generated_at is at or after 06:00 CT on its own date
      3. forecast_run_at is on the same Central date as generated_at
      4. missing_sources is empty
      5. none of data_flags da_empty / dart_empty / rt_empty is true
    One plain-English reason is recorded per failed condition."""
    gen = datetime.fromisoformat(feed["generated_at"]).astimezone(CT)
    expected = gen.date() - timedelta(days=1)                # calendar yesterday: right on 23/25-hour days
    reasons = []
    if feed.get("data_date") != expected.isoformat():
        reasons.append(f"data_date is {feed.get('data_date')} but yesterday in Central time is {expected}, "
                       f"so the report is not for the expected day")
    if gen.time() < FRESH_AFTER:
        reasons.append(f"the feed was generated at {gen:%H:%M} CT, before the {FRESH_AFTER:%H:%M} CT cutoff, "
                       f"so overnight data may be incomplete")
    if not feed.get("forecast_run_at"):
        reasons.append("forecast_run_at is missing, so the forecast run time cannot be checked")
    else:
        fr = datetime.fromisoformat(feed["forecast_run_at"]).astimezone(CT)
        if fr.date() != gen.date():
            reasons.append(f"the forecasts were last run {fr:%Y-%m-%d %H:%M} CT, not on the same Central date "
                           f"as the feed ({gen:%Y-%m-%d})")
    if feed.get("missing_sources"):
        reasons.append("some sources are missing today: " + ", ".join(str(x) for x in feed["missing_sources"]))
    flags = _dict(feed.get("data_flags"))
    for key, label in (("da_empty", "day-ahead (DA) energy prices"), ("dart_empty", "DART spreads"),
                       ("rt_empty", "real-time (RT) prices")):
        if flags.get(key) is True:
            reasons.append(f"{label} are empty ({key} is true)")
    return {"expected_data_date": expected.isoformat(), "is_fresh": not reasons, "stale_reasons": reasons}


def _assemble(core, latest, history, weather_days, constraints_n, top_constraints_n):
    feed = dict(core)
    feed.update(build_extras(latest, history, weather_days, constraints_n, top_constraints_n))
    # if freshness itself cannot be computed, say "not fresh": never claim fresh by accident
    feed["freshness"] = _section("freshness", lambda: build_freshness(feed),
                                 {"expected_data_date": None, "is_fresh": False,
                                  "stale_reasons": ["freshness could not be computed"]})
    return feed


def build(latest, now_ct=None, history=None, weather_days=WEATHER_DAYS, constraints_n=CONSTRAINTS_N,
          top_constraints_n=TOP_CONSTRAINTS_PER_NODE):
    """The full feed: the original sections untouched and in their original order, then the
    additive ones (constraints, weather, nodes, hubs, data_flags, freshness)."""
    return _assemble(build_core(latest, now_ct), latest, history, weather_days, constraints_n, top_constraints_n)


def fits(text):
    """Under the limit as a file (characters and bytes) and as json.dumps() of it, which is
    how the brief measures it (json.dumps' default separators are longer than the file's)."""
    return (len(text) < MAX_BYTES and len(text.encode("utf-8")) < MAX_BYTES
            and len(json.dumps(json.loads(text))) < MAX_BYTES)


def render(latest, now_ct=None, history=None):
    """Build and serialise, trimming only if needed, in this order: weather to 3 days, constraints to 6,
    then (last resort, so a size problem can never stop the daily feed) each node's top_constraints to 1.
    Returns (feed, text, note) where note says what was trimmed ('' if nothing)."""
    core = build_core(latest, now_ct)
    plans = [(WEATHER_DAYS, CONSTRAINTS_N, TOP_CONSTRAINTS_PER_NODE, ""),
             (WEATHER_DAYS_TRIMMED, CONSTRAINTS_N, TOP_CONSTRAINTS_PER_NODE, "weather cut to 3 days"),
             (WEATHER_DAYS_TRIMMED, CONSTRAINTS_N_TRIMMED, TOP_CONSTRAINTS_PER_NODE,
              "weather cut to 3 days, constraints cut to 6"),
             (WEATHER_DAYS_TRIMMED, CONSTRAINTS_N_TRIMMED, TOP_CONSTRAINTS_TRIMMED,
              "weather cut to 3 days, constraints cut to 6, top_constraints cut to 1 per node")]
    for weather_days, constraints_n, top_n, note in plans:
        feed = _assemble(core, latest, history, weather_days, constraints_n, top_n)
        text = dumps(feed)
        json.loads(text)                                      # must round-trip as valid JSON
        if fits(text):
            return feed, text, note
    raise ValueError(f"feed is {len(text)} characters even after trimming; limit is {MAX_BYTES}")


def dumps(feed):
    """Valid JSON, compact, one row per line (small and diff-friendly)."""
    j = lambda o: json.dumps(o, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    out = ["{"]
    for k in ("schema_version", "generated_at", "data_date", "forecast_run_at",
              "as_prices_end_date", "missing_sources"):
        out.append(f'"{k}":{j(feed[k])},')
    for k in ("hourly", "daily"):
        out.append(f'"{k}":[')
        out.append(",\n".join(j(r) for r in feed[k]))
        out.append("],")
    a = feed["as_spreads"]
    out.append('"as_spreads":{' + ",".join(f'"{k}":{j(a[k])}' for k in ("start", "end", "unit", "he_index"))
               + ',"series":[')
    out.append(",\n".join(j(r) for r in a["series"]))
    extras = [k for k in EXTRA_KEYS if k in feed]
    out.append("]}" + ("," if extras else ""))
    for i, k in enumerate(extras):
        comma, val = ("," if i < len(extras) - 1 else ""), feed[k]
        if k in ("data_flags", "freshness"):
            out.append(f'"{k}":{j(val)}{comma}')
        elif isinstance(val, list):                           # constraints: one per line
            out += [f'"{k}":[]{comma}'] if not val else [f'"{k}":[', ",\n".join(j(r) for r in val), "]" + comma]
        else:                                                 # weather / nodes / hubs: one key per line
            out += [f'"{k}":{{}}{comma}'] if not val else [
                f'"{k}":{{', ",\n".join(f"{j(n)}:{j(v)}" for n, v in val.items()), "}" + comma]
    out.append("}")
    return "\n".join(out) + "\n"


def write_atomic(path, text):
    folder = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=folder, prefix=".brief_feed.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def load_history(path):
    """history.json as a list, or None if it is missing or unreadable (the flag then reads null)."""
    try:
        with open(path, encoding="utf-8") as fh:
            h = json.load(fh)
        return h if isinstance(h, list) else None
    except (OSError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser(description="Build brief_feed.json from latest.json.")
    ap.add_argument("--in",  dest="src", default="latest.json")
    ap.add_argument("--history", dest="hist", default=os.path.join("dashboard", "history.json"))
    ap.add_argument("--out", dest="dst", default=os.path.join("dashboard", "brief_feed.json"))
    args = ap.parse_args()
    try:
        with open(args.src, encoding="utf-8") as fh:
            latest = json.load(fh)
        feed, text, note = render(latest, history=load_history(args.hist))
        size = len(text.encode("utf-8"))
        write_atomic(args.dst, text)
    except Exception as exc:                                 # previous file stays untouched
        print(f"brief_feed: FAILED, previous file left in place -- {exc}", file=sys.stderr)
        return 1
    flags = feed["data_flags"]
    print(f"brief_feed: {len(feed['hourly'])} hourly rows, {len(feed['daily'])} daily rows, "
          f"{len(feed['as_spreads']['series'])} AS series, {flags['constraints_count']} constraints, "
          f"{flags['weather_city_count']} weather cities, {flags['node_count']} nodes, {len(feed['hubs'])} hubs, "
          f"{size / 1000:.1f} KB, data_date={feed['data_date']}, missing_sources={feed['missing_sources']}"
          f"{' [' + note + ']' if note else ''}")
    print(f"brief_feed: data_flags da_empty={flags['da_empty']} dart_empty={flags['dart_empty']} "
          f"rt_empty={flags['rt_empty']} history_regional_empty={flags['history_regional_empty_for_data_date']} "
          f"forecast_missing_sources={flags['forecast_missing_sources']}")
    fresh = feed["freshness"]
    print(f"brief_feed: freshness is_fresh={fresh['is_fresh']} expected_data_date={fresh['expected_data_date']}"
          f"{' stale_reasons=' + str(fresh['stale_reasons']) if fresh['stale_reasons'] else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
