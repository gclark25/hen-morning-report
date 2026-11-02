#!/usr/bin/env python3
"""Build dashboard/brief_feed.json -- the small, market-data-only feed for the morning brief.

    python3 build_brief_feed.py                      latest.json -> dashboard/brief_feed.json
    python3 build_brief_feed.py --in dashboard/latest.json --out /tmp/brief_feed.json

Why it exists: latest.json is too large for the brief's fetch tool, so it arrives cut off.
This file carries only what the brief needs and stays well under 50 KB.

MARKET DATA ONLY. The script reads a fixed whitelist of latest.json sections and never
copies anything else, so HEN revenue, dispatch, state of charge and node-level data
cannot reach the feed:
    data_date, gross_load_hourly, wind_hourly, solar_hourly   (yesterday's actuals, GW)
    ercot_forecasts, bid_close_forecast                       (system forecasts, GW)
    as_prices                                                 (system AS clearing prices)

Refresh: rewritten every time the pipeline runs (daily at 6:00 AM Central via the Cloudflare
trigger, plus any manual run). It does not update intraday on its own.

Rules: GW fields rounded to 1 decimal; AS spreads to 2 (cents matter at that scale); null, never
0, for missing values; same shape every day. Atomic write: on any failure the previous file is
left untouched and the script exits 1, so a stale file is detectably stale (generated_at,
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
import os
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

CT  = ZoneInfo("America/Chicago")
UTC = timezone.utc

SCHEMA_VERSION = 1
N_DAYS         = 4                       # data_date (actuals) + today + the next two days
AS_DAYS        = 5                       # AS spread lookback shown in the feed
AS_PRODUCTS    = ["REGUP", "REGDN", "RRS", "NSPIN", "ECRS"]
DAYLIGHT_HE    = range(10, 18)           # HE10-HE17: solar is always > 0 here
MAX_BYTES      = 50_000
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
def build(latest, now_ct=None):
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


def dumps(feed):
    """Valid JSON, compact, one row per line (small and diff-friendly)."""
    j = lambda o: json.dumps(o, separators=(",", ":"), ensure_ascii=False)
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
    out.append("]}")
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


def main():
    ap = argparse.ArgumentParser(description="Build brief_feed.json from latest.json.")
    ap.add_argument("--in",  dest="src", default="latest.json")
    ap.add_argument("--out", dest="dst", default=os.path.join("dashboard", "brief_feed.json"))
    args = ap.parse_args()
    try:
        with open(args.src, encoding="utf-8") as fh:
            latest = json.load(fh)
        feed = build(latest)
        text = dumps(feed)
        json.loads(text)                                     # must round-trip as valid JSON
        size = len(text.encode("utf-8"))
        if size > MAX_BYTES:
            raise ValueError(f"feed is {size} bytes, over the {MAX_BYTES} byte limit")
        write_atomic(args.dst, text)
    except Exception as exc:                                 # previous file stays untouched
        print(f"brief_feed: FAILED, previous file left in place -- {exc}", file=sys.stderr)
        return 1
    print(f"brief_feed: {len(feed['hourly'])} hourly rows, {len(feed['daily'])} daily rows, "
          f"{len(feed['as_spreads']['series'])} AS series, {size / 1000:.1f} KB, "
          f"data_date={feed['data_date']}, missing_sources={feed['missing_sources']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
