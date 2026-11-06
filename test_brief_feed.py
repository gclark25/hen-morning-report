"""
Tests for build_brief_feed.py (the additive sections: constraints, weather, nodes, hubs, data_flags).

Run either way -- no setup beyond Python 3.9+; pytest is optional:

    python3 test_brief_feed.py
    python3 -m pytest -q test_brief_feed.py

Input: a SAVED latest.json (+ history.json). By default it uses dashboard/latest.json and
dashboard/history.json from the repo; override with LATEST_JSON=... HISTORY_JSON=... If the file is
not there it falls back to a built-in fixture that mirrors the pipeline's real latest.json shape, and
says so. Tests that need controlled inputs (rule checks, trimming, failure handling) always use the
fixture; the shape / size / NaN / rounding / unchanged-keys / market-only checks run on the saved file.
"""
import contextlib
import copy
import hashlib
import io
import json
import math
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_brief_feed as bf

NOW = datetime(2026, 11, 6, 6, 12, tzinfo=bf.CT)
CORE_KEYS = ["schema_version", "generated_at", "data_date", "forecast_run_at", "as_prices_end_date",
             "missing_sources", "hourly", "daily", "as_spreads"]
NEW_KEYS = ["constraints", "weather", "nodes", "hubs", "data_flags", "freshness"]
NODES = ["TOYAH_RN", "SADLBACK_RN", "FAULKNER_RN", "COYOTSPR_RN", "LONESTAR_RN", "RTLSNAKE_BT", "CEDRVALE_RN",
         "SBEAN_BESS", "GOMZ_RN", "GRDNE_ESR_RN", "JDKNS_RN", "SANDLAKE_RN", "OLNEYTN_RN", "DIBOL_RN",
         "FRMRSVLW_RN", "MNWL_BESS_RN", "LFSTH_RN", "PAULN_RN", "CISC_RN", "MV_VALV4_RN", "WLTC_ESR_RN",
         "MAINLAND_RN", "FALFUR_RN", "PAVLOV_BT_RN", "POTEETS_RN", "TYNAN_RN", "CATARINA_B1", "HOLCOMB_RN1",
         "HAMI_BESS_RN", "JUNCTION_RN", "RUSSEKST_RN", "FTDUNCAN_RN"]
HUBS = ["HB_HOUSTON", "HB_NORTH", "HB_SOUTH", "HB_WEST", "LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST"]
CITIES = ["Abilene, TX", "Amarillo, TX", "Austin, TX", "Corpus Christi, TX", "Dallas, TX", "El Paso, TX",
          "Houston, TX", "Laredo, TX", "Lubbock, TX", "Midland, TX", "San Angelo, TX", "San Antonio, TX",
          "Waco, TX", "Wichita Falls, TX"]
SIGNALS = ["STRONG_DA", "LEAN_DA", "NEUTRAL", "LEAN_RT", "STRONG_RT"]
NODE_KEYS = ["signal", "sharpe", "mean_dart", "mcc", "rt_avg", "rt_max", "rt_min", "dart"]      # the original eight
# the dashboard's own groups (REGIONS in hen_morning_report.py), as written to latest.json["regions"]
REGIONS = {
    "West Texas": ["TOYAH_RN", "SADLBACK_RN", "FAULKNER_RN", "COYOTSPR_RN", "LONESTAR_RN", "RTLSNAKE_BT", "CEDRVALE_RN",
                   "SBEAN_BESS", "GOMZ_RN", "GRDNE_ESR_RN", "JDKNS_RN", "SANDLAKE_RN"],
    "North Texas": ["OLNEYTN_RN", "DIBOL_RN", "FRMRSVLW_RN", "MNWL_BESS_RN", "LFSTH_RN", "PAULN_RN", "CISC_RN"],
    "Coastal": ["MV_VALV4_RN", "WLTC_ESR_RN", "MAINLAND_RN", "FALFUR_RN", "PAVLOV_BT_RN", "POTEETS_RN", "TYNAN_RN"],
    "Premium": ["CATARINA_B1", "HOLCOMB_RN1", "HAMI_BESS_RN", "JUNCTION_RN", "RUSSEKST_RN", "FTDUNCAN_RN"],
}
# HEN names exactly as given in the request (an independent copy, so a typo in the script is caught)
EXPECTED_HEN_NAMES = dict(pair.split(" ", 1) for pair in [
    "CATARINA_B1 Catarina", "CEDRVALE_RN Cedarvale", "CISC_RN Cisco", "COYOTSPR_RN Coyote Springs", "DIBOL_RN Diboll",
    "FALFUR_RN Falfurrias", "FAULKNER_RN Faulkner", "FRMRSVLW_RN Farmersville", "FTDUNCAN_RN Fort Duncan",
    "GOMZ_RN Gomez", "GRDNE_ESR_RN Garden City East", "HAMI_BESS_RN Hamilton", "HOLCOMB_RN1 Holcomb", "JDKNS_RN Judkins",
    "JUNCTION_RN Junction", "LFSTH_RN Lufkin South", "LONESTAR_RN Lonestar", "MAINLAND_RN Mainland",
    "MNWL_BESS_RN Mineral Wells East", "MV_VALV4_RN Val Verde", "OLNEYTN_RN Olney", "PAULN_RN Pauline",
    "PAVLOV_BT_RN Pavlov", "POTEETS_RN Poteets", "RTLSNAKE_BT Rattlesnake", "RUSSEKST_RN Russek Street",
    "SADLBACK_RN Saddleback", "SANDLAKE_RN Sandlake", "SBEAN_BESS Screwbean", "TOYAH_RN Cherry Creek",
    "TYNAN_RN Tynan", "WLTC_ESR_RN Weil Tract"])


# -- fixture: the shape the pipeline really writes ----------------------------------------------
def make_latest(data_date="2026-11-05", gen_utc="2026-11-06T12:07:58.123456Z"):
    dd = date.fromisoformat(data_date)
    gl = lambda h: 52 + 9 * math.sin((h - 9) / 24 * 2 * math.pi)
    wd = lambda h: 14 + 4 * math.cos(h / 24 * 2 * math.pi)
    so = lambda h: max(0.0, 26 * math.sin(math.pi * (h - 6.5) / 12.5)) if 7 <= h <= 19 else 0.0
    L = {"data_date": data_date, "generated_at": (dd + timedelta(days=1)).isoformat()}
    L["gross_load_hourly"] = {str(h): round(gl(h), 1) for h in range(1, 25)}
    L["wind_hourly"] = {str(h): round(wd(h), 1) for h in range(1, 25)}
    L["solar_hourly"] = {str(h): round(so(h), 1) for h in range(1, 25) if so(h) > 0}
    ts, a, b, c, n = [], [], [], [], []
    for i in range(9):
        d = dd + timedelta(days=i)
        for hb in range(24):
            g, w, s = gl(hb + 1), wd(hb + 1), so(hb + 1)
            ts.append(f"{d} {hb:02d}"); a.append(round(g, 2)); b.append(round(w, 2)); c.append(round(s, 2)); n.append(round(g - w - s, 2))
    L["ercot_forecasts"] = {"generated_at": gen_utc, "missing_sources": [],
                            "hourly_24hr": {"timestamps": ts, "gross_load": a, "wind": b, "solar": c, "net_load": n},
                            "daily_7day": {}}
    L["bid_close_forecast"] = {"hourly": {f"{d} {hb:02d}": {"gross_load": round(gl(hb + 1) - 1, 2), "wind": round(wd(hb + 1) - 1, 2),
                                                           "solar": round(so(hb + 1), 2), "net_load": 0.0}
                                          for d in (dd, dd + timedelta(days=1)) for hb in range(24)}}
    L["as_prices"] = {"end_date": data_date, "start_date": (dd - timedelta(days=5)).isoformat(), "as_types": bf.AS_PRODUCTS,
                      "series": {p: [{"date": (dd - timedelta(days=5 - k)).isoformat(), "he": he, "da": 1.0, "rt": 0.5, "spread": 0.5}
                                     for k in range(6) for he in range(1, 25)] for p in bf.AS_PRODUCTS}}
    # settlement points
    L["rt"] = {nd: {"avg": round(21 + i * 0.37, 2), "max": round(60 + i * 3.113, 2), "min": round(-4 + i * 0.211, 2)} for i, nd in enumerate(NODES)}
    L["da"] = {nd: {"avg": round(22 + i * 0.41, 2), "max": round(55 + i * 2.7, 2)} for i, nd in enumerate(NODES)}
    L["dart"] = {nd: round(L["da"][nd]["avg"] - L["rt"][nd]["avg"], 2) for nd in NODES}
    L["sharpe_ratios"] = {nd: {"sharpe": round(-0.9 + i * 0.0577, 4), "mean_dart": round(-3 + i * 0.1234, 4), "stdev_dart": 4.1,
                               "n_obs": 120, "signal": SIGNALS[i % 5]} for i, nd in enumerate(NODES)}
    L["hub_zone_rt"] = {h: {"avg": round(23.456 + i, 4), "max": round(70.1234 + i, 4), "min": round(-1.2345 + i, 4)} for i, h in enumerate(HUBS)}
    L["hub_zone_da"] = {h: {"avg": round(24.5678 + i, 4), "max": round(66.6666 + i, 4)} for i, h in enumerate(HUBS)}
    # constraints: shaped like collect_ercot_constraints (hourly keyed by int hour-ending -> strings after JSON)
    cons, mcc_sum = [], {}
    names = ["SNDSWX_BASE_1", "TREADW_YELWJC1_1", "MADDUX_TREADW1_1", "BLESSI_PAVLOV1_1", "MDOPHR99_A", "6635__G", "PALOUS_WOLFCA1_1",
             "CHAMBERS_AUSTIN1", "LAREDO_HAMILT2", "TWINBU_ODESSA1", "YELWJC_SANANG1", "OLNEY_WICHIT3", "GILLE_BLESSI1", "VALVER_COTULL1"]
    for k, nm in enumerate(names):
        avg = round(3.17 + (k * 7.3) % 41.0 + 0.00456, 2)
        hourly = {he: {"avg_shadow": round(avg * (0.4 + ((he * 7 + k) % 13) / 10), 2), "max_shadow": round(avg * 2, 2), "intervals": 12}
                  for he in range(1, 25) if (he + k) % 3}
        expo = {NODES[(k * 5 + j) % 32]: round(((-1) ** j) * (0.05 + 0.07 * j), 4) for j in range(5)}
        mcc = {nd: round(avg * sf, 4) for nd, sf in expo.items()}
        for nd, v in mcc.items():
            mcc_sum[nd] = round(mcc_sum.get(nd, 0) + v, 4)
        cons.append({"name": nm, "contingency": "X", "from_station": f"STA{k}A", "to_station": f"STA{k}B", "from_kv": 138, "to_kv": 138,
                     "avg_shadow": avg, "peak_shadow": round(avg * 3.2, 2), "hours_binding": len(hourly), "avg_violated_mw": 4.0,
                     "flow_direction": "S\u2192N", "hourly": hourly, "node_exposure": expo,
                     "hen_impact_score": round(max((abs(v) for v in mcc.values()), default=0), 4), "node_mcc": mcc, "shift_factors": {}})
    cons[3]["node_mcc"], cons[3]["node_exposure"], cons[3]["hen_impact_score"] = {}, {}, 0       # no HEN exposure
    cons[5]["hourly"] = {}                                                                       # no hourly detail
    cons.append({"name": "NO_AVG", "avg_shadow": None, "hourly": {}, "node_mcc": {}})            # unusable record
    # The West blind spot: SNDSW_MR1H is in the feed's top 8 but Cedarvale is not among its 3 biggest nodes;
    # WESTEX_X and KLNSW_MR1H have low shadow prices, so they fall OUTSIDE the feed's top 8 by avg_shadow.
    def _c(name, avg, mcc):
        return {"name": name, "contingency": "X", "from_station": "A", "to_station": "B", "from_kv": 138, "to_kv": 138,
                "avg_shadow": avg, "peak_shadow": round(avg * 2, 2), "hours_binding": 5, "avg_violated_mw": 1.0,
                "flow_direction": "S\u2192N", "hourly": {10: {"avg_shadow": avg, "max_shadow": avg, "intervals": 12}},
                "node_exposure": {n: 0.1 for n in mcc}, "hen_impact_score": max(abs(v) for v in mcc.values()),
                "node_mcc": mcc, "shift_factors": {}}
    cons += [_c("SNDSW_MR1H", 204.0, {"POTEETS_RN": 31.37, "TYNAN_RN": 30.8, "HOLCOMB_RN1": 30.68, "CEDRVALE_RN": -14.0}),
             _c("WESTEX_X", 15.0, {"CEDRVALE_RN": -6.0, "TOYAH_RN": -5.5}),
             _c("KLNSW_MR1H", 10.0, {"CEDRVALE_RN": -4.0}),
             _c("TINY_C", 1.0, {"FTDUNCAN_RN": -0.003})]                  # a contribution that rounds to 0.00
    mcc_sum = {}
    for c in cons:
        for nd, v in (c.get("node_mcc") or {}).items():
            mcc_sum[nd] = round(mcc_sum.get(nd, 0) + v, 4)
    L["constraints"], L["node_mcc_summary"] = cons, mcc_sum
    L["weather"] = {"generated_at": "2026-11-06T11:00:00Z", "source": "AG2 Trader", "cities": {
        city: {"days": [{"date": (dd + timedelta(days=1 + k)).isoformat(), "high": 60 + (i + k) % 25,
                         "low": 40 + (i + k) % 15, "precip_pct": (i * 10 + k * 5) % 100} for k in range(15)]}
        for i, city in enumerate(CITIES)}}
    L["weather"]["cities"]["Lubbock, TX"]["days"][1]["high"] = 0                                 # pipeline's "missing" marker
    # HEN-only content that must never reach the feed
    L["modo"] = {"indices": {"hen_2026": {"revenue_mw_year": 20608, "display_name": "HEN 2026"}}}
    L["asset_status"] = {"fleet_summary": {"state_of_charge": 71.5, "available_mw": 1234}}
    L["node_coords"], L["regions"] = {"TOYAH_RN": [31.3, -103.8]}, copy.deepcopy(REGIONS)
    return json.loads(json.dumps(L))            # round-trip so keys are strings, exactly like the saved file


def make_history(data_date="2026-11-05", regional_empty=False):
    reg = {} if regional_empty else {"West Texas": {"avg_dart": -1.2, "avg_rt": 24.1, "avg_intraday": 3.3}}
    return [{"date": "2026-11-04", "fleet": {"rt_avg": 25.0}, "regional": {"Coastal": {"avg_dart": 0.5}}, "battery": {"ending_soc": 40.0}},
            {"date": data_date, "fleet": {"rt_avg": 26.0}, "fundamentals": {}, "nodes": {}, "regional": reg,
             "battery": {"ending_soc": 71.5}}]


# -- the saved file (or the fixture) -------------------------------------------------------------
def _load(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


LATEST_PATH = os.environ.get("LATEST_JSON", os.path.join("dashboard", "latest.json"))
HISTORY_PATH = os.environ.get("HISTORY_JSON", os.path.join("dashboard", "history.json"))
_saved = _load(LATEST_PATH, None)
USING_SAVED = isinstance(_saved, dict) and bool(_saved.get("data_date"))
SAVED = _saved if USING_SAVED else make_latest()
SAVED_HISTORY = (_load(HISTORY_PATH, None) if USING_SAVED else None) or make_history(SAVED["data_date"])
FIX = make_latest()
FIX_HISTORY = make_history()


def _walk(o):
    """Yield (key, value) for every dict entry anywhere in a JSON structure."""
    if isinstance(o, dict):
        for k, v in o.items():
            yield k, v
            yield from _walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from _walk(v)


def _quiet(fn, *a, **k):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        out = fn(*a, **k)
    return out, buf.getvalue()


# -- tests ----------------------------------------------------------------------------------------
def test_all_new_keys_exist():
    feed = bf.build(SAVED, NOW, SAVED_HISTORY)
    assert list(feed) == CORE_KEYS + NEW_KEYS, list(feed)
    for c in feed["constraints"]:
        assert list(c) == ["name", "from_station", "to_station", "avg_shadow", "peak_shadow", "hours_binding",
                           "hen_impact_score", "peak_hour", "top_nodes", "peak_hour_ending"], list(c)
        assert all(list(t) == ["node", "mcc"] for t in c["top_nodes"])
    for city, days in feed["weather"].items():
        assert days and all(list(d) == ["date", "high", "precip_pct"] for d in days), city
    for node, v in feed["nodes"].items():                    # original eight first, then the additive fields
        assert list(v) == NODE_KEYS + ["top_constraints", "hen_name", "region"] + (["hen_name_confirmed"] if node == "TOYAH_RN" else []), (node, list(v))
    assert list(feed["freshness"]) == ["expected_data_date", "is_fresh", "stale_reasons"]
    assert isinstance(feed["freshness"]["is_fresh"], bool) and isinstance(feed["freshness"]["stale_reasons"], list)
    assert all(list(v) == ["rt_avg", "rt_max", "rt_min", "da_avg"] for v in feed["hubs"].values())
    assert list(feed["data_flags"]) == list(bf.FLAG_KEYS)
    assert len(feed["nodes"]) == len(SAVED.get("sharpe_ratios") or {})
    assert feed["data_flags"]["node_count"] == len(feed["nodes"])
    assert feed["data_flags"]["constraints_count"] == len(feed["constraints"])
    assert feed["data_flags"]["weather_city_count"] == len(feed["weather"])


def test_size_under_50000_characters():
    feed, text, note = bf.render(SAVED, NOW, SAVED_HISTORY)
    assert len(text) < 50_000 and len(text.encode()) < 50_000
    assert len(json.dumps(json.loads(text))) < 50_000          # the brief's own measure: len(json.dumps(d))
    print(f"      ({len(text):,} chars as a file, {len(json.dumps(json.loads(text))):,} as json.dumps; trimmed: {note or 'no'})")


def test_no_nan_or_infinity():
    dirty = copy.deepcopy(SAVED)
    first = next(iter(dirty.get("rt") or {}), None) or "X_RN"
    dirty.setdefault("rt", {})[first] = {"avg": float("nan"), "max": float("inf"), "min": float("-inf")}
    dirty.setdefault("dart", {})[first] = float("nan")
    for src in (SAVED, dirty):
        _, text, _ = bf.render(src, NOW, SAVED_HISTORY)
        assert "NaN" not in text and "Infinity" not in text
        json.loads(text, parse_constant=lambda c: (_ for _ in ()).throw(AssertionError(c)))
    node = bf.build(dirty, NOW, SAVED_HISTORY)["nodes"].get(first)
    if node is not None:
        assert node["rt_avg"] is None and node["rt_max"] is None and node["rt_min"] is None and node["dart"] is None


def test_prices_are_rounded():
    feed = bf.build(SAVED, NOW, SAVED_HISTORY)
    price_fields = {"avg_shadow", "peak_shadow", "hen_impact_score", "mcc", "mean_dart", "rt_avg", "rt_max", "rt_min",
                    "dart", "da_avg"}
    seen = 0
    for section in ("constraints", "nodes", "hubs"):
        for k, v in _walk(feed[section]):
            if k in price_fields and v is not None:
                assert round(v, 2) == v, (section, k, v)
                seen += 1
    assert seen > 0
    for k, v in _walk(feed["nodes"]):
        if k == "sharpe" and v is not None:
            assert round(v, 3) == v


def test_da_empty_true_when_da_is_empty():
    base = bf.build(SAVED, NOW, SAVED_HISTORY)["data_flags"]
    if SAVED.get("da"):
        assert base["da_empty"] is False
    for key, flag in (("da", "da_empty"), ("dart", "dart_empty"), ("rt", "rt_empty")):
        L = copy.deepcopy(SAVED)
        L[key] = {}
        flags = bf.build(L, NOW, SAVED_HISTORY)["data_flags"]
        assert flags[flag] is True, (key, flags)
        for other in ("da_empty", "dart_empty", "rt_empty"):
            if other != flag:
                assert flags[other] is base[other], (key, other)
    L = copy.deepcopy(SAVED); L.pop("da", None)                              # a missing key counts as empty too
    assert bf.build(L, NOW, SAVED_HISTORY)["data_flags"]["da_empty"] is True
    # with DA gone the feed still builds and the nodes still list (Sharpe comes from history)
    L = copy.deepcopy(SAVED); L["da"], L["dart"] = {}, {}
    f = bf.build(L, NOW, SAVED_HISTORY)
    assert all(v["dart"] is None for v in f["nodes"].values())


def test_history_regional_flag():
    dd = SAVED["data_date"]
    assert bf.build(SAVED, NOW, make_history(dd, regional_empty=True))["data_flags"]["history_regional_empty_for_data_date"] is True
    assert bf.build(SAVED, NOW, make_history(dd, regional_empty=False))["data_flags"]["history_regional_empty_for_data_date"] is False
    stale = make_history("2026-10-01")                                       # newest entry is not for data_date
    assert bf.build(SAVED, NOW, stale)["data_flags"]["history_regional_empty_for_data_date"] is True
    for unavailable in (None, []):
        assert bf.build(SAVED, NOW, unavailable)["data_flags"]["history_regional_empty_for_data_date"] is None


def test_forecast_missing_sources_is_a_copy():
    L = copy.deepcopy(SAVED)
    L.setdefault("ercot_forecasts", {})["missing_sources"] = ["wind"]
    assert bf.build(L, NOW, SAVED_HISTORY)["data_flags"]["forecast_missing_sources"] == ["wind"]
    L["ercot_forecasts"].pop("missing_sources")
    assert bf.build(L, NOW, SAVED_HISTORY)["data_flags"]["forecast_missing_sources"] == []


def test_preexisting_keys_unchanged():
    core = bf.build_core(SAVED, NOW)
    full = bf.build(SAVED, NOW, SAVED_HISTORY)
    assert list(core) == CORE_KEYS
    assert list(full)[:len(core)] == list(core), "existing keys were renamed or reordered"
    for k in core:
        assert full[k] == core[k], f"existing key changed: {k}"
    _, text, _ = bf.render(SAVED, NOW, SAVED_HISTORY)
    parsed, core_parsed = json.loads(text), json.loads(bf.dumps(core))
    assert list(parsed)[:len(core)] == list(core)
    assert all(parsed[k] == core_parsed[k] for k in core)                    # as serialised, too


NEW_ROW_FIELDS = {"constraints": {"peak_hour_ending"}, "nodes": {"top_constraints", "hen_name", "region", "hen_name_confirmed"}}


def _strip_new(feed):
    """The feed with every field added in the 2026-11-06 round removed (leaving exactly what was deployed before)."""
    out = {k: v for k, v in feed.items() if k != "freshness"}
    out["constraints"] = [{k: v for k, v in c.items() if k not in NEW_ROW_FIELDS["constraints"]} for c in feed["constraints"]]
    out["nodes"] = {n: {k: v for k, v in e.items() if k not in NEW_ROW_FIELDS["nodes"]} for n, e in feed["nodes"].items()}
    return out


# sha256 of json.dumps(<feed built by the PREVIOUS deployed build_brief_feed.py (commit f6dc7c6) from FIX>, separators=(",", ":")).
# If you edit make_latest()/make_history()/NOW, regenerate it:  git show f6dc7c6:build_brief_feed.py > /tmp/v2.py, import that
# module, hash json.dumps(v2.build(FIX, NOW, FIX_HISTORY), separators=(",", ":")).
GOLDEN_PREVIOUS_FEED_SHA256 = "ea0b92b963e5010a83b4d35fae8b3c2b2e4dfca01ac6bb2f5082ab2a2de4a53b"


def test_every_preexisting_key_is_byte_for_byte_unchanged():
    feed = bf.build(FIX, NOW, FIX_HISTORY)
    stripped = _strip_new(feed)
    digest = hashlib.sha256(json.dumps(stripped, separators=(",", ":")).encode()).hexdigest()
    assert digest == GOLDEN_PREVIOUS_FEED_SHA256, "a pre-existing key (name, order or value) changed"
    # and on the saved file: same top-level order, same per-row field order for the original fields
    full = bf.build(SAVED, NOW, SAVED_HISTORY)
    assert list(full)[:len(CORE_KEYS) + 5] == CORE_KEYS + NEW_KEYS[:-1] and list(full)[-1] == "freshness"
    for n, e in full["nodes"].items():
        assert list(e)[:len(NODE_KEYS)] == NODE_KEYS, n
    for c in full["constraints"]:
        assert list(c)[:9] == ["name", "from_station", "to_station", "avg_shadow", "peak_shadow", "hours_binding",
                               "hen_impact_score", "peak_hour", "top_nodes"]


def test_constraints_rules():
    cons = bf.build(FIX, NOW, FIX_HISTORY)["constraints"]
    assert len(cons) == 8
    avgs = [c["avg_shadow"] for c in cons]
    assert avgs == sorted(avgs, reverse=True)
    usable = sorted((c["avg_shadow"] for c in FIX["constraints"] if c.get("avg_shadow") is not None), reverse=True)
    assert avgs == [round(a, 2) for a in usable[:8]]
    assert "NO_AVG" not in [c["name"] for c in cons]
    for c in cons:
        raw = next(r for r in FIX["constraints"] if r["name"] == c["name"])
        assert len(c["top_nodes"]) <= 3
        order = sorted(raw["node_mcc"].items(), key=lambda t: (-abs(t[1]), t[0]))[:3]
        assert [(t["node"], t["mcc"]) for t in c["top_nodes"]] == [(n, round(v, 2)) for n, v in order]
        if raw["hourly"]:
            top = max(x["avg_shadow"] for x in raw["hourly"].values())
            ties = [int(h) for h, x in raw["hourly"].items() if x["avg_shadow"] == top]
            assert c["peak_hour"] == min(ties) and 1 <= c["peak_hour"] <= 24
        else:
            assert c["peak_hour"] is None
    wide = bf.build(FIX, NOW, FIX_HISTORY, constraints_n=20)["constraints"]
    quiet = next(c for c in wide if c["name"] == "BLESSI_PAVLOV1_1")        # no HEN exposure: still ranks on shadow price
    assert quiet["top_nodes"] == [] and quiet["hen_impact_score"] == 0.0


def test_weather_rules():
    f = bf.build(FIX, NOW, FIX_HISTORY)
    assert len(f["weather"]) == len(CITIES)
    for city, days in f["weather"].items():
        assert len(days) == 4
        assert all(d["date"] > FIX["data_date"] for d in days)
        assert [d["date"] for d in days] == sorted(d["date"] for d in days)
    assert [d["date"] for d in f["weather"]["Austin, TX"]] == ["2026-11-06", "2026-11-07", "2026-11-08", "2026-11-09"]
    assert f["weather"]["Lubbock, TX"][1]["high"] is None                      # pipeline's 0 = missing
    assert len(bf.build(FIX, NOW, FIX_HISTORY, weather_days=3)["weather"]["Austin, TX"]) == 3


def test_nodes_and_hubs_values():
    f = bf.build(FIX, NOW, FIX_HISTORY)
    assert set(f["nodes"]) == set(FIX["sharpe_ratios"]) and len(f["nodes"]) == 32
    n = f["nodes"]["FALFUR_RN"]
    assert n["signal"] == FIX["sharpe_ratios"]["FALFUR_RN"]["signal"]
    assert n["rt_avg"] == round(FIX["rt"]["FALFUR_RN"]["avg"], 2) and n["dart"] == round(FIX["dart"]["FALFUR_RN"], 2)
    assert n["mcc"] == (round(FIX["node_mcc_summary"]["FALFUR_RN"], 2) if "FALFUR_RN" in FIX["node_mcc_summary"] else None)
    L = copy.deepcopy(FIX); del L["dart"]["FALFUR_RN"]; del L["rt"]["FALFUR_RN"]
    n = bf.build(L, NOW, FIX_HISTORY)["nodes"]["FALFUR_RN"]
    assert n["dart"] is None and n["rt_avg"] is None and n["rt_max"] is None and n["rt_min"] is None   # null when missing
    h = f["hubs"]
    rt, da = FIX["hub_zone_rt"]["HB_WEST"], FIX["hub_zone_da"]["HB_WEST"]
    assert list(h) == sorted(HUBS)
    assert h["HB_WEST"] == {"rt_avg": round(rt["avg"], 2), "rt_max": round(rt["max"], 2), "rt_min": round(rt["min"], 2),
                            "da_avg": round(da["avg"], 2)}
    L = copy.deepcopy(FIX); L["hub_zone_da"] = {}
    assert all(v["da_avg"] is None for v in bf.build(L, NOW, FIX_HISTORY)["hubs"].values())


def _measure(text):
    """The binding size: the largest of the file's characters, bytes, and len(json.dumps(...)) (how the brief measures)."""
    return max(len(text), len(text.encode()), len(json.dumps(json.loads(text))))


def test_size_trim_order():
    full_feed, full_text, note = bf.render(FIX, NOW, FIX_HISTORY)
    assert note == ""
    original = bf.MAX_BYTES
    try:
        bf.MAX_BYTES = _measure(full_text)              # exactly at the limit is "not under" -> weather goes to 3 days first
        feed, text, note = bf.render(FIX, NOW, FIX_HISTORY)
        assert note == "weather cut to 3 days" and len(feed["constraints"]) == 8
        assert all(len(d) == 3 for d in feed["weather"].values())
        bf.MAX_BYTES = _measure(text)                   # smaller still -> constraints to 6 as well
        feed2, text2, note2 = bf.render(FIX, NOW, FIX_HISTORY)
        assert note2 == "weather cut to 3 days, constraints cut to 6" and len(feed2["constraints"]) == 6
        assert feed2["data_flags"]["constraints_count"] == 6                    # flags describe the final file
        assert all(len(d) == 3 for d in feed2["weather"].values())
        assert any(len(e["top_constraints"]) == 2 for e in feed2["nodes"].values())
        bf.MAX_BYTES = _measure(text2)                  # last resort: each node's top_constraints cut to 1
        feed3, text3, note3 = bf.render(FIX, NOW, FIX_HISTORY)
        assert note3 == "weather cut to 3 days, constraints cut to 6, top_constraints cut to 1 per node"
        assert all(len(e["top_constraints"]) <= 1 for e in feed3["nodes"].values())
        assert feed3["nodes"]["CEDRVALE_RN"]["top_constraints"] == [{"name": "SNDSW_MR1H", "mcc": -14.0}]   # keeps the biggest
        bf.MAX_BYTES = _measure(text3)                  # impossible -> fail loudly rather than publish an oversize file
        try:
            bf.render(FIX, NOW, FIX_HISTORY)
            raise AssertionError("expected ValueError")
        except ValueError as exc:
            assert "even after trimming" in str(exc)
    finally:
        bf.MAX_BYTES = original
    assert bf.render(FIX, NOW, FIX_HISTORY)[2] == ""                             # limit restored: no trimming needed


def test_market_data_only():
    _, text, _ = bf.render(SAVED, NOW, SAVED_HISTORY)
    forbidden_keys = {"modo", "asset_status", "battery", "ending_soc", "revenue", "revenue_mw_year", "state_of_charge", "soc",
                      "dispatch", "performance", "fleet", "fleet_summary", "node_coords", "regions", "market_breakdown"}
    keys = {k for k, _ in _walk(json.loads(text))}
    assert not (keys & forbidden_keys), keys & forbidden_keys
    for sentinel in ("revenue", "state_of_charge", "ending_soc", "fleet_summary"):
        assert sentinel not in text, sentinel
    if not USING_SAVED:                                                      # fixture sentinels (HEN-only values)
        for sentinel in ("71.5", "20608", "HEN 2026"):
            assert sentinel not in text, sentinel
    assert bf.build(SAVED, NOW, SAVED_HISTORY)["data_flags"]["history_regional_empty_for_data_date"] in (True, False, None)


def test_a_broken_section_never_breaks_the_feed():
    core = bf.build_core(FIX, NOW)
    for key, junk in (("constraints", "garbage"), ("weather", ["x"]), ("sharpe_ratios", 5), ("hub_zone_rt", "oops")):
        L = copy.deepcopy(FIX)
        L[key] = junk
        bf._WARNED.clear()
        feed, _ = _quiet(bf.build, L, NOW, FIX_HISTORY)
        assert list(feed) == CORE_KEYS + NEW_KEYS
        assert all(feed[k] == core[k] for k in core)                         # existing sections unaffected
    orig = bf.build_weather                                                  # an exception inside a builder is contained
    bf.build_weather = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    try:
        bf._WARNED.clear()
        feed, out = _quiet(bf.build, FIX, NOW, FIX_HISTORY)
    finally:
        bf.build_weather = orig
    assert feed["weather"] == {} and feed["data_flags"]["weather_city_count"] == 0
    assert "::warning::" in out and "weather" in out and len(feed["constraints"]) == 8


def test_deterministic_and_valid_json():
    a, b = bf.render(FIX, NOW, FIX_HISTORY)[1], bf.render(FIX, NOW, FIX_HISTORY)[1]
    assert a == b
    json.loads(a)


def test_cli_end_to_end():
    tmp = tempfile.mkdtemp()
    src, hist, dst = (os.path.join(tmp, "latest.json"), os.path.join(tmp, "history.json"),
                      os.path.join(tmp, "dashboard", "brief_feed.json"))
    json.dump(FIX, open(src, "w")); json.dump(FIX_HISTORY, open(hist, "w"))
    script = os.path.join(os.path.dirname(os.path.abspath(bf.__file__)), "build_brief_feed.py")
    run = lambda: subprocess.run([sys.executable, script, "--in", src, "--history", hist, "--out", dst], capture_output=True, text=True)
    p = run()
    assert p.returncode == 0, p.stderr
    assert "8 constraints" in p.stdout and "14 weather cities" in p.stdout and "32 nodes" in p.stdout and "8 hubs" in p.stdout
    good = open(dst).read(); json.loads(good)
    bad = copy.deepcopy(FIX); bad["gross_load_hourly"] = {"1": 99999}           # implausible units -> existing guard -> exit 1
    json.dump(bad, open(src, "w"))
    p = run()
    assert p.returncode == 1 and "FAILED" in p.stderr and open(dst).read() == good          # previous file untouched


def _now(y=2026, m=11, d=6, h=6, mi=12):
    return datetime(y, m, d, h, mi, tzinfo=bf.CT)


# -- nodes[].top_constraints ----------------------------------------------------------------------
def test_top_constraints_uses_the_full_list_and_at_most_two_per_node():
    f = bf.build(FIX, NOW, FIX_HISTORY)
    in_feed = {c["name"] for c in f["constraints"]}
    assert "WESTEX_X" not in in_feed and "KLNSW_MR1H" not in in_feed        # outside the feed's top 8 by avg_shadow ...
    assert f["nodes"]["CEDRVALE_RN"]["top_constraints"] == [{"name": "SNDSW_MR1H", "mcc": -14.0},
                                                            {"name": "WESTEX_X", "mcc": -6.0}]     # ... but still found here
    sndsw = next(c for c in f["constraints"] if c["name"] == "SNDSW_MR1H")
    assert "CEDRVALE_RN" not in [t["node"] for t in sndsw["top_nodes"]]      # the original blind spot
    for node, e in f["nodes"].items():                                        # every node, from the raw full list
        expect = sorted(((c["name"], v) for c in FIX["constraints"] for n, v in (c.get("node_mcc") or {}).items()
                         if n == node and abs(v) >= 0.005), key=lambda t: (-abs(t[1]), t[0]))[:2]
        assert [(t["name"], t["mcc"]) for t in e["top_constraints"]] == [(n, round(v, 2)) for n, v in expect], node
        assert len(e["top_constraints"]) <= 2
        assert all(list(t) == ["name", "mcc"] and round(t["mcc"], 2) == t["mcc"] for t in e["top_constraints"])
    on_saved = bf.build(SAVED, NOW, SAVED_HISTORY)["nodes"]                  # and on the saved file
    names = {c.get("name") for c in SAVED.get("constraints") or []}
    for node, e in on_saved.items():
        assert len(e["top_constraints"]) <= 2
        assert all(t["name"] in names for t in e["top_constraints"]), node
        mags = [abs(t["mcc"]) for t in e["top_constraints"]]
        assert mags == sorted(mags, reverse=True)


def test_top_constraints_is_empty_when_there_is_none():
    L = copy.deepcopy(FIX)
    for c in L["constraints"]:
        (c.get("node_mcc") or {}).pop("FTDUNCAN_RN", None)                   # no constraint touches this node any more
    assert bf.build(L, NOW, FIX_HISTORY)["nodes"]["FTDUNCAN_RN"]["top_constraints"] == []
    L = copy.deepcopy(FIX)                                                    # a contribution that rounds to 0.00 is not listed:
    for c in L["constraints"]:                                                # TINY_C touches FTDUNCAN_RN with -0.003 only
        if c["name"] != "TINY_C":
            (c.get("node_mcc") or {}).pop("FTDUNCAN_RN", None)
    assert any(c["name"] == "TINY_C" and "FTDUNCAN_RN" in c["node_mcc"] for c in L["constraints"])
    assert bf.build(L, NOW, FIX_HISTORY)["nodes"]["FTDUNCAN_RN"]["top_constraints"] == []
    L = copy.deepcopy(FIX); L["constraints"] = []
    assert all(e["top_constraints"] == [] for e in bf.build(L, NOW, FIX_HISTORY)["nodes"].values())


# -- nodes[].hen_name / region / hen_name_confirmed ----------------------------------------------------
def test_hen_names_regions_and_the_unconfirmed_flag():
    nodes = bf.build(FIX, NOW, FIX_HISTORY)["nodes"]
    assert len(EXPECTED_HEN_NAMES) == 32 and set(nodes) == set(EXPECTED_HEN_NAMES)
    for node, e in nodes.items():
        assert e["hen_name"] == EXPECTED_HEN_NAMES[node], node
        want = "South (Fort Duncan)" if node == "FTDUNCAN_RN" else next(r for r, m in REGIONS.items() if node in m)
        assert e["region"] == want, (node, e["region"])
    assert nodes["TOYAH_RN"]["hen_name_confirmed"] is False and nodes["TOYAH_RN"]["hen_name"] == "Cherry Creek"
    assert [n for n, e in nodes.items() if "hen_name_confirmed" in e] == ["TOYAH_RN"]     # this one only
    assert {e["region"] for e in nodes.values()} == {"West Texas", "North Texas", "Coastal", "South (Fort Duncan)", "Premium"}
    L = copy.deepcopy(FIX); del L["regions"]                                  # falls back to the built-in copy of the groups
    assert {n: e["region"] for n, e in bf.build(L, NOW, FIX_HISTORY)["nodes"].items()} == {n: e["region"] for n, e in nodes.items()}
    L = copy.deepcopy(FIX); L["sharpe_ratios"]["NEW_NODE_RN"] = {"sharpe": 0.1, "mean_dart": 1.0, "signal": "NEUTRAL"}
    e = bf.build(L, NOW, FIX_HISTORY)["nodes"]["NEW_NODE_RN"]                  # an unknown node degrades to nulls, no crash
    assert e["hen_name"] is None and e["region"] is None and e["top_constraints"] == [] and "hen_name_confirmed" not in e


# -- constraints[].peak_hour_ending -----------------------------------------------------------------------
def test_peak_hour_ending_is_the_explicit_hour_ending():
    cons = bf.build(SAVED, NOW, SAVED_HISTORY)["constraints"]
    assert cons
    for c in cons:
        assert c["peak_hour_ending"] == c["peak_hour"]                        # keys are already hour-ending (see build_constraints)
        assert c["peak_hour_ending"] is None or 1 <= c["peak_hour_ending"] <= 24
    row = bf.build(FIX, NOW, FIX_HISTORY, constraints_n=20)["constraints"]
    one = next(c for c in row if c["name"] == "WESTEX_X")                      # hourly has only hour-ending 10
    assert one["peak_hour"] == 10 and one["peak_hour_ending"] == 10
    nohourly = next(c for c in row if c["name"] == "6635__G")                  # no hourly detail
    assert nohourly["peak_hour"] is None and nohourly["peak_hour_ending"] is None


# -- freshness ----------------------------------------------------------------------------------------
def test_freshness_is_true_when_every_condition_holds():
    assert bf.build(FIX, NOW, FIX_HISTORY)["freshness"] == {"expected_data_date": "2026-11-05", "is_fresh": True, "stale_reasons": []}


def test_freshness_false_when_data_date_is_wrong():
    for wrong in ("2026-11-04", "2026-11-06"):                                 # a day old, and "from the future"
        fr = bf.build(make_latest(wrong), NOW, make_history(wrong))["freshness"]
        assert fr["is_fresh"] is False and fr["expected_data_date"] == "2026-11-05"
        assert len(fr["stale_reasons"]) >= 1 and any(f"data_date is {wrong}" in r for r in fr["stale_reasons"]), fr


def test_freshness_false_when_generated_before_0600_ct():
    for now, fresh in ((_now(h=5, mi=59), False), (_now(h=6, mi=0), True), (_now(h=0, mi=5), False)):   # 06:00 itself counts as fresh
        fr = bf.build(FIX, now, FIX_HISTORY)["freshness"]
        assert fr["is_fresh"] is fresh, (now, fr)
        if not fresh:
            assert fr["stale_reasons"] == [f"the feed was generated at {now:%H:%M} CT, before the 06:00 CT cutoff, so overnight data may be incomplete"]


def test_freshness_false_when_da_is_empty():
    for key, word in (("da", "day-ahead (DA)"), ("dart", "DART"), ("rt", "real-time (RT)")):
        L = copy.deepcopy(FIX); L[key] = {}
        fr = bf.build(L, NOW, FIX_HISTORY)["freshness"]
        assert fr["is_fresh"] is False and any(word in r and f"{key}_empty is true" in r for r in fr["stale_reasons"]), (key, fr)
    L = copy.deepcopy(FIX); L["da"] = {}
    assert bf.build(L, NOW, FIX_HISTORY)["data_flags"]["da_empty"] is True


def test_freshness_false_for_forecast_run_missing_or_on_another_date_or_sources_missing():
    L = make_latest(gen_utc="2026-11-05T22:00:00Z")                            # forecasts last ran yesterday 16:00 CT
    fr = bf.build(L, NOW, FIX_HISTORY)["freshness"]
    assert fr["is_fresh"] is False and any("not on the same Central date" in r for r in fr["stale_reasons"]), fr
    L = copy.deepcopy(FIX); L["ercot_forecasts"].pop("generated_at")
    fr = bf.build(L, NOW, FIX_HISTORY)["freshness"]
    assert fr["is_fresh"] is False and any("forecast_run_at is missing" in r for r in fr["stale_reasons"]), fr
    L = copy.deepcopy(FIX); L["ercot_forecasts"]["missing_sources"] = ["wind"]; L["ercot_forecasts"]["hourly_24hr"]["wind"] = [0.0] * 216
    fr = bf.build(L, NOW, FIX_HISTORY)["freshness"]
    assert fr["is_fresh"] is False and any("some sources are missing today: wind" in r for r in fr["stale_reasons"]), fr


def test_freshness_gives_one_reason_per_failed_condition():
    L = make_latest("2026-11-04", gen_utc="2026-11-05T22:00:00Z"); L["da"] = {}; L["ercot_forecasts"]["missing_sources"] = ["solar"]
    L["ercot_forecasts"]["hourly_24hr"]["solar"] = [0.0] * 216
    fr = bf.build(L, _now(h=5, mi=30), make_history("2026-11-04"))["freshness"]
    assert fr["is_fresh"] is False
    joined = " | ".join(fr["stale_reasons"])
    for piece in ("data_date is 2026-11-04", "before the 06:00 CT cutoff", "not on the same Central date", "sources are missing today", "day-ahead (DA)"):
        assert piece in joined, (piece, fr)
    assert len(fr["stale_reasons"]) >= 5


def test_freshness_expected_date_is_calendar_yesterday_on_clock_change_days():
    base = {"data_date": None, "forecast_run_at": None, "missing_sources": [], "data_flags": {}}
    cases = [("2026-11-02T06:10:00-06:00", "2026-11-01"),                     # day after the 25-hour day
             ("2026-11-01T06:10:00-06:00", "2026-10-31"),                     # the 25-hour day itself (06:10 is CST by then)
             ("2027-03-15T06:10:00-05:00", "2027-03-14"),                     # day after the 23-hour day
             ("2027-03-14T06:10:00-05:00", "2027-03-13"),                     # the 23-hour day itself
             ("2026-11-06T06:10:00-06:00", "2026-11-05")]
    for gen, expected in cases:
        feed = dict(base, generated_at=gen, data_date=expected, forecast_run_at=gen)
        fr = bf.build_freshness(feed)
        assert fr["expected_data_date"] == expected and fr["is_fresh"] is True, (gen, fr)


def test_a_25_hour_day_passes():
    L = make_latest("2026-11-01", gen_utc="2026-11-02T12:05:00.000000Z")      # 06:05 CST the next morning
    now = datetime(2026, 11, 2, 6, 12, tzinfo=bf.CT)
    feed, text, note = bf.render(L, now, make_history("2026-11-01"))
    rows = [r for r in feed["hourly"] if r["date"] == "2026-11-01"]
    assert [r["he"] for r in rows] == list(range(1, 25))                       # data_date is fully covered ...
    ts = {r["he"]: r["ts_ct"] for r in rows}
    assert ts[1] == "2026-11-01T00:00:00-05:00" and ts[2] == "2026-11-01T01:00:00-05:00" and ts[3] == "2026-11-01T02:00:00-06:00"
    assert all(r["status"] == "actual" for r in rows)                          # ... as actuals, with the offset flip at 02:00
    assert feed["data_date"] == "2026-11-01" and feed["missing_sources"] == []
    assert feed["freshness"] == {"expected_data_date": "2026-11-01", "is_fresh": True, "stale_reasons": []}
    assert "NaN" not in text and len(text) < 50_000 and bf.fits(text)


def test_a_23_hour_day_passes():
    L = make_latest("2027-03-14", gen_utc="2027-03-15T11:05:00.000000Z")      # 06:05 CDT the next morning
    now = datetime(2027, 3, 15, 6, 12, tzinfo=bf.CT)
    feed, text, note = bf.render(L, now, make_history("2027-03-14"))
    rows = [r for r in feed["hourly"] if r["date"] == "2027-03-14"]
    assert [r["he"] for r in rows] == [h for h in range(1, 25) if h != 3]      # HE3 does not exist that day
    assert {r["he"]: r["ts_ct"] for r in rows}[4] == "2027-03-14T03:00:00-05:00"
    assert feed["freshness"]["is_fresh"] is True and bf.fits(text)


# -- plain-python runner --------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"input: {'SAVED FILE ' + LATEST_PATH if USING_SAVED else 'built-in fixture (no saved latest.json found at ' + LATEST_PATH + ')'}")
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"OK    {name}")
        except BaseException as exc:                                # noqa: BLE001
            failed += 1
            print(f"FAIL  {name}: {exc!r}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
