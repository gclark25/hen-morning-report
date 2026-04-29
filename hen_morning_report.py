"""
HEN — Daily ERCOT Morning Report v3
=====================================

Enhancements over v2:
- Integrated AG2 Trader weather + ERCOT load forecasts (wsitrader.com)
- ERCOT binding constraint summary with per-asset shift factors
- Modo Energy custom index performance (4 HEN indices)
- PowerTools asset availability and outage schedule
- Extended AI morning analysis with all new data sources

REQUIRED ENVIRONMENT VARIABLES:

  ERCOT_USERNAME            apiexplorer.ercot.com email
  ERCOT_PASSWORD            apiexplorer.ercot.com password
  ERCOT_SUBSCRIPTION_KEY    API Explorer primary key
  ERCOT_NODES               comma-separated settlement point names

  AG2_ACCOUNT               wsitrader.com username
  AG2_PROFILE               wsitrader.com email address
  AG2_PASSWORD              wsitrader.com password

  MODO_API_KEY              Modo Energy X-Token (modoenergy.com/profile/developers)
  MODO_INDEX_IDS            Optional — "Name:id,Name:id,..." to skip discovery

  POWERTOOLS_URL            Full URL to your PowerTools platform
  POWERTOOLS_API_KEY        PowerTools Bearer token (if API key auth)
  POWERTOOLS_USERNAME       PowerTools username (if basic auth)
  POWERTOOLS_PASSWORD       PowerTools password (if basic auth)

OPTIONAL:
  SENDGRID_API_KEY          SendGrid API key
  FROM_EMAIL                verified sender address
  TO_EMAILS                 comma-separated recipient list
  S3_BUCKET                 archives report + JSON to S3
  ANTHROPIC_API_KEY         Claude AI analysis
"""

import os
import sys
import json
import time
import requests
from datetime import date, timedelta
from urllib.parse import quote
from collections import defaultdict

from hen_integrations import collect_all_integrations

# ── CONFIG ────────────────────────────────────────────────────────────────────

BASE_URL  = "https://api.ercot.com/api/public-reports"
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()
WEEK_AGO  = (date.today() - timedelta(days=7)).isoformat()
TODAY_STR = date.today().isoformat()

_nodes_env = os.environ.get("ERCOT_NODES", "").strip()
NODES = (
    [n.strip() for n in _nodes_env.split(",") if n.strip()]
    if _nodes_env
    else ["HB_BUSAVG", "HB_HOUSTON", "HB_NORTH", "HB_SOUTH", "HB_WEST"]
)

# Regional groupings — passed into latest.json for dashboard rendering
REGIONS = {
    "West Texas": [
        "TOYAH_RN","SADLBACK_RN","FAULKNER_RN","COYOTSPR_RN","LONESTAR_RN",
        "RTLSNAKE_BT","CEDRVALE_RN","SBEAN_BESS","GOMZ_RN","GRDNE_ESR_RN",
        "JDKNS_RN","SANDLAKE_RN"
    ],
    "North Texas": [
        "OLNEYTN_RN","DIBOL_RN","FRMRSVLW_RN","MNWL_BESS_RN","LFSTH_RN",
        "PAULN_RN","CISC_RN"
    ],
    "Coastal": [
        "MV_VALV4_RN","WLTC_ESR_RN","MAINLAND_RN","FALFUR_RN","PAVLOV_BT_RN",
        "POTEETS_RN","TYNAN_RN"
    ],
    "Premium": [
        "CATARINA_B1","HOLCOMB_RN1","HAMI_BESS_RN","JUNCTION_RN",
        "RUSSEKST_RN","FTDUNCAN_RN"
    ],
}

# Approximate lat/lon for each node — for Texas map on dashboard
NODE_COORDS = {
    "TOYAH_RN":      (31.32, -103.80),
    "SADLBACK_RN":   (31.10, -103.50),
    "FAULKNER_RN":   (31.45, -103.20),
    "COYOTSPR_RN":   (30.95, -103.65),
    "LONESTAR_RN":   (31.60, -102.90),
    "RTLSNAKE_BT":   (31.75, -102.70),
    "CEDRVALE_RN":   (31.20, -102.80),
    "SBEAN_BESS":    (31.35, -102.50),
    "GOMZ_RN":       (31.55, -102.20),
    "GRDNE_ESR_RN":  (31.80, -101.90),
    "JDKNS_RN":      (32.10, -101.60),
    "SANDLAKE_RN":   (31.65, -101.40),
    "OLNEYTN_RN":    (33.37,  -98.75),
    "DIBOL_RN":      (33.10,  -98.50),
    "FRMRSVLW_RN":   (33.65,  -98.10),
    "MNWL_BESS_RN":  (33.20,  -97.90),
    "LFSTH_RN":      (33.55,  -97.65),
    "PAULN_RN":      (33.80,  -97.40),
    "CISC_RN":       (32.85,  -98.00),
    "MV_VALV4_RN":   (28.70,  -97.10),
    "WLTC_ESR_RN":   (28.45,  -97.40),
    "MAINLAND_RN":   (29.55,  -95.10),
    "FALFUR_RN":     (27.22,  -98.14),
    "PAVLOV_BT_RN":  (27.80,  -97.50),
    "POTEETS_RN":    (29.05,  -98.57),
    "TYNAN_RN":      (28.20,  -97.80),
    "CATARINA_B1":   (28.35,  -99.62),
    "HOLCOMB_RN1":   (32.70, -102.10),
    "HAMI_BESS_RN":  (31.68, -100.13),
    "JUNCTION_RN":   (30.49,  -99.77),
    "RUSSEKST_RN":   (29.85,  -98.50),
    "FTDUNCAN_RN":   (29.37, -100.44),
}

# ── AUTH ──────────────────────────────────────────────────────────────────────

def get_token(username, password, sub_key):
    AUTH_URL = (
        "https://ercotb2c.b2clogin.com"
        "/ercotb2c.onmicrosoft.com"
        "/B2C_1_PUBAPI-ROPC-FLOW"
        "/oauth2/v2.0/token"
        "?username={u}&password={p}"
        "&grant_type=password"
        "&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access"
        "&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70"
        "&response_type=id_token"
    )
    r = requests.post(
        AUTH_URL.format(u=quote(username, safe=""), p=quote(password, safe="")),
        headers={"Ocp-Apim-Subscription-Key": sub_key},
        timeout=15,
    )
    r.raise_for_status()
    d = r.json()
    return d.get("id_token") or d.get("access_token")

# ── ERCOT API ─────────────────────────────────────────────────────────────────

def ercot_get(path, token, sub_key, params=None):
    headers = {
        "Authorization":             f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": sub_key,
        "Accept":                    "application/json",
    }
    p = {"size": 1000}
    if params:
        p.update(params)
    r = requests.get(f"{BASE_URL}/{path}", headers=headers, params=p, timeout=20)
    r.raise_for_status()
    body = r.json()
    if isinstance(body, list):
        return body
    if "data" in body:
        return body["data"]
    for v in body.values():
        if isinstance(v, list):
            return v
    return []

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0

def extract_price_with_interval(row):
    """
    Extract (hour, price) from an RT price row.
    RT row positional format: [deliveryDate, hour, interval, settlementPoint, type, price, ...]
    Returns (hour_int, price_float) or (None, None)
    """
    if isinstance(row, list) and len(row) >= 6:
        try:
            hour  = int(row[1])
            nums  = [x for x in row if isinstance(x, (int, float))
                     and not isinstance(x, bool) and x != 0]
            price = nums[-1] if nums else None
            return hour, price
        except (ValueError, TypeError):
            return None, None
    elif isinstance(row, dict):
        hour  = int(row.get("deliveryHour", 0))
        price = safe_float(row.get("settlementPointPrice") or
                           row.get("spp") or row.get("price") or 0)
        return hour, price if price != 0 else None
    return None, None

def extract_da_price_with_hour(row):
    """
    Extract (hour, price) from a DA price row.
    DA row positional format: [deliveryDate, deliveryHour, settlementPoint, price, ...]
    Returns (hour_int, price_float) or (None, None)
    """
    if isinstance(row, list) and len(row) >= 4:
        try:
            hour  = int(row[1]) if not isinstance(row[1], str) else int(row[1].split(":")[0])
            nums  = [x for x in row[2:] if isinstance(x, (int, float))
                     and not isinstance(x, bool)]
            price = nums[-1] if nums else None
            return hour, price
        except (ValueError, TypeError):
            return None, None
    elif isinstance(row, dict):
        hour  = int(row.get("deliveryHour", 0))
        price = safe_float(row.get("settlementPointPrice") or
                           row.get("spp") or row.get("price") or 0)
        return hour, price if price != 0 else None
    return None, None

# ── DATA COLLECTION ───────────────────────────────────────────────────────────

def collect_data(token, sub_key):
    data = {}

    # ── Gross load — hourly ──────────────────────────────────────────────────
    print("  Pulling gross load (hourly)...")
    try:
        rows = ercot_get("np6-345-cd/act_sys_load_by_wzn", token, sub_key)
        by_day_hour = defaultdict(lambda: defaultdict(list))
        by_day      = defaultdict(list)
        for row in rows:
            if not isinstance(row, list) or len(row) < 3:
                continue
            d = str(row[0])[:10]
            try:
                hr = int(str(row[1]).split(":")[0])
            except (ValueError, AttributeError):
                hr = 0
            nums = [x for x in row[1:] if isinstance(x, (int, float))
                    and not isinstance(x, bool)]
            val = nums[-1] if nums else 0
            if d and val:
                by_day_hour[d][hr].append(float(val))
                by_day[d].append(float(val))
        data["gross_load"] = {d: round(max(v) / 1000, 1) for d, v in by_day.items() if v}
        if by_day_hour:
            latest_load_day = sorted(by_day_hour.keys())[-1]
            data["gross_load_hourly"] = {
                str(hr): round(
                    sum(by_day_hour[latest_load_day][hr]) /
                    len(by_day_hour[latest_load_day][hr]) / 1000, 1
                )
                for hr in sorted(by_day_hour[latest_load_day].keys())
            }
        print(f"    {len(data['gross_load'])} days · hourly curve built")
    except Exception as e:
        print(f"    WARN: load failed — {e}")
        data["gross_load"]        = {}
        data["gross_load_hourly"] = {}

    # ── Wind — hourly ────────────────────────────────────────────────────────
    print("  Pulling wind generation (hourly)...")
    try:
        rows = ercot_get("np4-732-cd/wpp_hrly_avrg_actl_fcast", token, sub_key,
                         {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY})
        by_day      = defaultdict(list)
        by_day_hour = defaultdict(dict)
        for row in rows:
            if not isinstance(row, list) or len(row) < 4:
                continue
            d   = str(row[1])[:10]
            hr  = int(row[2]) if isinstance(row[2], (int, float)) else 0
            val = safe_float(row[3])
            if d and val:
                by_day[d].append(val)
                by_day_hour[d][hr] = round(val / 1000, 1)
        data["wind"] = {d: round(max(v) / 1000, 1) for d, v in by_day.items() if v}
        if by_day_hour:
            latest_wind_day = sorted(by_day_hour.keys())[-1]
            data["wind_hourly"] = {
                str(hr): by_day_hour[latest_wind_day][hr]
                for hr in sorted(by_day_hour[latest_wind_day].keys())
            }
        print(f"    {len(data['wind'])} days · hourly curve built")
    except Exception as e:
        print(f"    WARN: wind failed — {e}")
        data["wind"]        = {}
        data["wind_hourly"] = {}

    # ── Solar — hourly ───────────────────────────────────────────────────────
    print("  Pulling solar generation (hourly)...")
    try:
        rows = ercot_get("np4-737-cd/spp_hrly_avrg_actl_fcast", token, sub_key,
                         {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY})
        by_day      = defaultdict(list)
        by_day_hour = defaultdict(dict)
        for row in rows:
            if not isinstance(row, list) or len(row) < 4:
                continue
            d   = str(row[1])[:10]
            hr  = int(row[2]) if isinstance(row[2], (int, float)) else 0
            val = safe_float(row[3])
            if d and val:
                by_day[d].append(val)
                by_day_hour[d][hr] = round(val / 1000, 1)
        data["solar"] = {d: round(max(v) / 1000, 1) for d, v in by_day.items() if v}
        if by_day_hour:
            latest_solar_day = sorted(by_day_hour.keys())[-1]
            data["solar_hourly"] = {
                str(hr): by_day_hour[latest_solar_day][hr]
                for hr in sorted(by_day_hour[latest_solar_day].keys())
            }
        print(f"    {len(data['solar'])} days · hourly curve built")
    except Exception as e:
        print(f"    WARN: solar failed — {e}")
        data["solar"]        = {}
        data["solar_hourly"] = {}

    # ── RT + DA prices — hourly per node ────────────────────────────────────
    print(f"  Pulling RT + DA prices for {len(NODES)} nodes (hourly)...")
    rt_summary = {}
    rt_hourly  = {}
    da_summary = {}
    da_hourly  = {}

    for node in NODES:
        time.sleep(3)

        # RT prices — aggregate 4 x 15-min intervals into hourly averages
        try:
            rows = ercot_get("np6-905-cd/spp_node_zone_hub", token, sub_key,
                             {"settlementPoint":   node,
                              "deliveryDateFrom":  YESTERDAY,
                              "deliveryDateTo":    YESTERDAY})
            hour_buckets = defaultdict(list)
            all_prices   = []
            for row in rows:
                hr, price = extract_price_with_interval(row)
                if hr is not None and price is not None:
                    hour_buckets[hr].append(price)
                    all_prices.append(price)
            if all_prices:
                rt_summary[node] = {
                    "avg": round(sum(all_prices) / len(all_prices), 2),
                    "max": round(max(all_prices), 2),
                    "min": round(min(all_prices), 2),
                }
                rt_hourly[node] = {
                    str(hr): round(sum(v) / len(v), 2)
                    for hr, v in sorted(hour_buckets.items())
                }
        except Exception as e:
            print(f"    WARN: RT {node} — {e}")

        time.sleep(3)

        # DA prices — already hourly
        try:
            rows = ercot_get("np4-190-cd/dam_stlmnt_pnt_prices", token, sub_key,
                             {"settlementPoint":   node,
                              "deliveryDateFrom":  YESTERDAY,
                              "deliveryDateTo":    YESTERDAY})
            hour_prices = {}
            all_prices  = []
            for row in rows:
                hr, price = extract_da_price_with_hour(row)
                if hr is not None and price is not None:
                    hour_prices[hr] = price
                    all_prices.append(price)
            if all_prices:
                da_summary[node] = {
                    "avg": round(sum(all_prices) / len(all_prices), 2),
                    "max": round(max(all_prices), 2),
                }
                da_hourly[node] = {
                    str(hr): round(p, 2)
                    for hr, p in sorted(hour_prices.items())
                }
        except Exception as e:
            print(f"    WARN: DA {node} — {e}")

    data["rt"]       = rt_summary
    data["rt_hourly"] = rt_hourly
    data["da"]       = da_summary
    data["da_hourly"] = da_hourly

    # DART spreads — daily avg (DA − RT: positive = DA premium over RT)
    common = set(rt_summary) & set(da_summary)
    data["dart"] = {
        n: round(da_summary[n]["avg"] - rt_summary[n]["avg"], 2)
        for n in common
    }

    # Hourly DART spreads per node (DA − RT per hour)
    dart_hourly = {}
    for n in common:
        rth = rt_hourly.get(n, {})
        dah = da_hourly.get(n, {})
        shared_hrs = set(rth) & set(dah)
        if shared_hrs:
            dart_hourly[n] = {
                hr: round(dah[hr] - rth[hr], 2)
                for hr in sorted(shared_hrs)
            }
    data["dart_hourly"] = dart_hourly

    print(f"  RT: {len(rt_summary)} nodes DA: {len(da_summary)} nodes "
          f"DART hourly: {len(dart_hourly)} nodes")

    # ── Additional integrations ────────────────────────────────────────────
    print("\n── Collecting additional integrations ──")
    extras = collect_all_integrations(
        token=token,
        sub_key=sub_key,
        asset_nodes=NODES,
    )
    data.update(extras)

    return data

# ── TOP / BOTTOM ANALYSIS ─────────────────────────────────────────────────────

def compute_top_bottom(data):
    """
    For each node compute:
    - DART avg       = avg(DA hourly) − avg(RT hourly)  [positive = DA premium]
    - Intraday spread = (max RT hour − min RT hour) / 24 [$/MWh normalized]
    - Best DART hour  = hour with highest (DA − RT)
    - Worst DART hour = hour with lowest  (DA − RT)

    Returns Top 10 and Bottom 10 ranked by DART avg, plus regional summary.
    """
    dart       = data.get("dart", {})
    dart_hourly = data.get("dart_hourly", {})
    rt_hourly  = data.get("rt_hourly", {})
    da_hourly  = data.get("da_hourly", {})

    node_analysis = {}
    for node in dart:
        dh = dart_hourly.get(node, {})
        rh = rt_hourly.get(node, {})
        dah = da_hourly.get(node, {})
        if not dh or not rh:
            continue

        rt_values = list(rh.values())
        best_hr   = max(dh, key=dh.get)
        worst_hr  = min(dh, key=dh.get)
        neg_hrs   = [hr for hr, v in rh.items() if v < 0]
        spike_hrs = [hr for hr, v in rh.items() if v > 100]

        intraday_spread = round(
            (max(rt_values) - min(rt_values)) / 24, 2
        ) if rt_values else 0

        da_values = list(dah.values()) if dah else []

        node_analysis[node] = {
            "dart_avg":       dart[node],
            "intraday_spread": intraday_spread,
            "best_hour":      int(best_hr),
            "best_spread":    dh[best_hr],
            "worst_hour":     int(worst_hr),
            "worst_spread":   dh[worst_hr],
            "rt_max":         round(max(rt_values), 2) if rt_values else 0,
            "rt_min":         round(min(rt_values), 2) if rt_values else 0,
            "da_avg":         round(sum(da_values)/len(da_values), 2) if da_values else 0,
            "neg_hours":      len(neg_hrs),
            "spike_hours":    len(spike_hrs),
            "region":         next((r for r, nodes in REGIONS.items()
                                    if node in nodes), "Other"),
        }

    ranked  = sorted(node_analysis.items(),
                     key=lambda x: x[1]["dart_avg"], reverse=True)
    top10   = [{"node": n, **v} for n, v in ranked[:10]]
    bottom10 = [{"node": n, **v} for n, v in ranked[-10:]][::-1]

    regional = {}
    for region in REGIONS:
        region_nodes = [n for n in dart if n in REGIONS[region]]
        if region_nodes:
            spreads = [dart[n] for n in region_nodes]
            regional[region] = {
                "avg_dart":  round(sum(spreads) / len(spreads), 2),
                "best_node": max(region_nodes, key=lambda n: dart[n]),
                "best_dart": round(max(spreads), 2),
                "node_count": len(region_nodes),
            }

    return {"top10": top10, "bottom10": bottom10, "regional": regional}

# ── REPORT BUILDER ────────────────────────────────────────────────────────────

# ── NEW SECTION BUILDERS ─────────────────────────────────────────────────────

def _build_modo_html(data):
    """Build Modo Energy section: bar chart + table side by side."""
    modo    = data.get("modo", {})
    indices = modo.get("indices", {})
    if not indices:
        return ""

    # Find max value for bar scaling
    max_val = max((v.get("revenue_mw_year", 0) for v in indices.values()), default=1) or 1

    # Bar chart rows
    bars = ""
    table_rows = ""
    for v in indices.values():
        rev   = v.get("revenue_mw_year", 0)
        pct   = round(rev / max_val * 100, 1)
        name  = v.get("display_name", "")
        start = v.get("window_start", "")
        end   = v.get("window_end", "")
        ndays = v.get("n_days", 0)
        bars += f"""
        <div class="bar-row">
          <div class="bar-label" title="{name}">{name}</div>
          <div class="bar-track"><div class="bar-fill" style="width:{pct}%"></div></div>
          <div class="bar-val">${rev:,.0f}</div>
        </div>"""

        # Market breakdown for table
        bk    = v.get("market_breakdown", {})
        top3  = sorted(bk.items(), key=lambda x: x[1], reverse=True)[:3]
        bk_str = " / ".join(f"{m}: ${r:,.0f}" for m, r in top3) if top3 else "—"
        table_rows += f"""
        <tr>
          <td class="node-name">{name}</td>
          <td style="font-family:monospace">${rev:,.0f}</td>
          <td style="font-size:10px;color:#666">{start} → {end}</td>
          <td style="font-size:10px">{ndays}d</td>
          <td style="font-size:10px;color:#555">{bk_str}</td>
        </tr>"""

    data_date = modo.get("data_date", "")
    return f"""
    <div class="section" style="margin-top:20px">
      <div class="section-title">Modo Energy Custom Indices — 2026 YTD
        <span style="font-weight:400;font-size:10px;color:#aaa;margin-left:8px">
          Annualised $/MW/yr · Latest settled data through {data_date}
        </span>
      </div>
      <div class="modo-grid">
        <div class="chart-wrap">
          <div class="chart-title">$/MW/yr — YTD Average (Annualised)</div>
          {bars}
        </div>
        <div>
          <table style="width:100%">
            <thead><tr>
              <th style="text-align:left">Index</th>
              <th>$/MW/yr</th>
              <th>Window</th>
              <th>Days</th>
              <th>Top Markets</th>
            </tr></thead>
            <tbody>{table_rows}</tbody>
          </table>
        </div>
      </div>
    </div>"""


def _build_weather_html(data):
    """Build AG2 weather section: city cards with 7-day temp + precip."""
    wx     = data.get("weather", {})
    cities = wx.get("cities", {})
    if not cities:
        return ""

    cards = ""
    for city_name, city_data in sorted(cities.items()):
        days = city_data.get("days", [])[:7]
        if not days:
            continue
        rows = ""
        for d in days:
            precip = f'<span class="precip">{d["precip_pct"]}%</span>' if d.get("precip_pct") else ""
            rows += f"""
          <div class="city-row">
            <span class="city-date">{d["date"][5:]}</span>
            <span>{d["high"]}° / {d["low"]}°</span>
            {precip}
          </div>"""
        cards += f"""
        <div class="city-card">
          <div class="city-name">{city_name}</div>
          {rows}
        </div>"""

    source     = wx.get("source", "AG2 Trader")
    gen_at     = wx.get("generated_at", "")[:10]
    return f"""
    <div class="section" style="margin-top:20px">
      <div class="section-title">15-Day Weather Forecast — ERCOT Metro Stations
        <span style="font-weight:400;font-size:10px;color:#aaa;margin-left:8px">
          Hi°F / Lo°F · Precip % · Source: {source} · As of {gen_at}
        </span>
      </div>
      <div class="weather-grid">{cards}</div>
    </div>"""


def _build_ai_html(data):
    """Build AI analysis section from dashboard/ai_analysis.json if available."""
    import json, os
    try:
        with open("dashboard/ai_analysis.json", "r", encoding="utf-8") as f:
            ai_data = json.load(f)
        text = (ai_data.get("morning") or {}).get("analysis", "").strip()
        if not text:
            return ""
        # Escape HTML special chars
        text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return f"""
    <div class="section" style="margin-top:20px">
      <div class="section-title">AI Morning Analysis
        <span style="font-weight:400;font-size:10px;color:#aaa;margin-left:8px">
          Generated by Claude · {YESTERDAY}
        </span>
      </div>
      <div class="ai-box"><div class="ai-text">{text}</div></div>
    </div>"""
    except Exception:
        return ""


def _build_forecast_html(data):
    """Build ERCOT 7-day forward forecast section for the HTML report."""
    fc = data.get("ercot_forecasts", {})
    d7 = fc.get("daily_7day", {})
    if not d7 or not d7.get("dates"):
        return ""

    dates          = d7.get("dates", [])
    load_peaks     = d7.get("gross_load_peak", [])
    wind_avgs      = d7.get("wind_avg", [])
    solar_peaks    = d7.get("solar_peak", [])
    net_load_peaks = d7.get("net_load_peak", [])

    max_load = max(load_peaks) if load_peaks else 1

    rows = ""
    chart_bars = ""
    for i, day in enumerate(dates):
        gl  = load_peaks[i]     if i < len(load_peaks)     else 0
        wnd = wind_avgs[i]      if i < len(wind_avgs)      else 0
        sol = solar_peaks[i]    if i < len(solar_peaks)    else 0
        net = net_load_peaks[i] if i < len(net_load_peaks) else 0
        pct = round(gl / max_load * 100, 1) if max_load else 0
        net_pct = round(net / max_load * 100, 1) if max_load else 0
        dow = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"][
            __import__("datetime").date.fromisoformat(day).weekday()
        ]
        rows += f"""
        <tr>
          <td>{dow} {day[5:]}</td>
          <td style="font-family:monospace">{gl:.1f}</td>
          <td style="font-family:monospace;color:#1a7a3f">{wnd:.1f}</td>
          <td style="font-family:monospace;color:#c87800">{sol:.1f}</td>
          <td style="font-family:monospace;font-weight:600">{net:.1f}</td>
        </tr>"""
        chart_bars += f"""
        <div class="bar-row">
          <div class="bar-label" style="width:75px">{dow} {day[5:]}</div>
          <div class="bar-track">
            <div class="bar-fill" style="width:{pct}%;background:#dde8f5"></div>
            <div class="bar-fill" style="width:{net_pct}%;background:#0055aa;margin-top:-14px"></div>
          </div>
          <div class="bar-val" style="width:120px;font-size:10px">
            {gl:.1f} GW / net {net:.1f}
          </div>
        </div>"""

    h24 = fc.get("hourly_24hr", {})
    h24_note = ""
    if h24.get("timestamps"):
        n = len(h24["timestamps"])
        peak_load = max(h24.get("gross_load", [0]))
        peak_net  = max(h24.get("net_load", [0]))
        h24_note = (f"  24-hr outlook: peak gross load {peak_load:.1f} GW · "
                    f"peak net load {peak_net:.1f} GW over {n} hours")

    gen_date = fc.get("forecast_date", "")
    return f"""
    <div class="section" style="margin-top:20px">
      <div class="section-title">ERCOT 7-Day Forward Forecast
        <span style="font-weight:400;font-size:10px;color:#aaa;margin-left:8px">
          Daily peak GW · Gross load (grey) vs Net load (blue) · Source: ERCOT · As of {gen_date}
        </span>
      </div>
      <div style="margin-bottom:12px">{chart_bars}</div>
      <table>
        <thead><tr>
          <th style="text-align:left">Date</th>
          <th>Peak Gross Load</th>
          <th>Wind Avg</th>
          <th>Solar Peak</th>
          <th>Peak Net Load</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
      {"<div style='font-size:11px;color:#888;margin-top:8px'>" + h24_note + "</div>" if h24_note else ""}
    </div>"""


def build_report(data):
    rt    = data.get("rt", {})
    da    = data.get("da", {})
    dart  = data.get("dart", {})
    load  = data.get("gross_load", {})
    wind  = data.get("wind", {})
    solar = data.get("solar", {})
    tb    = compute_top_bottom(data)

    all_rt_avg = [v["avg"] for v in rt.values()] if rt else [0]
    fleet_avg  = round(sum(all_rt_avg) / len(all_rt_avg), 2) if all_rt_avg else 0
    fleet_max  = round(max(v["max"] for v in rt.values()), 2) if rt else 0
    spike_nodes = [n for n, v in rt.items() if v["max"] > 100]
    neg_nodes   = [n for n, v in rt.items() if v["min"] < 0]
    best_dart   = max(dart, key=dart.get) if dart else None
    worst_dart  = min(dart, key=dart.get) if dart else None

    shared_days = sorted(set(load) & set(wind) & set(solar))[-7:]

    fund_rows = ""
    for d in shared_days:
        g   = load.get(d, 0)
        w   = wind.get(d, 0)
        s   = solar.get(d, 0)
        net = round(g - w - s, 1)
        flag = "charging-window" if net < 30 else ""
        fund_rows += f"""
        <tr class="{flag}">
          <td>{d}</td><td>{g:.1f}</td><td>{w:.1f}</td>
          <td>{s:.1f}</td><td>{net:.1f}</td>
        </tr>"""

    price_rows = ""
    for node in sorted(rt.keys(), key=lambda n: rt[n]["avg"], reverse=True):
        r      = rt.get(node, {})
        dv     = da.get(node, {})
        sp     = dart.get(node)
        region = next((reg for reg, nodes in REGIONS.items() if node in nodes), "Other")
        spike_cls = ' class="spike"' if r.get("max", 0) > 100 else ""
        neg_cls   = ' class="neg"'   if r.get("min", 0) < 0   else ""
        dart_cls  = (' class="rt-prem"' if (sp or 0) > 5 else
                     ' class="da-prem"' if (sp or 0) < -5 else "")
        sp_str = (f"+${sp:.2f}" if sp and sp > 0 else
                  f"-${abs(sp):.2f}" if sp else "—")
        price_rows += f"""
        <tr>
          <td class="node-name">{node}</td>
          <td class="region-tag region-{region.lower().replace(' ','-')}">{region}</td>
          <td{spike_cls}>${r.get('avg',0):.2f}</td>
          <td{neg_cls}>${r.get('min',0):.2f}</td>
          <td{spike_cls}>${r.get('max',0):.2f}</td>
          <td>${dv.get('avg',0):.2f}</td>
          <td{dart_cls}>{sp_str}</td>
        </tr>"""

    top_rows = ""
    for item in tb["top10"]:
        top_rows += f"""
        <tr>
          <td class="node-name">{item['node']}</td>
          <td class="region-tag region-{item['region'].lower().replace(' ','-')}">{item['region']}</td>
          <td class="da-prem">+${item['dart_avg']:.2f}</td>
          <td>${item['intraday_spread']:.2f}</td>
          <td>HE {item['best_hour']:02d}:00</td>
          <td class="da-prem">+${item['best_spread']:.2f}</td>
          <td>{item['spike_hours']} hrs</td>
          <td>{item['neg_hours']} hrs</td>
        </tr>"""

    bot_rows = ""
    for item in tb["bottom10"]:
        bot_rows += f"""
        <tr>
          <td class="node-name">{item['node']}</td>
          <td class="region-tag region-{item['region'].lower().replace(' ','-')}">{item['region']}</td>
          <td class="rt-prem">${item['dart_avg']:.2f}</td>
          <td>${item['intraday_spread']:.2f}</td>
          <td>HE {item['worst_hour']:02d}:00</td>
          <td class="rt-prem">${item['worst_spread']:.2f}</td>
          <td>{item['spike_hours']} hrs</td>
          <td>{item['neg_hours']} hrs</td>
        </tr>"""

    best_str  = f"{best_dart} +${dart[best_dart]:.2f}/MWh"  if best_dart  else "N/A"
    worst_str = f"{worst_dart} ${dart[worst_dart]:.2f}/MWh" if worst_dart else "N/A"
    dow = date.today().strftime("%A")

    # Build new integration sections
    modo_html     = _build_modo_html(data)
    weather_html  = _build_weather_html(data)
    forecast_html = _build_forecast_html(data)
    ai_html       = _build_ai_html(data)

    html = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>HEN Morning Report — {YESTERDAY}</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:#f4f5f7;margin:0;padding:24px 16px;color:#1a1a2e}}
.outer{{max-width:900px;margin:0 auto}}
.header{{background:#0a3d2e;border-radius:10px 10px 0 0;padding:20px 28px;
         display:flex;justify-content:space-between;align-items:center}}
.header h1{{margin:0;font-size:20px;color:#fff;font-weight:600}}
.header p{{margin:4px 0 0;font-size:12px;color:#7fc8a0}}
.header-right .date{{font-size:13px;color:#b8dfc8;font-family:monospace}}
.header-right .gen{{font-size:11px;color:#5a9e78;margin-top:2px}}
.body{{background:#fff;padding:0 0 24px}}
.kpi-strip{{display:grid;grid-template-columns:repeat(4,1fr);border-bottom:1px solid #eee}}
.kpi{{padding:16px 18px;border-right:1px solid #eee}}
.kpi:last-child{{border-right:none}}
.kpi-label{{font-size:10px;font-weight:600;color:#888;text-transform:uppercase;
            letter-spacing:.05em;margin-bottom:6px}}
.kpi-value{{font-size:22px;font-weight:600;color:#1a1a2e;font-family:monospace}}
.kpi-sub{{font-size:11px;color:#888;margin-top:3px}}
.section{{padding:20px 24px 0}}
.section-title{{font-size:11px;font-weight:700;color:#888;text-transform:uppercase;
                letter-spacing:.07em;margin-bottom:12px;padding-bottom:8px;
                border-bottom:1px solid #f0f0f0}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{background:#f8f9fa;padding:7px 10px;text-align:right;font-weight:600;
    color:#555;font-size:11px;border-bottom:2px solid #e8e8e8}}
th:first-child,th:nth-child(2){{text-align:left}}
td{{padding:7px 10px;text-align:right;border-bottom:1px solid #f4f4f4;color:#2a2a3e}}
td:first-child{{text-align:left}}
td:nth-child(2){{text-align:left}}
tr:hover td{{background:#fafbfc}}
.node-name{{font-family:monospace;font-size:11px;color:#444}}
.spike{{color:#b33000;font-weight:600}}
.neg{{color:#0066cc}}
.rt-prem{{color:#1a7a3f;font-weight:600}}
.da-prem{{color:#7a3a1a}}
.charging-window td{{background:#f0faf4}}
.region-tag{{font-size:10px;padding:2px 6px;border-radius:3px;display:inline-block;white-space:nowrap}}
.region-west-texas{{background:#e8f0fe;color:#1a3a8a}}
.region-north-texas{{background:#e8f5e9;color:#1a5c2a}}
.region-coastal{{background:#e3f2fd;color:#0d47a1}}
.region-premium{{background:#fce4ec;color:#880e4f}}
.callout-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:14px}}
.callout{{background:#f8f9fa;border-radius:6px;padding:12px 14px;border-left:3px solid #ddd}}
.callout.green{{border-left-color:#1a7a3f}}
.callout.amber{{border-left-color:#c87800}}
.callout.red{{border-left-color:#b33000}}
.callout.blue{{border-left-color:#0055aa}}
.callout-label{{font-size:10px;font-weight:700;color:#888;text-transform:uppercase;
                letter-spacing:.05em;margin-bottom:4px}}
.callout-value{{font-size:12px;color:#1a1a2e;font-family:monospace}}
.footer{{background:#f8f9fa;border-radius:0 0 10px 10px;padding:12px 24px;
         display:flex;justify-content:space-between;border-top:1px solid #eee;
         font-size:11px;color:#aaa}}
.modo-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:12px}}
.chart-wrap{{background:#f8f9fa;border-radius:6px;padding:12px 14px}}
.chart-title{{font-size:10px;font-weight:700;color:#888;text-transform:uppercase;
              letter-spacing:.05em;margin-bottom:8px}}
.bar-row{{display:flex;align-items:center;margin-bottom:6px;gap:8px}}
.bar-label{{font-size:11px;color:#444;width:185px;flex-shrink:0;
            white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.bar-track{{flex:1;background:#e8e8e8;border-radius:3px;height:14px}}
.bar-fill{{height:14px;border-radius:3px;background:#1a7a3f}}
.bar-val{{font-size:10px;font-family:monospace;color:#444;width:90px;
          text-align:right;flex-shrink:0}}
.weather-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(195px,1fr));
               gap:10px;margin-top:12px}}
.city-card{{background:#f8f9fa;border-radius:6px;padding:10px 12px}}
.city-name{{font-size:10px;font-weight:700;color:#555;text-transform:uppercase;
            letter-spacing:.04em;margin-bottom:5px}}
.city-row{{display:flex;justify-content:space-between;align-items:center;
           font-size:11px;color:#333;padding:2px 0;border-bottom:1px solid #eeeeee}}
.city-row:last-child{{border-bottom:none}}
.city-date{{color:#888;font-size:10px;font-family:monospace}}
.precip{{color:#0066cc;font-size:10px}}
.ai-box{{background:#f0f7f4;border-radius:6px;padding:14px 16px;margin-top:12px;
         border-left:3px solid #1a7a3f}}
.ai-text{{font-size:12px;color:#2a2a3e;line-height:1.65;white-space:pre-wrap}}
</style>
</head><body>
<div class="outer">
  <div class="header">
    <div><h1>Hunt Energy Network</h1>
    <p>ERCOT Commercial Morning Report</p></div>
    <div class="header-right">
      <div class="date">{dow}, {YESTERDAY}</div>
      <div class="gen">Generated {TODAY_STR} · Data through 24:00 CT</div>
    </div>
  </div>
  <div class="body">
    <div class="kpi-strip">
      <div class="kpi"><div class="kpi-label">Fleet avg RT</div>
        <div class="kpi-value">${fleet_avg:.2f}</div>
        <div class="kpi-sub">$/MWh · {len(rt)} nodes</div></div>
      <div class="kpi"><div class="kpi-label">Fleet peak RT</div>
        <div class="kpi-value">${fleet_max:.2f}</div>
        <div class="kpi-sub">$/MWh highest interval</div></div>
      <div class="kpi"><div class="kpi-label">Spike events</div>
        <div class="kpi-value">{len(spike_nodes)}</div>
        <div class="kpi-sub">nodes &gt;$100/MWh</div></div>
      <div class="kpi"><div class="kpi-label">Negative nodes</div>
        <div class="kpi-value">{len(neg_nodes)}</div>
        <div class="kpi-sub">charging opportunities</div></div>
    </div>

    <div class="section">
      <div class="section-title">Key signals</div>
      <div class="callout-grid">
        <div class="callout green"><div class="callout-label">Best DART</div>
          <div class="callout-value">{best_str}</div></div>
        <div class="callout amber"><div class="callout-label">Largest DA premium</div>
          <div class="callout-value">{worst_str}</div></div>
        <div class="callout red"><div class="callout-label">Spike nodes</div>
          <div class="callout-value">{', '.join(spike_nodes) or 'None'}</div></div>
        <div class="callout blue"><div class="callout-label">Negative price nodes</div>
          <div class="callout-value">{', '.join(neg_nodes) or 'None'}</div></div>
      </div>
    </div>

    <div class="section" style="margin-top:20px">
      <div class="section-title">Top 10 DART performers — {YESTERDAY}
        <span style="font-weight:400;font-size:10px;color:#aaa;margin-left:8px">
          DART = DA avg − RT avg · Intraday = (RT max − RT min) / 24
        </span>
      </div>
      <table><thead><tr>
        <th>Node</th><th>Region</th><th>DART avg</th>
        <th>Intraday $/MWh</th><th>Best DART hour</th><th>Best spread</th>
        <th>Spike hrs</th><th>Neg hrs</th>
      </tr></thead><tbody>{top_rows}</tbody></table>
    </div>

    <div class="section" style="margin-top:20px">
      <div class="section-title">Bottom 10 DART performers — {YESTERDAY}</div>
      <table><thead><tr>
        <th>Node</th><th>Region</th><th>DART avg</th>
        <th>Intraday $/MWh</th><th>Worst DART hour</th><th>Worst spread</th>
        <th>Spike hrs</th><th>Neg hrs</th>
      </tr></thead><tbody>{bot_rows}</tbody></table>
    </div>

    <div class="section" style="margin-top:20px">
      <div class="section-title">RT vs DA prices by node — {YESTERDAY}</div>
      <table><thead><tr>
        <th>Node</th><th>Region</th><th>RT avg</th>
        <th>RT min</th><th>RT max</th><th>DA avg</th><th>DART</th>
      </tr></thead><tbody>{price_rows}</tbody></table>
    </div>

    <div class="section" style="margin-top:20px">
      <div class="section-title">ERCOT fundamentals — 7-day lookback (GW daily peak)</div>
      <table><thead><tr>
        <th>Date</th><th>Gross load</th><th>Wind</th>
        <th>Solar</th><th>Net load</th>
      </tr></thead><tbody>{fund_rows or
        '<tr><td colspan="5" style="text-align:center;color:#aaa;padding:16px">'
        'Data not available — ERCOT publishes after 8 AM CT</td></tr>'}
      </tbody></table>
    </div>

    {modo_html}

    {weather_html}

    {forecast_html}

    {ai_html}

  </div>

  <div class="footer">
    <span>Hunt Energy Network · Commercial Operations · Confidential</span>
    <span style="font-family:monospace">ERCOT Public API · {TODAY_STR}</span>
  </div>
</div>
</body></html>"""

    return html

# ── EMAIL VIA SENDGRID ────────────────────────────────────────────────────────

def send_email(html, subject, from_addr, to_addrs, api_key):
    payload = {
        "personalizations": [{"to": [{"email": a} for a in to_addrs],
                               "subject": subject}],
        "from":    {"email": from_addr, "name": "HEN Morning Report"},
        "content": [{"type": "text/html", "value": html}],
    }
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type":  "application/json"},
        json=payload, timeout=30,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid {r.status_code}: {r.text[:300]}")
    print(f"  Email sent to {len(to_addrs)} recipient(s)")

# ── S3 ARCHIVE ────────────────────────────────────────────────────────────────

def archive_to_s3(html, data_json, bucket):
    try:
        import boto3
    except ImportError:
        print("  SKIP: boto3 not installed")
        return
    s3 = boto3.client("s3")
    yr, mo, dy = YESTERDAY[:4], YESTERDAY[5:7], YESTERDAY[8:]
    s3.put_object(Bucket=bucket,
                  Key=f"reports/{yr}/{mo}/{dy}/morning-report.html",
                  Body=html.encode("utf-8"), ContentType="text/html")
    s3.put_object(Bucket=bucket,
                  Key=f"raw-data/{YESTERDAY}/ercot-public.json",
                  Body=json.dumps(data_json, indent=2).encode("utf-8"),
                  ContentType="application/json")
    print(f"  Archived to s3://{bucket}/reports/{yr}/{mo}/{dy}/")

# ── DASHBOARD JSON ────────────────────────────────────────────────────────────

def write_dashboard_json(data):
    tb = compute_top_bottom(data)
    payload = {
        "data_date":          YESTERDAY,
        "generated_at":       TODAY_STR,
        "regions":            REGIONS,
        "node_coords":        NODE_COORDS,
        "rt":                 data.get("rt", {}),
        "rt_hourly":          data.get("rt_hourly", {}),
        "da":                 data.get("da", {}),
        "da_hourly":          data.get("da_hourly", {}),
        "dart":               data.get("dart", {}),
        "dart_hourly":        data.get("dart_hourly", {}),
        "gross_load":         data.get("gross_load", {}),
        "gross_load_hourly":  data.get("gross_load_hourly", {}),
        "wind":               data.get("wind", {}),
        "wind_hourly":        data.get("wind_hourly", {}),
        "solar":              data.get("solar", {}),
        "solar_hourly":       data.get("solar_hourly", {}),
        "top_bottom":         tb,
        # ── New integration data ─────────────────────────────────────────
        "constraints":        data.get("constraints", []),
        "weather":            data.get("weather", {}),
        "modo":               data.get("modo", {}),
        "asset_status":       data.get("asset_status", {}),
        # ── ERCOT forward forecasts ──────────────────────────────────────────
        "ercot_forecasts":    data.get("ercot_forecasts", {}),
        "as_prices":          data.get("as_prices", {}),
    }
    with open("latest.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print("  Dashboard data written to latest.json")

# ── HISTORY JSON ─────────────────────────────────────────────────────────────

def _calc_daily_ending_soc(rt, data, token=None, sub_key=None, soc_start=50.0):
    """
    Calculate the implied ending SOC for yesterday using actual ERCOT ESR
    charging MW data. Falls back to price-inference if API pull fails.
    """
    ERCOT_BESS_CAPACITY_MW = 14000.0
    SOC_STEP_PER_MW_HR     = 100.0 / ERCOT_BESS_CAPACITY_MW

    if token and sub_key:
        try:
            ESR_BASE = "https://api.ercot.com/api/public-data"
            headers  = {
                "Authorization":             f"Bearer {token}",
                "Ocp-Apim-Subscription-Key": sub_key,
                "Accept":                    "application/json",
            }
            params = {
                "AGCExecTimeUTCFrom": f"{YESTERDAY}T06:00:00Z",
                "AGCExecTimeUTCTo":   f"{TODAY_STR}T05:59:59Z",
                "size": 10000,
            }
            r = requests.get(f"{ESR_BASE}/rptesr-m/4_sec_esr_charging_mw",
                             headers=headers, params=params, timeout=30)
            if r.ok:
                body = r.json()
                rows = body if isinstance(body, list) else body.get("data", [])
                if rows:
                    by_hour = defaultdict(list)
                    for row in rows:
                        if isinstance(row, dict):
                            mw_val   = (row.get("ESRChargingMW") or
                                        row.get("esrChargingMw") or
                                        row.get("esrchargingmw"))
                            exec_utc = (row.get("AGCExecTimeUTC") or
                                        row.get("agcExecTimeUTC") or
                                        row.get("agcexectimeutc") or "")
                        elif isinstance(row, list) and len(row) >= 2:
                            nums     = [x for x in row if isinstance(x, (int, float))
                                        and not isinstance(x, bool)]
                            mw_val   = nums[-1] if nums else None
                            exec_utc = str(row[0]) if row else ""
                        else:
                            continue
                        if mw_val is None:
                            continue
                        try:
                            if "T" in str(exec_utc):
                                utc_hr = int(str(exec_utc).split("T")[1].split(":")[0])
                                ct_hr  = (utc_hr - 5) % 24
                                he     = ct_hr + 1
                                by_hour[he].append(float(mw_val))
                        except Exception:
                            pass
                    if by_hour:
                        soc = soc_start
                        for he in range(1, 25):
                            vals = by_hour.get(he, [])
                            if not vals:
                                continue
                            avg_mw = sum(vals) / len(vals)
                            delta  = -avg_mw * SOC_STEP_PER_MW_HR
                            soc    = max(0, min(100, soc + delta))
                        ending = round(soc, 1)
                        print(f"  ESR SOC calc: {len(rows)} samples · ending SOC = {ending}%")
                        return ending
        except Exception as e:
            print(f"  WARN: ESR SOC pull failed — {e}, falling back to price inference")

    print("  SOC calc: using price inference fallback")
    CHARGE_THRESHOLD    = 15.0
    DISCHARGE_THRESHOLD = 50.0
    SOC_STEP            = 4.0
    rt_hourly           = data.get("rt_hourly", {})
    soc = soc_start
    for hr in range(1, 25):
        vals = [rt_hourly.get(node, {}).get(str(hr))
                for node in rt_hourly
                if rt_hourly.get(node, {}).get(str(hr)) is not None]
        if not vals:
            continue
        price = sum(vals) / len(vals)
        if price < 0:
            delta = SOC_STEP * 1.5
        elif price < CHARGE_THRESHOLD:
            delta = SOC_STEP
        elif price < DISCHARGE_THRESHOLD:
            delta = 0
        elif price < 100:
            delta = -SOC_STEP
        else:
            delta = -SOC_STEP * 1.5
        soc = max(0, min(100, soc + delta))
    return round(soc, 1)


def write_history_json(data, history_path="dashboard/history.json", token=None, sub_key=None):
    """
    Maintains a rolling 5-day history file.
    Each day's entry contains fleet-level and per-node summaries.
    """
    rt    = data.get("rt", {})
    da    = data.get("da", {})
    dart  = data.get("dart", {})
    load  = data.get("gross_load", {})
    wind  = data.get("wind", {})
    solar = data.get("solar", {})
    tb    = compute_top_bottom(data)

    load_val  = load.get(YESTERDAY, 0)
    wind_val  = wind.get(YESTERDAY, 0)
    solar_val = solar.get(YESTERDAY, 0)
    net_val   = round(load_val - wind_val - solar_val, 1) if load_val else 0

    nodes_snapshot = {}
    for node in dart:
        r      = rt.get(node, {})
        dv     = da.get(node, {})
        tb_node = next((n for n in tb.get("top10", []) + tb.get("bottom10", [])
                        if n["node"] == node), {})
        nodes_snapshot[node] = {
            "dart":      dart[node],
            "intraday":  tb_node.get("intraday_spread", 0),
            "rt_avg":    r.get("avg", 0),
            "rt_max":    r.get("max", 0),
            "rt_min":    r.get("min", 0),
            "da_avg":    dv.get("avg", 0),
            "region":    next((reg for reg, nodes in REGIONS.items()
                               if node in nodes), "Other"),
        }

    regional_snapshot = {}
    for region, nodes in REGIONS.items():
        rn = [n for n in nodes if n in dart]
        if rn:
            regional_snapshot[region] = {
                "avg_dart": round(sum(dart[n] for n in rn) / len(rn), 2),
                "avg_rt":   round(sum(rt.get(n, {}).get("avg", 0) for n in rn) / len(rn), 2),
                "avg_intraday": round(sum(
                    next((x["intraday_spread"] for x in
                          tb.get("top10", []) + tb.get("bottom10", [])
                          if x["node"] == n), 0)
                    for n in rn) / len(rn), 2),
            }

    today_entry = {
        "date": YESTERDAY,
        "fleet": {
            "rt_avg":      round(sum(v["avg"] for v in rt.values()) / len(rt), 2) if rt else 0,
            "rt_max":      round(max(v["max"] for v in rt.values()), 2) if rt else 0,
            "spike_nodes": len([n for n, v in rt.items() if v["max"] > 100]),
            "neg_nodes":   len([n for n, v in rt.items() if v["min"] < 0]),
        },
        "fundamentals": {
            "gross_load": load_val,
            "wind":       wind_val,
            "solar":      solar_val,
            "net_load":   net_val,
        },
        "nodes":    nodes_snapshot,
        "regional": regional_snapshot,
        "battery":  {
            "ending_soc": _calc_daily_ending_soc(rt, data, token=token, sub_key=sub_key),
        },
    }

    history = []
    try:
        with open(history_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    history = [e for e in history if e.get("date") != YESTERDAY]
    history.append(today_entry)
    history = sorted(history, key=lambda e: e["date"])[-5:]

    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)
    print(f"  History updated — {len(history)} days stored in {history_path}")

# ── AI PROMPT HELPERS ─────────────────────────────────────────────────────────

def _fmt_constraints(data):
    constraints = data.get("constraints", [])
    if not constraints:
        return "  Not available"
    lines = []
    for c in constraints:
        sf_str = ", ".join(
            f"{n}:{v:+.2f}" for n, v in list(c.get("shift_factors", {}).items())[:5]
        )
        lines.append(
            f"  {c['name']}: ${c['avg_shadow']:.2f}/MWh shadow · "
            f"{c['hours_binding']}h binding · {c['flow_direction']} · "
            f"shift factors: {sf_str}"
        )
    return "\n".join(lines)


def _fmt_weather(data):
    wx     = data.get("weather", {})
    cities = wx.get("cities", {})
    if not cities:
        return "  Not available"
    lines = []
    # Show first 7 days for each city
    for city_name, city_data in cities.items():
        days = city_data.get("days", [])[:7]
        if not days:
            continue
        lines.append(f"  {city_name} ({city_data.get('station', '')}):")
        for d in days:
            precip = f" {d['precip_pct']}% precip" if d.get("precip_pct") else ""
            lines.append(
                f"    {d['date']}: Hi {d['high']}°F / Lo {d['low']}°F{precip}"
            )
    return "\n".join(lines)


def _fmt_modo(data):
    modo = data.get("modo", {})
    if not modo or modo.get("error"):
        return f"  Not available ({modo.get('error', 'Modo not connected')})"
    indices = modo.get("indices", {})
    if not indices:
        return "  No index data returned"
    lines = []
    for key, v in indices.items():
        n = v.get("n_days", 0)
        lines.append(
            f"  {v['display_name']}: ${v['revenue_mw_year']:,.0f}/MW/yr "
            f"({v['window_start']} → {v['window_end']}, {n} days)"
        )
        if v.get("market_breakdown"):
            breakdown = ", ".join(
                f"{mkt}: ${rev:,.0f}"
                for mkt, rev in sorted(
                    v["market_breakdown"].items(),
                    key=lambda x: x[1], reverse=True
                )[:3]
            )
            lines.append(f"    Breakdown: {breakdown}")
    return "\n".join(lines)


def _fmt_asset_status(data):
    ast = data.get("asset_status", {})
    if not ast or ast.get("error"):
        return f"  Not available ({ast.get('error', 'PowerTools not connected')})"
    fs = ast.get("fleet_summary", {})
    lines = [
        f"  Fleet: {fs.get('online', 0)} online / {fs.get('total_assets', 0)} total · "
        f"{fs.get('fleet_availability_pct', 0)}% available · "
        f"{fs.get('available_mw', 0)} MW of {fs.get('total_capacity_mw', 0)} MW",
        f"  Planned outages: {fs.get('planned_outage_mw', 0)} MW · "
        f"Forced outages: {fs.get('forced_outage_mw', 0)} MW",
    ]
    for o in ast.get("outage_schedule", []):
        lines.append(
            f"  OUTAGE: {o['asset']} · {o['type']} · {o['mw']} MW · "
            f"{o['start']} → {o['end']} · {o['reason']}"
        )
    return "\n".join(lines)

# ── AI ANALYSIS ───────────────────────────────────────────────────────────────

def build_ai_prompt_morning(data, history):
    """Build the prompt for the morning AI analysis."""
    tb    = compute_top_bottom(data)
    rt    = data.get("rt", {})
    da    = data.get("da", {})
    dart  = data.get("dart", {})
    load  = data.get("gross_load", {})
    wind  = data.get("wind", {})
    solar = data.get("solar", {})

    all_rt_avg  = [v["avg"] for v in rt.values()] if rt else [0]
    fleet_avg   = round(sum(all_rt_avg) / len(all_rt_avg), 2) if all_rt_avg else 0
    fleet_max   = round(max(v["max"] for v in rt.values()), 2) if rt else 0
    spike_nodes = [n for n, v in rt.items() if v["max"] > 100]
    neg_nodes   = [n for n, v in rt.items() if v["min"] < 0]
    best_dart   = max(dart, key=dart.get) if dart else None
    worst_dart  = min(dart, key=dart.get) if dart else None

    top5_dart = sorted(dart.items(), key=lambda x: x[1], reverse=True)[:5]
    bot5_dart = sorted(dart.items(), key=lambda x: x[1])[:5]

    load_val  = load.get(YESTERDAY, "N/A")
    wind_val  = wind.get(YESTERDAY, "N/A")
    solar_val = solar.get(YESTERDAY, "N/A")

    hist_summary = ""
    if history:
        for entry in sorted(history, key=lambda e: e["date"])[-3:]:
            hist_summary += (
                f"  {entry['date']}: fleet RT avg ${entry['fleet'].get('rt_avg', 0):.2f}, "
                f"spike nodes {entry['fleet'].get('spike_nodes', 0)}, "
                f"neg nodes {entry['fleet'].get('neg_nodes', 0)}\n"
            )

    prompt = f"""You are a commercial energy analyst for Hunt Energy Network (HEN), operator of 32 BESS sites across ERCOT. Generate a concise but comprehensive morning analysis for {YESTERDAY}.

ERCOT PRICE SUMMARY:
- Fleet avg RT: ${fleet_avg}/MWh across {len(rt)} nodes
- Fleet peak RT: ${fleet_max}/MWh
- Spike nodes (>$100/MWh): {spike_nodes if spike_nodes else 'None'}
- Negative price nodes: {neg_nodes if neg_nodes else 'None'}
- Best DART: {best_dart} at ${dart.get(best_dart, 0):.2f}/MWh if best_dart else 'N/A'
- Largest DA premium: {worst_dart} at ${dart.get(worst_dart, 0):.2f}/MWh if worst_dart else 'N/A'

TOP 5 DART PERFORMERS:
{chr(10).join(f"  {n}: ${v:.2f}/MWh" for n, v in top5_dart)}

BOTTOM 5 DART PERFORMERS:
{chr(10).join(f"  {n}: ${v:.2f}/MWh" for n, v in bot5_dart)}

ERCOT FUNDAMENTALS ({YESTERDAY}):
- Gross load: {load_val} GW
- Wind: {wind_val} GW
- Solar: {solar_val} GW

RECENT HISTORY (last 3 days):
{hist_summary or '  No history available'}

---
TOP-5 BINDING CONSTRAINTS (ERCOT):
{_fmt_constraints(data)}

15-DAY WEATHER OUTLOOK (AG2 Trader):
{_fmt_weather(data)}

MODO ENERGY CUSTOM INDICES:
{_fmt_modo(data)}

ASSET AVAILABILITY & OUTAGES (PowerTools):
{_fmt_asset_status(data)}
---

Please provide:
1. PERFORMANCE SUMMARY: Key highlights from yesterday — what drove outperformance or underperformance across the fleet
2. CONSTRAINT ANALYSIS: Which constraints mattered most and their impact on HEN nodes by shift factor
3. WEATHER & LOAD OUTLOOK: What the 15-day forecast means for ERCOT pricing and HEN dispatch over the next 2 weeks
4. MODO INDEX CONTEXT: How HEN's custom indices performed relative to prior day; what the market breakdown reveals
5. ASSET AVAILABILITY: Any outage impacts on the fleet and operational risk flags
6. FORWARD OPPORTUNITIES: Top 3 specific actionable opportunities in the next 7-14 days based on all available data
7. RISK FLAGS: Any structural concerns worth escalating to the trading desk

Be specific, use numbers, and focus on commercially actionable insights. Keep each section to 3-5 sentences.
"""
    return prompt


def run_ai_analysis(data, history, api_key):
    """Call Claude API with the morning prompt and return structured analysis."""
    prompt = build_ai_prompt_morning(data, history)
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-sonnet-4-6",
                "max_tokens": 2000,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()
        return {"generated_at": TODAY_STR, "analysis": text, "data_date": YESTERDAY}
    except Exception as e:
        print(f"  WARN: AI analysis failed — {e}")
        return {}


def write_ai_analysis_json(analysis, path="dashboard/ai_analysis.json"):
    """Write or update the AI analysis JSON file used by the dashboard."""
    existing = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    existing["morning"] = analysis
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)
    print(f"  AI analysis written to {path}")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\nHEN Morning Report — {YESTERDAY}")
    print(f"Nodes: {len(NODES)}")

    username = os.environ.get("ERCOT_USERNAME", "")
    password = os.environ.get("ERCOT_PASSWORD", "")
    sub_key  = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")

    if not all([username, password, sub_key]):
        print("ERROR: Missing ERCOT credentials")
        sys.exit(1)

    # ── Authenticate ──────────────────────────────────────────────────────
    try:
        token = get_token(username, password, sub_key)
        print(f"  Auth: token obtained")
    except Exception as e:
        print(f"  Auth FAILED: {e}")
        sys.exit(1)

    # ── Collect all data (ERCOT + integrations) ───────────────────────────
    print("\nCollecting data...")
    data = collect_data(token, sub_key)

    # ── Load history for AI context ───────────────────────────────────────
    history = []
    try:
        with open("dashboard/history.json", "r", encoding="utf-8") as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # ── Build HTML report ─────────────────────────────────────────────────
    print("\nBuilding report...")
    html    = build_report(data)
    subject = f"HEN Morning Report — {YESTERDAY}"

    with open("morning_report.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("  Report saved to morning_report.html")

    # ── Write dashboard JSON ──────────────────────────────────────────────
    print("\nWriting dashboard data...")
    write_dashboard_json(data)

    # ── Write history JSON ────────────────────────────────────────────────
    write_history_json(data, token=token, sub_key=sub_key)

    # ── Run AI analysis ───────────────────────────────────────────────────
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if anthropic_key:
        print("\nGenerating AI analysis...")
        analysis = run_ai_analysis(data, history, anthropic_key)
        if analysis:
            write_ai_analysis_json(analysis)

    # ── S3 archive ────────────────────────────────────────────────────────
    s3_bucket = os.environ.get("S3_BUCKET", "")
    if s3_bucket:
        archive_to_s3(html, data, s3_bucket)

    print(f"\nDone. Report generated for {YESTERDAY}.")


if __name__ == "__main__":
    main()
