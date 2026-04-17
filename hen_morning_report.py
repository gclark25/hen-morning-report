"""
HEN — Daily ERCOT Morning Report v2
=====================================
Enhancements over v1:
  - Hourly RT and DA price curves per node (96 intervals → 24 hourly averages)
  - Top/Bottom DART spread analysis with regional groupings
  - Hourly gross load, wind, solar curves for duck curve visibility
  - Regional node map data for dashboard Texas map
  - Full latest.json schema for dashboard click-through node view

REQUIRED ENVIRONMENT VARIABLES:
  ERCOT_USERNAME          apiexplorer.ercot.com email
  ERCOT_PASSWORD          apiexplorer.ercot.com password
  ERCOT_SUBSCRIPTION_KEY  API Explorer primary key
  ERCOT_NODES             comma-separated settlement point names

OPTIONAL:
  SENDGRID_API_KEY        SendGrid API key
  FROM_EMAIL              verified sender address
  TO_EMAILS               comma-separated recipient list
  S3_BUCKET               archives report + JSON to S3
"""

import os
import sys
import json
import time
import requests
from datetime import date, timedelta
from urllib.parse import quote
from collections import defaultdict

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
    "TOYAH_RN":     (31.32, -103.80),
    "SADLBACK_RN":  (31.10, -103.50),
    "FAULKNER_RN":  (31.45, -103.20),
    "COYOTSPR_RN":  (30.95, -103.65),
    "LONESTAR_RN":  (31.60, -102.90),
    "RTLSNAKE_BT":  (31.75, -102.70),
    "CEDRVALE_RN":  (31.20, -102.80),
    "SBEAN_BESS":   (31.35, -102.50),
    "GOMZ_RN":      (31.55, -102.20),
    "GRDNE_ESR_RN": (31.80, -101.90),
    "JDKNS_RN":     (32.10, -101.60),
    "SANDLAKE_RN":  (31.65, -101.40),
    "OLNEYTN_RN":   (33.37, -98.75),
    "DIBOL_RN":     (33.10, -98.50),
    "FRMRSVLW_RN":  (33.65, -98.10),
    "MNWL_BESS_RN": (33.20, -97.90),
    "LFSTH_RN":     (33.55, -97.65),
    "PAULN_RN":     (33.80, -97.40),
    "CISC_RN":      (32.85, -98.00),
    "MV_VALV4_RN":  (28.70, -97.10),
    "WLTC_ESR_RN":  (28.45, -97.40),
    "MAINLAND_RN":  (29.55, -95.10),
    "FALFUR_RN":    (27.22, -98.14),
    "PAVLOV_BT_RN": (27.80, -97.50),
    "POTEETS_RN":   (29.05, -98.57),
    "TYNAN_RN":     (28.20, -97.80),
    "CATARINA_B1":  (28.35, -99.62),
    "HOLCOMB_RN1":  (32.70, -102.10),
    "HAMI_BESS_RN": (31.68, -100.13),
    "JUNCTION_RN":  (30.49, -99.77),
    "RUSSEKST_RN":  (29.85, -98.50),
    "FTDUNCAN_RN":  (29.37, -100.44),
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
        "Authorization": f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": sub_key,
        "Accept": "application/json",
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
    Extract (hour, interval, price) from an RT price row.
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
            d    = str(row[0])[:10]
            # row[1] is the hour string e.g. "14:00"
            try:
                hr = int(str(row[1]).split(":")[0])
            except (ValueError, AttributeError):
                hr = 0
            nums = [x for x in row[1:] if isinstance(x, (int, float))
                    and not isinstance(x, bool)]
            val  = nums[-1] if nums else 0
            if d and val:
                by_day_hour[d][hr].append(float(val))
                by_day[d].append(float(val))
        # Daily peak (GW)
        data["gross_load"] = {d: round(max(v) / 1000, 1)
                              for d, v in by_day.items() if v}
        # Hourly curve for most recent day (GW)
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
        data["gross_load"] = {}
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
        data["wind"] = {}
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
        data["solar"] = {}
        data["solar_hourly"] = {}

    # ── RT + DA prices — hourly per node ────────────────────────────────────
    print(f"  Pulling RT + DA prices for {len(NODES)} nodes (hourly)...")
    rt_summary = {}   # node → {avg, max, min}
    rt_hourly  = {}   # node → {hour: avg_price}
    da_summary = {}   # node → {avg, max}
    da_hourly  = {}   # node → {hour: price}

    for node in NODES:
        time.sleep(2)
        # RT prices — aggregate 4 x 15-min intervals into hourly averages
        try:
            rows = ercot_get("np6-905-cd/spp_node_zone_hub", token, sub_key,
                             {"settlementPoint": node,
                              "deliveryDateFrom": YESTERDAY,
                              "deliveryDateTo":   YESTERDAY})
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

        time.sleep(2)
        # DA prices — already hourly
        try:
            rows = ercot_get("np4-190-cd/dam_stlmnt_pnt_prices", token, sub_key,
                             {"settlementPoint": node,
                              "deliveryDateFrom": YESTERDAY,
                              "deliveryDateTo":   YESTERDAY})
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

    data["rt"]         = rt_summary
    data["rt_hourly"]  = rt_hourly
    data["da"]         = da_summary
    data["da_hourly"]  = da_hourly

    # DART spreads — daily avg and hourly
    common = set(rt_summary) & set(da_summary)
    data["dart"] = {
        n: round(rt_summary[n]["avg"] - da_summary[n]["avg"], 2)
        for n in common
    }

    # Hourly DART spreads per node
    dart_hourly = {}
    for n in common:
        rth = rt_hourly.get(n, {})
        dah = da_hourly.get(n, {})
        shared_hrs = set(rth) & set(dah)
        if shared_hrs:
            dart_hourly[n] = {
                hr: round(rth[hr] - dah[hr], 2)
                for hr in sorted(shared_hrs)
            }
    data["dart_hourly"] = dart_hourly

    print(f"    RT: {len(rt_summary)} nodes  DA: {len(da_summary)} nodes  "
          f"DART hourly: {len(dart_hourly)} nodes")

    return data

# ── TOP / BOTTOM ANALYSIS ─────────────────────────────────────────────────────

def compute_top_bottom(data):
    """
    For each node compute:
      - Best hour (highest DART spread in $/MWh)
      - Worst hour (lowest DART spread)
      - Daily DART avg
    Returns sorted lists for Top 10 and Bottom 10 across all nodes,
    plus a regional summary.
    """
    dart        = data.get("dart", {})
    dart_hourly = data.get("dart_hourly", {})
    rt_hourly   = data.get("rt_hourly", {})

    node_analysis = {}
    for node in dart:
        dh = dart_hourly.get(node, {})
        rh = rt_hourly.get(node, {})
        if not dh:
            continue
        best_hr  = max(dh, key=dh.get)
        worst_hr = min(dh, key=dh.get)
        neg_hrs  = [hr for hr, v in rh.items() if v < 0]
        spike_hrs= [hr for hr, v in rh.items() if v > 100]
        node_analysis[node] = {
            "dart_avg":      dart[node],
            "best_hour":     int(best_hr),
            "best_spread":   dh[best_hr],
            "worst_hour":    int(worst_hr),
            "worst_spread":  dh[worst_hr],
            "neg_hours":     len(neg_hrs),
            "spike_hours":   len(spike_hrs),
            "region":        next((r for r, nodes in REGIONS.items()
                                   if node in nodes), "Other"),
        }

    # Rank by daily DART avg
    ranked = sorted(node_analysis.items(),
                    key=lambda x: x[1]["dart_avg"], reverse=True)
    top10    = [{"node": n, **v} for n, v in ranked[:10]]
    bottom10 = [{"node": n, **v} for n, v in ranked[-10:]][::-1]

    # Regional summary
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

    return {
        "top10":    top10,
        "bottom10": bottom10,
        "regional": regional,
    }

# ── REPORT BUILDER ────────────────────────────────────────────────────────────

def build_report(data):
    rt    = data.get("rt", {})
    da    = data.get("da", {})
    dart  = data.get("dart", {})
    load  = data.get("gross_load", {})
    wind  = data.get("wind", {})
    solar = data.get("solar", {})
    tb    = compute_top_bottom(data)

    all_rt_avg  = [v["avg"] for v in rt.values()] if rt else [0]
    fleet_avg   = round(sum(all_rt_avg) / len(all_rt_avg), 2) if all_rt_avg else 0
    fleet_max   = round(max(v["max"] for v in rt.values()), 2) if rt else 0
    spike_nodes = [n for n, v in rt.items() if v["max"] > 100]
    neg_nodes   = [n for n, v in rt.items() if v["min"] < 0]
    best_dart   = max(dart, key=dart.get) if dart else None
    worst_dart  = min(dart, key=dart.get) if dart else None

    shared_days = sorted(set(load) & set(wind) & set(solar))[-7:]
    fund_rows = ""
    for d in shared_days:
        g = load.get(d, 0)
        w = wind.get(d, 0)
        s = solar.get(d, 0)
        net = round(g - w - s, 1)
        flag = "charging-window" if net < 30 else ""
        fund_rows += f"""
        <tr class="{flag}">
          <td>{d}</td><td>{g:.1f}</td><td>{w:.1f}</td>
          <td>{s:.1f}</td><td>{net:.1f}</td>
        </tr>"""

    price_rows = ""
    for node in sorted(rt.keys(), key=lambda n: rt[n]["avg"], reverse=True):
        r  = rt.get(node, {})
        dv = da.get(node, {})
        sp = dart.get(node)
        region = next((reg for reg, nodes in REGIONS.items()
                       if node in nodes), "Other")
        spike_cls = ' class="spike"' if r.get("max", 0) > 100 else ""
        neg_cls   = ' class="neg"'   if r.get("min", 0) < 0   else ""
        dart_cls  = ' class="rt-prem"' if (sp or 0) > 5 else (
                    ' class="da-prem"' if (sp or 0) < -5 else "")
        sp_str    = f"+${sp:.2f}" if sp and sp > 0 else (
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

    # Top/Bottom table rows
    top_rows = ""
    for item in tb["top10"]:
        top_rows += f"""
        <tr>
          <td class="node-name">{item['node']}</td>
          <td class="region-tag region-{item['region'].lower().replace(' ','-')}">{item['region']}</td>
          <td class="rt-prem">+${item['dart_avg']:.2f}</td>
          <td>HE {item['best_hour']:02d}:00</td>
          <td class="rt-prem">+${item['best_spread']:.2f}</td>
          <td>{item['spike_hours']} hrs</td>
          <td>{item['neg_hours']} hrs</td>
        </tr>"""

    bot_rows = ""
    for item in tb["bottom10"]:
        bot_rows += f"""
        <tr>
          <td class="node-name">{item['node']}</td>
          <td class="region-tag region-{item['region'].lower().replace(' ','-')}">{item['region']}</td>
          <td class="da-prem">${item['dart_avg']:.2f}</td>
          <td>HE {item['worst_hour']:02d}:00</td>
          <td class="da-prem">${item['worst_spread']:.2f}</td>
          <td>{item['spike_hours']} hrs</td>
          <td>{item['neg_hours']} hrs</td>
        </tr>"""

    best_str  = f"{best_dart} +${dart[best_dart]:.2f}/MWh" if best_dart else "N/A"
    worst_str = f"{worst_dart} ${dart[worst_dart]:.2f}/MWh" if worst_dart else "N/A"
    dow = date.today().strftime("%A")

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
  .kpi-strip{{display:grid;grid-template-columns:repeat(4,1fr);
              border-bottom:1px solid #eee}}
  .kpi{{padding:16px 18px;border-right:1px solid #eee}}
  .kpi:last-child{{border-right:none}}
  .kpi-label{{font-size:10px;font-weight:600;color:#888;
              text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}}
  .kpi-value{{font-size:22px;font-weight:600;color:#1a1a2e;font-family:monospace}}
  .kpi-sub{{font-size:11px;color:#888;margin-top:3px}}
  .section{{padding:20px 24px 0}}
  .section-title{{font-size:11px;font-weight:700;color:#888;
                  text-transform:uppercase;letter-spacing:.07em;
                  margin-bottom:12px;padding-bottom:8px;
                  border-bottom:1px solid #f0f0f0}}
  table{{width:100%;border-collapse:collapse;font-size:12px}}
  th{{background:#f8f9fa;padding:7px 10px;text-align:right;font-weight:600;
      color:#555;font-size:11px;border-bottom:2px solid #e8e8e8}}
  th:first-child,th:nth-child(2){{text-align:left}}
  td{{padding:7px 10px;text-align:right;border-bottom:1px solid #f4f4f4;
      color:#2a2a3e}}
  td:first-child{{text-align:left}}
  td:nth-child(2){{text-align:left}}
  tr:hover td{{background:#fafbfc}}
  .node-name{{font-family:monospace;font-size:11px;color:#444}}
  .spike{{color:#b33000;font-weight:600}}
  .neg{{color:#0066cc}}
  .rt-prem{{color:#1a7a3f;font-weight:600}}
  .da-prem{{color:#7a3a1a}}
  .charging-window td{{background:#f0faf4}}
  .region-tag{{font-size:10px;padding:2px 6px;border-radius:3px;
               display:inline-block;white-space:nowrap}}
  .region-west-texas{{background:#e8f0fe;color:#1a3a8a}}
  .region-north-texas{{background:#e8f5e9;color:#1a5c2a}}
  .region-coastal{{background:#e3f2fd;color:#0d47a1}}
  .region-premium{{background:#fce4ec;color:#880e4f}}
  .callout-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;
                 margin-top:14px}}
  .callout{{background:#f8f9fa;border-radius:6px;padding:12px 14px;
            border-left:3px solid #ddd}}
  .callout.green{{border-left-color:#1a7a3f}}
  .callout.amber{{border-left-color:#c87800}}
  .callout.red{{border-left-color:#b33000}}
  .callout.blue{{border-left-color:#0055aa}}
  .callout-label{{font-size:10px;font-weight:700;color:#888;
                  text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px}}
  .callout-value{{font-size:12px;color:#1a1a2e;font-family:monospace}}
  .footer{{background:#f8f9fa;border-radius:0 0 10px 10px;
           padding:12px 24px;display:flex;justify-content:space-between;
           border-top:1px solid #eee;font-size:11px;color:#aaa}}
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
      <div class="section-title">Top 10 DART performers — {YESTERDAY}</div>
      <table><thead><tr>
        <th>Node</th><th>Region</th><th>DART avg</th>
        <th>Best hour</th><th>Best spread</th>
        <th>Spike hrs</th><th>Neg hrs</th>
      </tr></thead><tbody>{top_rows}</tbody></table>
    </div>

    <div class="section" style="margin-top:20px">
      <div class="section-title">Bottom 10 DART performers — {YESTERDAY}</div>
      <table><thead><tr>
        <th>Node</th><th>Region</th><th>DART avg</th>
        <th>Worst hour</th><th>Worst spread</th>
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
        "from": {"email": from_addr, "name": "HEN Morning Report"},
        "content": [{"type": "text/html", "value": html}],
    }
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"},
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
        "data_date":         YESTERDAY,
        "generated_at":      TODAY_STR,
        "regions":           REGIONS,
        "node_coords":       NODE_COORDS,
        "rt":                data.get("rt", {}),
        "rt_hourly":         data.get("rt_hourly", {}),
        "da":                data.get("da", {}),
        "da_hourly":         data.get("da_hourly", {}),
        "dart":              data.get("dart", {}),
        "dart_hourly":       data.get("dart_hourly", {}),
        "gross_load":        data.get("gross_load", {}),
        "gross_load_hourly": data.get("gross_load_hourly", {}),
        "wind":              data.get("wind", {}),
        "wind_hourly":       data.get("wind_hourly", {}),
        "solar":             data.get("solar", {}),
        "solar_hourly":      data.get("solar_hourly", {}),
        "top_bottom":        tb,
    }
    with open("latest.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print("   Dashboard data written to latest.json")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\nHEN Morning Report v2 — {YESTERDAY}")
    print(f"Nodes: {len(NODES)} across {len(REGIONS)} regions")

    username   = os.environ.get("ERCOT_USERNAME", "")
    password   = os.environ.get("ERCOT_PASSWORD", "")
    sub_key    = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")
    sg_api_key = os.environ.get("SENDGRID_API_KEY", "")
    from_addr  = os.environ.get("FROM_EMAIL", "").strip()
    to_raw     = os.environ.get("TO_EMAILS", "")
    s3_bucket  = os.environ.get("S3_BUCKET", "")
    to_addrs   = [e.strip() for e in to_raw.split(",") if e.strip()]

    missing = []
    if not username: missing.append("ERCOT_USERNAME")
    if not password: missing.append("ERCOT_PASSWORD")
    if not sub_key:  missing.append("ERCOT_SUBSCRIPTION_KEY")
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    email_enabled = bool(sg_api_key and from_addr and to_addrs)

    print("\n1. Authenticating with ERCOT...")
    try:
        token = get_token(username, password, sub_key)
        print("   Token obtained.")
    except Exception as e:
        print(f"   FAILED: {e}")
        sys.exit(1)

    print("\n2. Collecting ERCOT data (hourly)...")
    data = collect_data(token, sub_key)

    print("\n3. Building HTML report...")
    html = build_report(data)
    with open("morning_report.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("   Report written to morning_report.html")
    write_dashboard_json(data)

    if s3_bucket:
        print(f"\n4. Archiving to S3...")
        try:
            archive_to_s3(html, data, s3_bucket)
        except Exception as e:
            print(f"   WARN: {e}")

    if email_enabled:
        print("\n5. Sending email via SendGrid...")
        dow = date.today().strftime("%A")
        try:
            send_email(html, f"HEN Morning Report — {dow} {YESTERDAY}",
                       from_addr, to_addrs, sg_api_key)
        except Exception as e:
            print(f"   WARN: Email failed — {e}")
    else:
        print("\n5. Email skipped — SendGrid not configured")

    print(f"\nDone. Report delivered for {YESTERDAY}.\n")

if __name__ == "__main__":
    main()
