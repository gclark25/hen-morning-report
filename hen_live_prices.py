"""
HEN — Live Intraday Price Puller
==================================
Pulls today's RT prices for all 32 nodes, writes live.json.
Runs every 15 minutes during market hours via GitHub Actions cron.
Designed to be fast — only RT prices, no fundamentals, no DA.

REQUIRED ENVIRONMENT VARIABLES:
  ERCOT_USERNAME          apiexplorer.ercot.com email
  ERCOT_PASSWORD          apiexplorer.ercot.com password
  ERCOT_SUBSCRIPTION_KEY  API Explorer primary key
  ERCOT_NODES             comma-separated settlement point names
"""

import os
import sys
import json
import time
import requests
from datetime import date, datetime, timezone, timedelta
from urllib.parse import quote
from collections import defaultdict

BASE_URL = "https://api.ercot.com/api/public-reports"
TODAY    = date.today().isoformat()

# Central Time offset (UTC-5 standard, UTC-6 daylight — use UTC-5 conservatively)
CT_OFFSET = timedelta(hours=-5)
NOW_CT    = datetime.now(timezone.utc) + CT_OFFSET
NOW_STR   = NOW_CT.strftime("%H:%M CT")

_nodes_env = os.environ.get("ERCOT_NODES", "").strip()
NODES = (
    [n.strip() for n in _nodes_env.split(",") if n.strip()]
    if _nodes_env
    else []
)

REGIONS = {
    "West Texas":  ["TOYAH_RN","SADLBACK_RN","FAULKNER_RN","COYOTSPR_RN","LONESTAR_RN",
                    "RTLSNAKE_BT","CEDRVALE_RN","SBEAN_BESS","GOMZ_RN","GRDNE_ESR_RN",
                    "JDKNS_RN","SANDLAKE_RN"],
    "North Texas": ["OLNEYTN_RN","DIBOL_RN","FRMRSVLW_RN","MNWL_BESS_RN","LFSTH_RN",
                    "PAULN_RN","CISC_RN"],
    "Coastal":     ["MV_VALV4_RN","WLTC_ESR_RN","MAINLAND_RN","FALFUR_RN","PAVLOV_BT_RN",
                    "POTEETS_RN","TYNAN_RN"],
    "Premium":     ["CATARINA_B1","HOLCOMB_RN1","HAMI_BESS_RN","JUNCTION_RN",
                    "RUSSEKST_RN","FTDUNCAN_RN"],
}

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

def extract_rt_price_hour(row):
    """Returns (hour, price) from an RT price row."""
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

def main():
    print(f"\nHEN Live Price Update — {TODAY} {NOW_STR}")
    print(f"Nodes: {len(NODES)}")

    username  = os.environ.get("ERCOT_USERNAME", "")
    password  = os.environ.get("ERCOT_PASSWORD", "")
    sub_key   = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")

    if not all([username, password, sub_key]):
        print("ERROR: Missing ERCOT credentials")
        sys.exit(1)

    if not NODES:
        print("ERROR: ERCOT_NODES not set")
        sys.exit(1)

    # Authenticate
    try:
        token = get_token(username, password, sub_key)
        print("  Auth: token obtained")
    except Exception as e:
        print(f"  Auth FAILED: {e}")
        sys.exit(1)

    # Pull today's RT prices for all nodes
    rt_today     = {}   # node → {avg, max, min, intervals}
    rt_hourly    = {}   # node → {hour: avg_price}
    hours_seen   = set()

    print(f"  Pulling today's RT prices ({len(NODES)} nodes)...")
    for node in NODES:
        time.sleep(3)
        try:
            rows = ercot_get(
                "np6-905-cd/spp_node_zone_hub", token, sub_key,
                {"settlementPoint": node,
                 "deliveryDateFrom": TODAY,
                 "deliveryDateTo":   TODAY}
            )
            hour_buckets = defaultdict(list)
            all_prices   = []
            for row in rows:
                hr, price = extract_rt_price_hour(row)
                if hr is not None and price is not None:
                    hour_buckets[hr].append(price)
                    all_prices.append(price)
                    hours_seen.add(hr)
            if all_prices:
                rt_today[node] = {
                    "avg":       round(sum(all_prices) / len(all_prices), 2),
                    "max":       round(max(all_prices), 2),
                    "min":       round(min(all_prices), 2),
                    "intervals": len(all_prices),
                }
                rt_hourly[node] = {
                    str(hr): round(sum(v) / len(v), 2)
                    for hr, v in sorted(hour_buckets.items())
                }
        except Exception as e:
            print(f"    WARN: {node} — {e}")

    print(f"  Pulled {len(rt_today)} nodes · "
          f"{len(hours_seen)} hours cleared so far today")

    # Fleet summary
    all_rt = [v["avg"] for v in rt_today.values()]
    fleet_avg   = round(sum(all_rt) / len(all_rt), 2) if all_rt else 0
    fleet_max   = round(max(v["max"] for v in rt_today.values()), 2) if rt_today else 0
    spike_nodes = [n for n, v in rt_today.items() if v["max"] > 100]
    neg_nodes   = [n for n, v in rt_today.items() if v["min"] < 0]

    # Regional averages for nodes that have data
    regional = {}
    for region, nodes in REGIONS.items():
        rn = [n for n in nodes if n in rt_today]
        if rn:
            regional[region] = {
                "avg_rt":  round(sum(rt_today[n]["avg"] for n in rn) / len(rn), 2),
                "max_rt":  round(max(rt_today[n]["max"] for n in rn), 2),
                "node_count": len(rn),
            }

    # Hours cleared — for progress indicator on dashboard
    max_hour_cleared = max(hours_seen) if hours_seen else 0

    payload = {
        "date":              TODAY,
        "as_of":             NOW_STR,
        "max_hour_cleared":  max_hour_cleared,
        "hours_cleared":     sorted(list(hours_seen)),
        "fleet": {
            "rt_avg":      fleet_avg,
            "rt_max":      fleet_max,
            "spike_nodes": len(spike_nodes),
            "neg_nodes":   len(neg_nodes),
            "spike_list":  spike_nodes,
            "neg_list":    neg_nodes,
            "node_count":  len(rt_today),
        },
        "rt":        rt_today,
        "rt_hourly": rt_hourly,
        "regional":  regional,
    }

    with open("live.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"  live.json written — fleet avg ${fleet_avg:.2f}, "
          f"HE1-{max_hour_cleared} cleared")

    # Intraday AI analysis — only run on the hour (HH:00) to save API calls
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if anthropic_key and NOW_CT.minute < 16:
        print("  Generating intraday AI analysis...")
        try:
            # Load history for context
            history = []
            try:
                with open("dashboard/history.json", "r") as f:
                    history = json.load(f)
            except Exception:
                pass

            # Build prompt inline (import from morning report module if available)
            headers = {
                "x-api-key":         anthropic_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            }
            yest_rt = history[-1].get("fleet", {}).get("rt_avg", 0) if history else 0
            prompt = f"""You are a commercial energy analyst for Hunt Energy Network (HEN), operator of 32 BESS sites in ERCOT. It is {NOW_STR} CT and HE01-HE{max_hour_cleared:02d} have cleared today.

TODAY'S DATA:
- Fleet avg RT: ${fleet_avg}/MWh across {len(rt_today)} nodes
- Spike nodes (>$100): {spike_nodes if spike_nodes else 'None'}
- Negative price nodes: {neg_nodes if neg_nodes else 'None'}
- Regional RT avgs: {json.dumps({r: v.get("avg_rt",0) for r,v in regional.items()})}
- Yesterday fleet avg RT: ${yest_rt}/MWh

Respond ONLY with valid JSON, no markdown:
{{
  "generated_at": "{NOW_STR}",
  "type": "intraday",
  "intraday_narrative": "2 sentences on how today's RT prices are tracking and what to watch for the rest of the day",
  "vs_yesterday": "one sentence comparing today vs yesterday",
  "charging_signal": "bullish|neutral|bearish",
  "charging_rationale": "one sentence on RT levels and charging opportunity",
  "alerts": [
    {{"type": "spike|negative|opportunity|anomaly", "node": "NODE", "detail": "one sentence"}}
  ]
}}"""

            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json={"model": "claude-sonnet-4-5", "max_tokens": 800,
                      "messages": [{"role": "user", "content": prompt}]},
                timeout=30,
            )
            r.raise_for_status()
            text = r.json()["content"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"): text = text[4:]
            intraday_analysis = json.loads(text.strip())
            intraday_analysis["data_date"] = TODAY

            # Merge with any existing morning analysis
            existing = {}
            try:
                with open("dashboard/ai_analysis.json", "r") as f:
                    existing = json.load(f)
            except Exception:
                pass
            existing["intraday"] = intraday_analysis

            os.makedirs("dashboard", exist_ok=True)
            with open("dashboard/ai_analysis.json", "w") as f:
                json.dump(existing, f, indent=2)
            print("  Intraday AI analysis written")
        except Exception as e:
            print(f"  WARN: Intraday AI analysis failed — {e}")

if __name__ == "__main__":
    main()
