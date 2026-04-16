"""
HEN — ERCOT Public API Connection Test
=======================================
Tests all public API endpoints needed for the morning report.
No MIS credentials required.

Reads credentials from environment variables — works locally
and in GitHub Actions without changing any code.

CREDENTIALS NEEDED (3 things from apiexplorer.ercot.com):
  ERCOT_USERNAME         your apiexplorer.ercot.com email
  ERCOT_PASSWORD         your apiexplorer.ercot.com password
  ERCOT_SUBSCRIPTION_KEY your Primary Key from the API Explorer

TO RUN LOCALLY:
  pip install requests python-dateutil
  export ERCOT_USERNAME="you@huntenergy.com"
  export ERCOT_PASSWORD="yourpassword"
  export ERCOT_SUBSCRIPTION_KEY="your-subscription-key"
  python ercot_public_test.py

ON GITHUB ACTIONS:
  Add those three values as repository secrets.
  The workflow file injects them as environment variables automatically.

WHAT THIS TESTS:
  [1] Token auth    — can we get an ID token from ERCOT's Okta endpoint
  [2] Gross load    — 7-day hourly actual load (Section 2 of morning report)
  [3] Wind          — 7-day hourly wind generation actual + forecast
  [4] Solar         — 7-day hourly solar generation actual + forecast
  [5] Net load      — derived from gross - wind - solar
  [6] RT prices     — real-time settlement point prices for your nodes
  [7] DA prices     — day-ahead settlement point prices for your nodes
  [8] DART spread   — RT minus DA per interval (revenue signal)

OUTPUT:
  Prints PASS / FAIL for each test with sample values.
  Saves ercot_public_test_results.json on completion.
  Exit code 0 = all passed, 1 = any failures (GitHub Actions reads this).
"""

import os
import sys
import json
import requests
from datetime import date, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.ercot.com/api/public-reports"

# Settlement points to test DART prices against.
# Replace with your actual site node names — hub averages work for initial testing.
TEST_NODES = [
    n.strip() for n in
    os.environ.get("ERCOT_TEST_NODES", "HB_BUSAVG,HB_HOUSTON,HB_NORTH,HB_SOUTH,HB_WEST").split(",")
    if n.strip()
]

YESTERDAY = (date.today() - timedelta(days=1)).isoformat()
WEEK_AGO  = (date.today() - timedelta(days=7)).isoformat()

# ── TERMINAL COLORS ───────────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

results = {"passed": 0, "failed": 0, "data": {}}

def ok(label, detail=""):
    results["passed"] += 1
    line = f"{GREEN}  PASS{RESET}  {label}"
    if detail:
        line += f"\n{BLUE}        ↳ {detail}{RESET}"
    print(line)

def fail(label, detail=""):
    results["failed"] += 1
    line = f"{RED}  FAIL{RESET}  {label}"
    if detail:
        line += f"\n{YELLOW}        ↳ {detail}{RESET}"
    print(line)

def info(msg):
    print(f"{BLUE}        {msg}{RESET}")

def section(title):
    print(f"\n{BOLD}{'─' * 62}")
    print(f"  {title}")
    print(f"{'─' * 62}{RESET}")

# ── AUTH ──────────────────────────────────────────────────────────────────────

def get_id_token(username, password, subscription_key):
    """
    ERCOT auth exactly as documented at developer.ercot.com.
    Credentials are URL-encoded then passed as query parameters on a POST.
    URL encoding is critical — any special character (digits, symbols) in
    the password will break auth if not encoded first.
    """
    from urllib.parse import quote
    AUTH_URL = (
        "https://ercotb2c.b2clogin.com"
        "/ercotb2c.onmicrosoft.com"
        "/B2C_1_PUBAPI-ROPC-FLOW"
        "/oauth2/v2.0/token"
        "?username={username}"
        "&password={password}"
        "&grant_type=password"
        "&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access"
        "&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70"
        "&response_type=id_token"
    )
    r = requests.post(
        AUTH_URL.format(
            username=quote(username, safe=""),
            password=quote(password, safe=""),
        ),
        headers={"Ocp-Apim-Subscription-Key": subscription_key},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return data.get("id_token") or data.get("access_token")


def ercot_get(path, token, subscription_key, params=None):
    """Single authenticated GET to the ERCOT public API."""
    headers = {
        "Authorization":             f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
        "Accept":                    "application/json",
    }
    base_params = {"size": 1000}
    if params:
        base_params.update(params)
    r = requests.get(
        f"{BASE_URL}/{path}",
        headers=headers,
        params=base_params,
        timeout=20,
    )
    r.raise_for_status()
    return r.json()

# ── HELPERS ───────────────────────────────────────────────────────────────────

def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0

def avg(values):
    v = [x for x in values if x]
    return round(sum(v) / len(v), 2) if v else 0.0

def peak(values):
    v = [x for x in values if x]
    return round(max(v), 2) if v else 0.0


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{BOLD}HEN — ERCOT Public API Connection Test{RESET}")
    print(f"  Testing date range: {WEEK_AGO} → {YESTERDAY}")
    print(f"  Settlement points:  {', '.join(TEST_NODES)}")

    # ── Read credentials from environment ────────────────────────────────────
    username         = os.environ.get("ERCOT_USERNAME", "")
    password         = os.environ.get("ERCOT_PASSWORD", "")
    subscription_key = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")

    missing = []
    if not username:         missing.append("ERCOT_USERNAME")
    if not password:         missing.append("ERCOT_PASSWORD")
    if not subscription_key: missing.append("ERCOT_SUBSCRIPTION_KEY")

    if missing:
        print(f"\n{RED}  ERROR: Missing environment variables: {', '.join(missing)}{RESET}")
        print(f"  Set them before running or add them as GitHub Secrets.\n")
        sys.exit(1)

    # ── TEST 1: Authentication ────────────────────────────────────────────────
    section("TEST 1 — Authentication")

    token = None
    try:
        token = get_id_token(username, password, subscription_key)
        if token:
            ok("ID token obtained", f"token preview: {token[:20]}...  (expires in 1 hour)")
            results["data"]["auth"] = "ok"
        else:
            fail("Token response was empty", "Check username/password at apiexplorer.ercot.com")
            sys.exit(1)
    except requests.HTTPError as e:
        status = e.response.status_code
        fail(f"Auth failed — HTTP {status}", e.response.text[:300])
        if status == 400:
            info("Likely cause: wrong username or password")
        elif status == 401:
            info("Likely cause: subscription key is incorrect or expired")
        sys.exit(1)
    except Exception as e:
        fail("Auth request failed", str(e))
        sys.exit(1)

    # ── TEST 2: Gross Load ────────────────────────────────────────────────────
    section("TEST 2 — Gross Load (7-day hourly actual)")

    gross_by_day = {}
    try:
        data = ercot_get(
            "np4-745-cd/hb_busavg", token, subscription_key,
            {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY}
        )
        rows = data.get("data", [])
        if rows:
            # Group by date for per-day peak
            for r in rows:
                d   = r.get("deliveryDate", "")[:10]
                val = safe_float(r.get("hbBusAvg") or r.get("value") or 0)
                if d not in gross_by_day:
                    gross_by_day[d] = []
                gross_by_day[d].append(val)

            peak_day  = max(gross_by_day, key=lambda d: max(gross_by_day[d]))
            peak_load = round(max(gross_by_day[peak_day]), 1)
            ok(f"Gross load — {len(rows)} hourly records across {len(gross_by_day)} days",
               f"7-day peak: {peak_load} MW on {peak_day}")

            info("Daily peak load (GW):")
            for d in sorted(gross_by_day.keys()):
                day_peak = round(max(gross_by_day[d]) / 1000, 1)
                bar = "█" * int(day_peak / 5)
                info(f"  {d}  {day_peak:5.1f} GW  {bar}")

            results["data"]["gross_load"] = {
                "records": len(rows), "days": len(gross_by_day),
                "peak_mw": peak_load, "peak_day": peak_day
            }
        else:
            fail("Gross load — no data returned",
                 "ERCOT typically publishes yesterday's data by 8 AM CT. "
                 "Try again after 8 AM or adjust deliveryDateTo to 2 days ago.")
    except requests.HTTPError as e:
        fail(f"Gross load — HTTP {e.response.status_code}", e.response.text[:200])
    except Exception as e:
        fail("Gross load — request failed", str(e))

    # ── TEST 3: Wind Generation ───────────────────────────────────────────────
    section("TEST 3 — Wind Generation (7-day hourly actual + forecast)")

    wind_by_day = {}
    try:
        data = ercot_get(
            "np4-733-cd/wpp_hrly_avrg_actl_fcast", token, subscription_key,
            {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY}
        )
        rows = data.get("data", [])
        if rows:
            for r in rows:
                d = r.get("deliveryDate", "")[:10]
                # Try multiple possible field names
                val = safe_float(
                    r.get("actualStelLoad") or r.get("actual") or
                    r.get("hsLoad") or r.get("value") or 0
                )
                if d not in wind_by_day:
                    wind_by_day[d] = []
                wind_by_day[d].append(val)

            peak_day  = max(wind_by_day, key=lambda d: max(wind_by_day[d]))
            peak_wind = round(max(wind_by_day[peak_day]) / 1000, 1)
            ok(f"Wind generation — {len(rows)} records across {len(wind_by_day)} days",
               f"7-day peak: {peak_wind} GW on {peak_day}")

            info("Daily peak wind (GW):")
            for d in sorted(wind_by_day.keys()):
                day_peak = round(max(wind_by_day[d]) / 1000, 1)
                bar = "█" * int(day_peak / 2)
                info(f"  {d}  {day_peak:5.1f} GW  {bar}")

            results["data"]["wind"] = {
                "records": len(rows), "peak_gw": peak_wind, "peak_day": peak_day
            }
        else:
            fail("Wind generation — no data returned")
    except requests.HTTPError as e:
        fail(f"Wind generation — HTTP {e.response.status_code}", e.response.text[:200])
    except Exception as e:
        fail("Wind generation — request failed", str(e))

    # ── TEST 4: Solar Generation ──────────────────────────────────────────────
    section("TEST 4 — Solar Generation (7-day hourly actual + forecast)")

    solar_by_day = {}
    try:
        data = ercot_get(
            "np4-737-cd/spp_hrly_avrg_actl_fcast", token, subscription_key,
            {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY}
        )
        rows = data.get("data", [])
        if rows:
            for r in rows:
                d = r.get("deliveryDate", "")[:10]
                val = safe_float(
                    r.get("actualStelLoad") or r.get("actual") or
                    r.get("hsLoad") or r.get("value") or 0
                )
                if d not in solar_by_day:
                    solar_by_day[d] = []
                solar_by_day[d].append(val)

            peak_day   = max(solar_by_day, key=lambda d: max(solar_by_day[d]))
            peak_solar = round(max(solar_by_day[peak_day]) / 1000, 1)
            ok(f"Solar generation — {len(rows)} records across {len(solar_by_day)} days",
               f"7-day peak: {peak_solar} GW on {peak_day}")

            info("Daily peak solar (GW):")
            for d in sorted(solar_by_day.keys()):
                day_peak = round(max(solar_by_day[d]) / 1000, 1)
                bar = "█" * int(day_peak / 1.5)
                info(f"  {d}  {day_peak:5.1f} GW  {bar}")

            results["data"]["solar"] = {
                "records": len(rows), "peak_gw": peak_solar, "peak_day": peak_day
            }
        else:
            fail("Solar generation — no data returned")
    except requests.HTTPError as e:
        fail(f"Solar generation — HTTP {e.response.status_code}", e.response.text[:200])
    except Exception as e:
        fail("Solar generation — request failed", str(e))

    # ── TEST 5: Net Load (derived) ────────────────────────────────────────────
    section("TEST 5 — Net Load (gross minus wind minus solar, derived)")

    if gross_by_day and wind_by_day and solar_by_day:
        shared_days = sorted(
            set(gross_by_day) & set(wind_by_day) & set(solar_by_day)
        )
        if shared_days:
            net_load_summary = {}
            for d in shared_days:
                g = max(gross_by_day[d])
                w = max(wind_by_day[d])
                s = max(solar_by_day[d])
                net_min = round((g - w - s) / 1000, 1)
                net_load_summary[d] = net_min

            min_net_day = min(net_load_summary, key=net_load_summary.get)
            ok(f"Net load derived for {len(shared_days)} days",
               f"Lowest net load: {net_load_summary[min_net_day]} GW on {min_net_day}  "
               f"(solar curtailment / storage charging signal)")

            info("Daily minimum net load (GW):")
            for d in shared_days:
                val = net_load_summary[d]
                flag = " ← charging window" if val < 30 else ""
                info(f"  {d}  {val:5.1f} GW{flag}")

            results["data"]["net_load"] = net_load_summary
        else:
            fail("Net load — no overlapping days across all three sources")
    else:
        fail("Net load — skipped (upstream data missing)",
             "Fix gross load, wind, or solar failures first")

    # ── TEST 6: Real-Time Settlement Point Prices ─────────────────────────────
    section("TEST 6 — Real-Time Settlement Point Prices (DART — RT leg)")

    rt_prices = {}
    print(f"  Testing {len(TEST_NODES)} nodes: {', '.join(TEST_NODES)}\n")

    for node in TEST_NODES:
        try:
            data = ercot_get(
                "np6-905-cd/spp_node_zone_hub", token, subscription_key,
                {
                    "settlementPoint":  node,
                    "deliveryDateFrom": YESTERDAY,
                    "deliveryDateTo":   YESTERDAY,
                }
            )
            rows = data.get("data", [])
            if rows:
                prices = [safe_float(r.get("settlementPointPrice") or
                                     r.get("price") or 0) for r in rows]
                prices = [p for p in prices if p != 0]
                avg_p  = avg(prices)
                max_p  = peak(prices)
                min_p  = round(min(prices), 2) if prices else 0
                rt_prices[node] = {
                    "avg": avg_p, "max": max_p, "min": min_p,
                    "intervals": len(rows)
                }
                # Price spike flag
                spike = f"  ⚡ SPIKE" if max_p > 100 else ""
                ok(f"RT prices: {node:<18} avg=${avg_p:>7.2f}  "
                   f"min=${min_p:>7.2f}  max=${max_p:>7.2f}/MWh{spike}",
                   f"{len(rows)} intervals on {YESTERDAY}")
            else:
                fail(f"RT prices: {node} — no data",
                     "Check settlement point name — must match ERCOT exactly (case-sensitive)")
        except requests.HTTPError as e:
            fail(f"RT prices: {node} — HTTP {e.response.status_code}",
                 e.response.text[:150])
        except Exception as e:
            fail(f"RT prices: {node} — failed", str(e))

    results["data"]["rt_prices"] = rt_prices

    # ── TEST 7: Day-Ahead Settlement Point Prices ─────────────────────────────
    section("TEST 7 — Day-Ahead Settlement Point Prices (DART — DA leg)")

    da_prices = {}
    print(f"  Testing {len(TEST_NODES)} nodes\n")

    for node in TEST_NODES:
        try:
            data = ercot_get(
                "np4-190-cd/dam_stlmnt_pnt_prices", token, subscription_key,
                {
                    "settlementPoint":  node,
                    "deliveryDateFrom": YESTERDAY,
                    "deliveryDateTo":   YESTERDAY,
                }
            )
            rows = data.get("data", [])
            if rows:
                prices = [safe_float(r.get("settlementPointPrice") or
                                     r.get("price") or 0) for r in rows]
                prices = [p for p in prices if p != 0]
                avg_p  = avg(prices)
                max_p  = peak(prices)
                da_prices[node] = {"avg": avg_p, "max": max_p, "intervals": len(rows)}
                ok(f"DA prices: {node:<18} avg=${avg_p:>7.2f}  "
                   f"max=${max_p:>7.2f}/MWh",
                   f"{len(rows)} hourly intervals on {YESTERDAY}")
            else:
                fail(f"DA prices: {node} — no data")
        except requests.HTTPError as e:
            fail(f"DA prices: {node} — HTTP {e.response.status_code}",
                 e.response.text[:150])
        except Exception as e:
            fail(f"DA prices: {node} — failed", str(e))

    results["data"]["da_prices"] = da_prices

    # ── TEST 8: DART Spread ───────────────────────────────────────────────────
    section("TEST 8 — DART Spread (RT minus DA signal)")

    common_nodes = set(rt_prices) & set(da_prices)
    if common_nodes:
        dart_spreads = {}
        for node in sorted(common_nodes):
            rt  = rt_prices[node]["avg"]
            da  = da_prices[node]["avg"]
            spread = round(rt - da, 2)
            dart_spreads[node] = spread
            direction = "RT PREMIUM ↑" if spread > 0 else "DA PREMIUM ↓"
            flag = f"  ← significant" if abs(spread) > 10 else ""
            ok(f"DART spread: {node:<18} RT ${rt:>7.2f} − DA ${da:>7.2f} "
               f"= {spread:>+7.2f}/MWh  {direction}{flag}")

        results["data"]["dart_spreads"] = dart_spreads
        best = max(dart_spreads, key=lambda n: dart_spreads[n])
        info(f"Highest RT premium yesterday: {best} at "
             f"+${dart_spreads[best]:.2f}/MWh above DA")
    else:
        fail("DART spread — no nodes with both RT and DA data",
             "Fix RT or DA price failures above first")

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    section("SUMMARY")

    total   = results["passed"] + results["failed"]
    pct     = int(results["passed"] / total * 100) if total else 0
    bar_len = 40
    filled  = int(bar_len * pct / 100)
    bar     = f"{GREEN}{'█' * filled}{RESET}{'░' * (bar_len - filled)}"

    print(f"\n  {bar}  {pct}%")
    print(f"\n  {GREEN}Passed:{RESET} {results['passed']}/{total}")
    print(f"  {RED}Failed:{RESET} {results['failed']}/{total}\n")

    if results["failed"] == 0:
        print(f"{GREEN}{BOLD}  All public API tests passed.{RESET}")
        print(f"  Your ERCOT connection is fully validated.")
        print(f"  Ready to share results with your developer.\n")
    elif results["passed"] > 0:
        print(f"{YELLOW}{BOLD}  Partial success — review FAIL items above.{RESET}")
        print(f"  Most common fixes:")
        print(f"    • Wrong credentials → re-check apiexplorer.ercot.com login")
        print(f"    • Data not published yet → ERCOT posts ~8 AM CT, run again after")
        print(f"    • Wrong node name → must match ERCOT spelling exactly\n")
    else:
        print(f"{RED}{BOLD}  All tests failed.{RESET}")
        print(f"  Check that ERCOT_USERNAME / PASSWORD / SUBSCRIPTION_KEY are correct.\n")

    # ── Save results JSON ─────────────────────────────────────────────────────
    output = {
        "test_date":    date.today().isoformat(),
        "data_date":    YESTERDAY,
        "nodes_tested": TEST_NODES,
        "summary":      {"passed": results["passed"], "failed": results["failed"]},
        "data":         results["data"],
    }
    with open("ercot_public_test_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"  Results saved → ercot_public_test_results.json")
    print(f"  Share this file with developer candidates.\n")

    sys.exit(0 if results["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
