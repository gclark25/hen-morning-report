"""
HEN — Missing API Integrations
================================
Drop-in module for hen_morning_report.py.

Adds four data sources that the prototype does not yet cover:

  1. ERCOT         — Yesterday's top-5 binding constraints + per-asset shift factors
  2. AG2           — 15-day weather outlook with prior-forecast delta comparison
  3. Modo Energy   — 3rd-party custom index performance (battery capture, wind/solar
                     capture rates, DART index, AS opportunity)
  4. PowerTools    — Asset availability (MW) and planned/forced outage schedule
                     for all 32 HEN BESS sites, via the internal PowerTools platform

NOTE — Meteologica (7-day load/wind/solar forecasts) is intentionally excluded.
Will be added once Meteologica access is provisioned — see commented stub below.

NOTE — Drew forward curve is intentionally excluded.
Will be added once the Drew internal service is accessible — see commented stub below.

USAGE IN hen_morning_report.py
-------------------------------
Step 1 — import at the top of the file:
    from hen_integrations import collect_all_integrations

Step 2 — at the end of collect_data(), before the return statement, add:
    print("\\n── Collecting additional integrations ──")
    extras = collect_all_integrations(token=token, sub_key=sub_key,
                                      asset_nodes=NODES)
    data.update(extras)

Step 3 — inside write_dashboard_json(), merge into payload:
    payload["constraints"]    = data.get("constraints", [])
    payload["weather"]        = data.get("weather", {})
    payload["modo"]           = data.get("modo", {})
    payload["asset_status"]   = data.get("asset_status", {})

REQUIRED ENVIRONMENT VARIABLES
--------------------------------
Add these to GitHub Secrets and to the env: block in hen-morning-report.yml:

  AG2_ACCOUNT               AG2 Trader username (your wsitrader.com login email username)
  AG2_PROFILE               AG2 Trader profile (your wsitrader.com login email address)
  AG2_PASSWORD              AG2 Trader password (your wsitrader.com password)
  MODO_API_KEY              Modo Energy X-Token (from modoenergy.com/profile/developers)
  MODO_INDEX_IDS            Optional — comma-separated "name:id" pairs to skip discovery.
                            Example: "2026 - 1Hr Without HEN:1234,HEN 2026:1235,..."
                            Get IDs by running once without this set and checking logs.
  POWERTOOLS_URL            Full URL to your PowerTools platform
  POWERTOOLS_API_KEY        PowerTools API key or Bearer token (if required)
  POWERTOOLS_USERNAME       PowerTools login username (if auth is form-based)
  POWERTOOLS_PASSWORD       PowerTools login password (if auth is form-based)

  # Coming soon — add when Meteologica access is provisioned:
  # METEOLOGICA_API_KEY     API key from Meteologica (X-API-Key header)
  # METEOLOGICA_SITE_ID     Your ERCOT site/portfolio identifier

  # Coming soon — add when Drew service is accessible:
  # DREW_API_URL            Base URL for Drew's internal forward curve service
  # DREW_API_KEY            Drew service API key
  # DREW_HUB                Hub name to price against (default: HB_NORTH)

All four collectors are safe — they catch every exception internally and return
an empty/stub dict so a single API outage never kills the morning report.
"""

import os
import json
import time
import requests
from datetime import date, timedelta, datetime
from collections import defaultdict

# ── Shared date constants (mirrors hen_morning_report.py) ─────────────────────
TODAY_STR  = date.today().isoformat()
YESTERDAY  = (date.today() - timedelta(days=1)).isoformat()
IN_15_DAYS = (date.today() + timedelta(days=15)).isoformat()
PRIOR_15   = (date.today() - timedelta(days=15)).isoformat()
DAY_BEFORE = (date.today() - timedelta(days=2)).isoformat()

# Modo Energy ERCOT index data has a ~60-day publication lag (settled market data).
# Window end = today - 62 days (latest published data).
# 1-hour indices (HEN 2026, 1Hr Without HEN) start Jan 1, 2026.
# 2-hour indices (Fort Duncan, 2Hr Without Fort Duncan) start Jan 21, 2026
# when those assets began commercial operation.
MODO_DATE                  = (date.today() - timedelta(days=62)).isoformat()
MODO_WINDOW_START_1HR      = "2026-01-01"
MODO_WINDOW_START_2HR      = "2026-01-21" 


def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# 1.  ERCOT — Yesterday's top-5 binding constraints + per-asset shift factors
# ══════════════════════════════════════════════════════════════════════════════

def collect_ercot_constraints(token, sub_key, asset_nodes=None):
    """
    Pull yesterday's binding transmission constraints from ERCOT and compute
    the shift factor each constraint has on every HEN asset node.

    ERCOT endpoints:
      np6-86-cd/co_hsl_lapf   — SCED binding constraint records
                                 (shadow price per 15-min interval)
      np6-787-cd/ptdf_sf      — Power Transfer Distribution Factors
                                 (shift factor per settlement point per constraint)

    SCED row schema (positional):
      [deliveryDate, deliveryHour, constraintName, contingencyName,
       shadowPrice, maxShadowPrice, overloadedElement, ...]

    PTDF row schema (positional):
      [deliveryDate, constraintName, settlementPoint, shiftFactor, ...]

    Returns:
    {
      "constraints": [
        {
          "name":           "North_South_345",
          "element":        "Waco-Austin 345kV",
          "contingency":    "N-1",
          "avg_shadow":     48.20,      # $/MWh average while binding
          "peak_shadow":    62.10,      # $/MWh maximum interval
          "hours_binding":  6.5,        # hours the constraint was binding
          "flow_direction": "S->N",
          "shift_factors": {
            "TOYAH_RN":    0.38,
            "MAINLAND_RN": -0.12,
            ...                         # one entry per asset in ERCOT_NODES
          }
        },
        ...   (top 5 by avg_shadow * hours_binding)
      ],
      "data_date": "YYYY-MM-DD",
      "source":    "ERCOT"
    }
    """
    if asset_nodes is None:
        _env = os.environ.get("ERCOT_NODES", "")
        asset_nodes = [n.strip() for n in _env.split(",") if n.strip()]

    BASE = "https://api.ercot.com/api/public-reports"
    headers = {
        "Authorization":             f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": sub_key,
        "Accept":                    "application/json",
    }

    def _ercot_get(path, params=None):
        p = {"size": 5000}
        if params:
            p.update(params)
        try:
            r = requests.get(f"{BASE}/{path}", headers=headers,
                             params=p, timeout=25)
            r.raise_for_status()
            body = r.json()
            if isinstance(body, list):
                return body
            if "data" in body:
                return body["data"]
            for v in body.values():
                if isinstance(v, list):
                    return v
        except Exception as e:
            print(f"  WARN [ERCOT constraints] {path} — {e}")
        return []

    # ── Step 1: Pull SCED binding constraints ─────────────────────────────
    print("  Pulling ERCOT SCED binding constraints...")
    sced_rows = _ercot_get(
        "np6-86-cd/co_hsl_lapf",
        {"deliveryDateFrom": YESTERDAY, "deliveryDateTo": YESTERDAY}
    )

    constraint_agg = defaultdict(lambda: {
        "shadow_prices": [],
        "hours":         set(),
        "element":       "",
        "contingency":   "",
    })

    for row in sced_rows:
        if isinstance(row, list) and len(row) >= 5:
            d_date   = str(row[0])[:10]
            d_hour   = row[1]
            c_name   = str(row[2]).strip()
            cont     = str(row[3]).strip() if len(row) > 3 else ""
            shadow   = safe_float(row[4])
            element  = str(row[6]).strip() if len(row) > 6 else ""
        elif isinstance(row, dict):
            d_date   = str(row.get("deliveryDate",    ""))[:10]
            d_hour   = row.get("deliveryHour", 0)
            c_name   = str(row.get("constraintName",  "")).strip()
            cont     = str(row.get("contingencyName", "")).strip()
            shadow   = safe_float(row.get("shadowPrice", 0))
            element  = str(row.get("overloadedElement", "")).strip()
        else:
            continue

        if d_date != YESTERDAY or not c_name or shadow <= 0:
            continue

        constraint_agg[c_name]["shadow_prices"].append(shadow)
        constraint_agg[c_name]["hours"].add(d_hour)
        if element and not constraint_agg[c_name]["element"]:
            constraint_agg[c_name]["element"] = element
        if cont and not constraint_agg[c_name]["contingency"]:
            constraint_agg[c_name]["contingency"] = cont

    # Rank by total shadow price impact (avg_shadow × distinct_hours)
    ranked = sorted(
        constraint_agg.items(),
        key=lambda x: (
            sum(x[1]["shadow_prices"]) / max(len(x[1]["shadow_prices"]), 1)
        ) * len(x[1]["hours"]),
        reverse=True,
    )
    top5_names = [name for name, _ in ranked[:5]]
    print(f"    {len(constraint_agg)} binding constraints found. "
          f"Top 5: {', '.join(top5_names)}")

    # ── Step 2: Pull PTDF shift factors per constraint × per asset ─────────
    print(f"  Pulling PTDF shift factors "
          f"({len(top5_names)} constraints × {len(asset_nodes)} assets)...")
    shift_factors = defaultdict(dict)

    for c_name in top5_names:
        time.sleep(1)   # be polite — avoid 429 on ERCOT API
        ptdf_rows = _ercot_get(
            "np6-787-cd/ptdf_sf",
            {
                "constraintName":   c_name,
                "deliveryDateFrom": YESTERDAY,
                "deliveryDateTo":   YESTERDAY,
            },
        )
        for row in ptdf_rows:
            if isinstance(row, list) and len(row) >= 4:
                sp = str(row[2]).strip()
                sf = safe_float(row[3])
            elif isinstance(row, dict):
                sp = str(row.get("settlementPoint", "")).strip()
                sf = safe_float(row.get("shiftFactor", 0))
            else:
                continue
            if sp in asset_nodes:
                shift_factors[c_name][sp] = round(sf, 4)

        print(f"    {c_name}: {len(shift_factors[c_name])} asset shift factors")

    # ── Step 3: Build output list ──────────────────────────────────────────
    constraints = []
    for c_name in top5_names:
        agg    = constraint_agg[c_name]
        prices = agg["shadow_prices"]
        hrs    = agg["hours"]

        avg_shadow  = round(sum(prices) / len(prices), 2) if prices else 0.0
        peak_shadow = round(max(prices), 2)               if prices else 0.0
        # SCED runs every 15 min → multiply distinct 15-min slots by 0.25 for hours
        hours_bind  = round(len(hrs) * 0.25, 1)

        # Infer flow direction from name/element heuristics
        el = agg["element"].upper()
        nm = c_name.upper()
        if "SOUTH" in el or nm.startswith("S_") or "SOUTH" in nm:
            direction = "S->N"
        elif "NORTH" in el or nm.startswith("N_") or "NORTH" in nm:
            direction = "N->S"
        elif "WEST" in el or nm.startswith("W_") or "PAN" in nm or "PANHANDLE" in nm:
            direction = "W->E"
        else:
            direction = "N/A"

        constraints.append({
            "name":           c_name,
            "element":        agg["element"] or c_name,
            "contingency":    agg["contingency"],
            "avg_shadow":     avg_shadow,
            "peak_shadow":    peak_shadow,
            "hours_binding":  hours_bind,
            "flow_direction": direction,
            "shift_factors":  dict(shift_factors.get(c_name, {})),
        })

    return {
        "constraints": constraints,
        "data_date":   YESTERDAY,
        "source":      "ERCOT",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2.  AG2 — 15-day city temperature & precip forecast for major ERCOT metros
# ══════════════════════════════════════════════════════════════════════════════

AG2_BASE = "https://www.wsitrader.com/Services/CSVDownloadService.svc"

# ERCOT city names exactly as they appear in AG2 Trader's ERCOT region table.
# The API returns City column values matching these display names.
# Using a set for fast membership testing; we pull all cities and filter to these.
AG2_ERCOT_CITIES = {
    "Abilene, TX",
    "Austin, TX",
    "Corpus Christi, TX",
    "Dallas Fort Worth, TX",
    "Galveston, TX",
    "Houston Iah, TX",
    "Lubbock, TX",
    "Midland, TX",
    "San Antonio, TX",
    "Waco, TX",
    "Wichita Falls, TX",
    "Brownsville, TX",
    "Laredo Afb, TX",
    "Victoria, TX",
}


def _ag2_auth_params():
    """Return the three required auth parameters for every AG2 Trader API call."""
    return {
        "Account":  os.environ.get("AG2_ACCOUNT", ""),
        "Profile":  os.environ.get("AG2_PROFILE", ""),
        "Password": os.environ.get("AG2_PASSWORD", ""),
    }


def _ag2_csv_get(endpoint, extra_params, timeout=25):
    """
    GET an AG2 Trader CSV endpoint and return the raw response text.
    Auth is passed as query parameters — no headers needed.
    """
    url = f"{AG2_BASE}/{endpoint}"
    params = {**_ag2_auth_params(), **extra_params}
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  WARN [AG2 Trader] {endpoint} — {e}")
        return ""


def _parse_ag2_csv(csv_text):
    """Parse an AG2 Trader CSV response into a list of dicts."""
    import csv, io
    rows = []
    if not csv_text or not csv_text.strip():
        return rows
    try:
        reader = csv.DictReader(io.StringIO(csv_text.strip()))
        for row in reader:
            rows.append({k.strip(): v.strip() for k, v in row.items() if k})
    except Exception as e:
        print(f"  WARN [AG2 CSV parse] {e}")
    return rows


def collect_ag2_weather():
    """
    Pull 15-day MinMax temperature and precip probability for 8 major ERCOT
    metro stations using AG2 Trader's GetCityTableForecast endpoint.

    Two calls per station batch:
      CurrentTabName=MinMax  → daily high/low °F
      CurrentTabName=POP     → probability of precipitation (%)

    Uses allcities with Region=NA to get all provisioned stations in one call,
    then filters to the ERCOT metros we care about by station ID.

    Returns:
    {
      "weather": {
        "cities": {
          "Dallas/Ft Worth": {
            "station": "KDFW",
            "days": [
              { "date": "2026-04-23", "high": 88, "low": 64, "precip_pct": 10 },
              ...
            ]
          },
          ...
        },
        "generated_at": "ISO",
        "source": "AG2 Trader (wsitrader.com)"
      }
    }
    """
    acct = os.environ.get("AG2_ACCOUNT", "")
    if not acct:
        print("  SKIP [AG2 Trader] AG2_ACCOUNT not set")
        return {"weather": {}}

    print(f"  Pulling AG2 Trader 15-day city forecasts for ERCOT metros...")

    # ── Call 1: MinMax temperature for all provisioned NA cities ─────────
    minmax_csv = _ag2_csv_get(
        "GetCityTableForecast",
        {
            "IsCustom":       "false",
            "CurrentTabName": "MinMax",
            "TempUnits":      "F",
            "Id":             "allcities",
            "Region":         "NA",
        },
    )
    minmax_rows = _parse_ag2_csv(minmax_csv)

    # ── Call 2: POP (precip probability) for all provisioned NA cities ───
    pop_csv = _ag2_csv_get(
        "GetCityTableForecast",
        {
            "IsCustom":       "false",
            "CurrentTabName": "POP",
            "TempUnits":      "F",
            "Id":             "allcities",
            "Region":         "NA",
        },
    )
    pop_rows = _parse_ag2_csv(pop_csv)

    # pop_rows parsed above — matching and lookup built in city loop below

    # Build city forecasts by matching AG2 display names.
    # The CSV City column contains values like "Dallas Fort Worth, TX".
    # We match case-insensitively against AG2_ERCOT_CITIES.
    cities_out = {}
    city_days  = {}   # city_name → {date: {high, low}}
    ag2_lower  = {c.lower(): c for c in AG2_ERCOT_CITIES}

    for row in minmax_rows:
        city = str(
            row.get("City") or row.get("Station") or row.get("Location") or
            row.get("CityName") or row.get("city") or ""
        ).strip()
        city_key = city.lower()
        if city_key not in ag2_lower:
            continue
        canonical = ag2_lower[city_key]
        dt = str(row.get("Date") or row.get("date") or "")[:10]
        # MinMax columns: MaxTemp / Max / High and MinTemp / Min / Low
        hi = int(safe_float(
            row.get("MaxTemp") or row.get("Max") or row.get("High") or
            row.get("maxtemp") or row.get("max") or 0
        ))
        lo = int(safe_float(
            row.get("MinTemp") or row.get("Min") or row.get("Low") or
            row.get("mintemp") or row.get("min") or 0
        ))
        if dt:
            city_days.setdefault(canonical, {})[dt] = {"high": hi, "low": lo}

    # Build POP lookup by city name
    pop_by_city = {}
    for row in pop_rows:
        city = str(
            row.get("City") or row.get("Station") or row.get("Location") or ""
        ).strip()
        city_key = city.lower()
        if city_key not in ag2_lower:
            continue
        canonical = ag2_lower[city_key]
        dt      = str(row.get("Date") or row.get("date") or "")[:10]
        pop_val = int(safe_float(
            row.get("POP") or row.get("Precip") or row.get("PoP") or
            row.get("pop") or 0
        ))
        if dt:
            pop_by_city.setdefault(canonical, {})[dt] = pop_val

    for city_name, days_data in city_days.items():
        pop_for_city = pop_by_city.get(city_name, {})
        days_list    = []
        for dt in sorted(days_data.keys())[:15]:
            d = days_data[dt]
            days_list.append({
                "date":       dt,
                "high":       d["high"],
                "low":        d["low"],
                "precip_pct": pop_for_city.get(dt, 0),
            })
        if days_list:
            cities_out[city_name] = {"days": days_list}

    n_days = len(next(iter(cities_out.values()))["days"]) if cities_out else 0
    print(f"    AG2: {len(cities_out)} cities · {n_days} days each")
    if not cities_out:
        # Log first few raw rows to help debug column name mismatches
        sample = minmax_rows[:3]
        if sample:
            print(f"    DEBUG — sample CSV columns: {list(sample[0].keys())}")
            print(f"    DEBUG — sample City values: {[r.get('City') or r.get('Station') or r.get('Location') or '?' for r in sample]}")

    return {
        "weather": {
            "cities":       cities_out,
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "source":       "AG2 Trader (wsitrader.com)",
        }
    }

# ══════════════════════════════════════════════════════════════════════════════
# 3.  MODO ENERGY — HEN custom index performance
# ══════════════════════════════════════════════════════════════════════════════

MODO_BASE = "https://api.modoenergy.com/pub/v1"

# The four HEN custom indices defined in the Modo platform.
# Keys are stable short names used internally; values are the exact display
# names as they appear in the Modo platform. The integration resolves these
# to integer IDs via the /pub/v1/indices/ discovery endpoint on first run.
# Once you have run the report and confirmed the IDs, set MODO_INDEX_IDS
# in GitHub Secrets to skip discovery on every subsequent run (faster + safer).
#
# Format for MODO_INDEX_IDS secret (comma-separated name:id pairs):
#   2026 - 1Hr Without HEN:1234,HEN 2026:1235,2-hour Without Fort Duncan:1236,Fort Duncan:1237
# HEN custom indices — IDs confirmed via Modo discovery call.
# 1-hour indices (HEN 2026, 1Hr Without HEN): start Jan 1 2026
# 2-hour indices (Fort Duncan, 2Hr Without Fort Duncan): start Jan 21 2026
HEN_CUSTOM_INDICES = {
    "1hr_without_hen":         {"name": "2026 - 1Hr Without HEN",      "id": 4752, "start": MODO_WINDOW_START_1HR},
    "hen_2026":                {"name": "HEN 2026",                    "id": 4872, "start": MODO_WINDOW_START_1HR},
    "2hr_without_fort_duncan": {"name": "2-hour Without Fort Duncan",  "id": 4891, "start": MODO_WINDOW_START_2HR},
    "fort_duncan":             {"name": "Fort Duncan",                 "id": 5006, "start": MODO_WINDOW_START_2HR},
}


def _modo_headers():
    """Return the correct Modo API auth header. Auth = X-Token header (not Bearer)."""
    return {
        "X-Token": os.environ.get("MODO_API_KEY", ""),
        "Accept":  "application/json",
    }


def _modo_get(path, params=None, timeout=25):
    """
    Single authenticated GET against the Modo Energy API.
    Base URL: https://api.modoenergy.com/pub/v1
    Auth:     X-Token header (from MODO_API_KEY secret)
    Returns:  parsed JSON body or {}
    """
    url = f"{MODO_BASE}/{path}"
    try:
        r = requests.get(url, headers=_modo_headers(),
                         params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  WARN [Modo] {path} — {e}")
        return {}


def _modo_paginate(path, params=None):
    """
    Fetch all pages from a cursor-paginated Modo endpoint.
    Modo uses cursor-based pagination: response contains 'next' with a cursor param.
    Returns a flat list of all result objects across all pages.
    """
    all_results = []
    params = dict(params or {})
    params.setdefault("limit", 10000)
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor
        body = _modo_get(path, params=params)
        results = body.get("results") or body.get("data") or []
        all_results.extend(results)

        # Check for next page
        next_url = body.get("next")
        if not next_url:
            break
        # Extract cursor from next URL
        import urllib.parse as urlparse
        qs = urlparse.parse_qs(urlparse.urlparse(next_url).query)
        cursor = qs.get("cursor", [None])[0]
        if not cursor:
            break

    return all_results


def _modo_resolve_index_ids():
    """
    Resolve HEN custom index names to Modo integer IDs.

    Strategy (in priority order):
      1. MODO_INDEX_IDS env var — a comma-separated list of "name:id" pairs.
         Set this once you know the IDs to avoid a discovery call on every run.
         Example: "2026 - 1Hr Without HEN:1234,HEN 2026:1235,..."
      2. Discovery via GET /pub/v1/indices/ — lists all indices your token can
         access; filter by name to find the custom ones.

    Returns a dict: { short_key: integer_id, ... }
    e.g. { "1hr_without_hen": 1234, "hen_2026": 1235, ... }
    """
    # ── Priority 0: IDs hardcoded in HEN_CUSTOM_INDICES dict ─────────────
    hardcoded = {k: meta["id"] for k, meta in HEN_CUSTOM_INDICES.items()
                 if "id" in meta}
    if hardcoded:
        print(f"    Modo IDs from index config: {hardcoded}")
        return hardcoded

    # ── Priority 1: manual override from secret ────────────────────────────
    id_override = os.environ.get("MODO_INDEX_IDS", "").strip()
    if id_override:
        resolved = {}
        for pair in id_override.split(","):
            pair = pair.strip()
            if ":" not in pair:
                continue
            name, id_str = pair.rsplit(":", 1)
            name = name.strip()
            # Match against HEN_CUSTOM_INDICES display names
            for key, meta in HEN_CUSTOM_INDICES.items():
                if meta["name"].lower() == name.lower():
                    try:
                        resolved[key] = int(id_str.strip())
                    except ValueError:
                        pass
        if resolved:
            print(f"    Modo IDs from MODO_INDEX_IDS secret: {resolved}")
            return resolved

    # ── Priority 2: discovery call ─────────────────────────────────────────
    print("    Discovering Modo custom index IDs via /pub/v1/indices/...")
    all_indices = _modo_paginate("indices/", params={"limit": 500})

    resolved = {}
    display_to_key = {meta["name"].lower(): k for k, meta in HEN_CUSTOM_INDICES.items()}

    for idx in all_indices:
        # Modo index object fields: id (int), name (str), market_region, ...
        idx_id   = idx.get("id")
        idx_name = str(idx.get("name") or idx.get("title") or "").strip()
        if not idx_id or not idx_name:
            continue
        key = display_to_key.get(idx_name.lower())
        if key:
            resolved[key] = int(idx_id)
            print(f"      Found: '{idx_name}' → id={idx_id}")

    missing = [meta["name"] for k, meta in HEN_CUSTOM_INDICES.items() if k not in resolved]
    if missing:
        print(f"    WARN [Modo] Could not resolve IDs for: {missing}")
        print("    TIP: Set MODO_INDEX_IDS secret to bypass discovery. "
              "Run this once with MODO_API_KEY set to see all available index names.")

    return resolved


def _modo_index_daily_revenue(index_id, date_str):
    """
    Fetch the total daily revenue for a given index ID on a specific date.
    Uses the Index Revenue Timeseries endpoint:
      GET /pub/v1/indices/{id}/revenue/timeseries/
        interval_start : date_str + T00:00:00
        interval_end   : date_str + T23:59:59
        granularity    : daily
        capacity_normalisation : mw     (returns $/MW)
        time_basis     : year           (annualised $/MW/year for comparability)

    Returns a dict:
      { "revenue_mw_year": float, "revenue_mw_day": float,
        "market_breakdown": { "energy": x, "ancillary": y, ... } }
    or {} on failure.
    """
    body = _modo_get(
        f"indices/{index_id}/revenue/timeseries/",
        params={
            "interval_start":          f"{date_str}T00:00:00",
            "interval_end":            f"{date_str}T23:59:59",
            "granularity":             "daily",
            "capacity_normalisation":  "mw",
            "time_basis":              "year",
            "breakdown":               "market",
            "limit":                   100,
        },
    )

    results = body.get("results") or []
    if not results:
        return {}

    # Sum across all result rows for the day (should be 1 row with granularity=daily)
    total_annualised = 0.0
    market_breakdown = {}
    for row in results:
        if not isinstance(row, dict):
            continue
        rev = safe_float(row.get("revenue") or row.get("value") or 0)
        market = str(row.get("market") or row.get("service") or "total")
        market_breakdown[market] = round(market_breakdown.get(market, 0.0) + rev, 2)
        total_annualised += rev

    # Also fetch without time_basis=year to get raw $/MW/day
    body_day = _modo_get(
        f"indices/{index_id}/revenue/timeseries/",
        params={
            "interval_start":         f"{date_str}T00:00:00",
            "interval_end":           f"{date_str}T23:59:59",
            "granularity":            "daily",
            "capacity_normalisation": "mw",
            "limit":                  100,
        },
    )
    results_day = body_day.get("results") or []
    total_day = sum(
        safe_float(r.get("revenue") or r.get("value") or 0)
        for r in results_day
        if isinstance(r, dict)
    )

    return {
        "revenue_mw_year": round(total_annualised, 2),
        "revenue_mw_day":  round(total_day, 4),
        "market_breakdown": market_breakdown,
    }


def _modo_index_window_revenue(index_id, start_date, end_date):
    """
    Fetch total revenue for a given index over a date range window.
    Uses monthly granularity aggregated to an annualised $/MW/year figure.

    GET /pub/v1/indices/{id}/revenue/timeseries/
      interval_start          : start_date + T00:00:00
      interval_end            : end_date   + T23:59:59
      granularity             : daily  (sum within window client-side)
      capacity_normalisation  : mw     ($/MW)
      time_basis              : year   (annualised $/MW/year)
      breakdown               : market (split by energy/ancillary)

    Returns:
      { "revenue_mw_year": float, "market_breakdown": { market: float } }
    """
    # Response schema (from Modo API docs):
    # { "next": null, "results": { "units": "USD/MW/year", "records": [...] } }
    # With breakdown=market each record has: market, interval_start, interval_end, revenue
    # Without breakdown each record has:     interval_start, interval_end, revenue
    def _fetch(extra_params=None):
        params = {
            "interval_start":         f"{start_date}T00:00:00",
            "interval_end":           f"{end_date}T23:59:59",
            "granularity":            "daily",
            "capacity_normalisation": "mw",
            "time_basis":             "year",
            "limit":                  10000,
        }
        if extra_params:
            params.update(extra_params)
        body = _modo_get(f"indices/{index_id}/revenue/timeseries/", params=params)
        # Navigate the nested response: body → results (dict) → records (list)
        results_obj = body.get("results") or {}
        if isinstance(results_obj, dict):
            records = results_obj.get("records") or []
            units   = results_obj.get("units", "")
        else:
            # Fallback: older flat list format
            records = results_obj if isinstance(results_obj, list) else []
            units   = ""
        return records, units

    # First try with market breakdown
    records, units = _fetch({"breakdown": "market"})
    # If empty, retry without breakdown
    if not records:
        records, units = _fetch()

    if not records:
        return {}

    total = 0.0
    market_breakdown = {}
    # With breakdown=market there is one record per (day × market).
    # Without breakdown there is one record per day.
    # We track unique dates to get n_days correctly.
    seen_dates = set()
    for row in records:
        if not isinstance(row, dict):
            continue
        rev    = safe_float(row.get("revenue") or 0)
        market = str(row.get("market") or "total")
        date   = str(row.get("interval_start") or "")[:10]
        market_breakdown[market] = round(market_breakdown.get(market, 0.0) + rev, 2)
        total += rev
        if date:
            seen_dates.add(date)

    n_days = len(seen_dates) or len(records)
    # When breakdown=market, total is sum across all markets per day.
    # Divide by n_days to get average daily annualised $/MW/year.
    # (Each day's records already represent the annualised figure for that day.)
    # With breakdown, sum all markets for the same day = that day's total.
    # We need the average across days, not sum across days × markets.
    if market_breakdown and len(market_breakdown) > 1:
        # Re-compute total as per-day total (sum of markets for one day)
        # already captured correctly in total above since time_basis=year
        # means each record is already an annualised rate, not a sum.
        # Average across days:
        avg = round(total / max(n_days, 1) / len(market_breakdown), 2) if market_breakdown else 0.0
        # Simpler: just average the per-market totals
        market_avgs = {k: round(v / n_days, 2) for k, v in market_breakdown.items()}
        avg = round(sum(market_avgs.values()), 2)
    else:
        avg = round(total / n_days, 2) if n_days else 0.0
        market_avgs = market_breakdown

    print(f"      → {n_days} days · {len(records)} records · units: {units} · total: {avg:.2f}")

    return {
        "revenue_mw_year":  avg,
        "n_days":           n_days,
        "market_breakdown": market_avgs,
    }


def collect_modo_indices():
    """
    Pull HEN's four custom Modo Energy indices for yesterday and the prior day,
    computing a day-over-day delta for each.

    Real Modo API details (from developers.modoenergy.com, Apr 2026):
      Base URL : https://api.modoenergy.com/pub/v1
      Auth     : X-Token header (NOT Authorization: Bearer)
      Indices  : GET /pub/v1/indices/                          — list all
      Revenue  : GET /pub/v1/indices/{id}/revenue/timeseries/  — daily revenue

    HEN's four custom indices (defined in the Modo platform):
      "2026 - 1Hr Without HEN"       — 1-hour ERCOT market without HEN assets
      "HEN 2026"                     — HEN portfolio 2026 benchmark
      "2-hour Without Fort Duncan"   — 2-hour ERCOT market ex Fort Duncan
      "Fort Duncan"                  — Fort Duncan standalone asset index

    Index IDs are resolved via discovery on first run. Set MODO_INDEX_IDS
    secret to hardcode them and skip discovery (recommended for production).

    Returns:
    {
      "modo": {
        "data_date":  "YYYY-MM-DD",
        "source":     "Modo Energy (api.modoenergy.com)",
        "indices": {
          "1hr_without_hen": {
            "display_name":    "2026 - 1Hr Without HEN",
            "id":              1234,
            "revenue_mw_year": 142500.00,   # $/MW/year annualised
            "revenue_mw_day":  390.41,      # $/MW for the day
            "delta_dod":       +1200.00,    # vs prior day $/MW/year
            "market_breakdown": { "energy": 45000, "ecrs": 62000, ... }
          },
          "hen_2026":              { ... },
          "2hr_without_fort_duncan": { ... },
          "fort_duncan":           { ... }
        }
      }
    }
    """
    api_key = os.environ.get("MODO_API_KEY", "")
    if not api_key:
        print("  SKIP [Modo] MODO_API_KEY not set")
        return {"modo": {}}

    print(f"  Pulling Modo Energy custom indices (end: {MODO_DATE})...")

    # ── Step 1: Resolve index names → IDs ─────────────────────────────────
    index_ids = _modo_resolve_index_ids()
    if not index_ids:
        print("  WARN [Modo] No index IDs resolved — skipping revenue pull")
        return {"modo": {"data_date": MODO_DATE, "source": "Modo Energy",
                         "error": "No index IDs resolved"}}

    # ── Step 2: Pull 2026 YTD for each index using its specific start date ─
    # 1-hour indices: Jan 1 2026 → MODO_DATE
    # 2-hour indices: Jan 21 2026 → MODO_DATE (commercial operation start)
    indices_out = {}
    for key, meta in HEN_CUSTOM_INDICES.items():
        display_name = meta["name"]
        window_start = meta["start"]
        idx_id       = index_ids.get(key)
        if not idx_id:
            print(f"    SKIP {display_name} — ID not resolved")
            continue

        result = _modo_index_window_revenue(idx_id, window_start, MODO_DATE)
        rev    = result.get("revenue_mw_year", 0.0)

        indices_out[key] = {
            "display_name":     display_name,
            "id":               idx_id,
            "window_start":     window_start,
            "window_end":       MODO_DATE,
            "revenue_mw_year":  rev,
            "n_days":           result.get("n_days", 0),
            "market_breakdown": result.get("market_breakdown", {}),
        }

        print(f"    {display_name}: ${rev:,.0f}/MW/yr "
              f"({result.get('n_days', 0)} days, {window_start} → {MODO_DATE})")

    return {
        "modo": {
            "data_date": MODO_DATE,
            "source":    "Modo Energy (api.modoenergy.com)",
            "indices":   indices_out,
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4.  POWERTOOLS — Asset availability & outage schedule
# ══════════════════════════════════════════════════════════════════════════════

def _powertools_probe(url, headers):
    """
    Auto-detect what the PowerTools URL returns so we can parse it correctly.

    Strategy:
      1. Try a JSON GET — if Content-Type is application/json, parse directly.
      2. Try common REST sub-paths (/api/assets, /api/v1/assets, /assets).
      3. If all JSON attempts return 401/403, flag that auth credentials are needed.
      4. If the response is HTML, flag that this is a web dashboard requiring
         a login flow and log a clear message for the operator.

    Returns (mode, base_url) where mode is one of:
      "json_root"    — root URL returns JSON directly
      "json_api"     — a sub-path returns JSON (base_url updated to that path)
      "auth_required"— server responds but rejects our credentials
      "html_dashboard"— URL serves an HTML login page
      "unreachable"  — connection failed entirely
    """
    probe_paths = ["", "/api/assets", "/api/v1/assets", "/assets",
                   "/api/outages", "/api/v1/outages", "/data/assets"]

    for path in probe_paths:
        probe_url = url.rstrip("/") + path
        try:
            r = requests.get(probe_url, headers=headers, timeout=10)
            ct = r.headers.get("Content-Type", "")

            if r.status_code in (401, 403):
                return "auth_required", probe_url

            if "application/json" in ct and r.status_code == 200:
                mode = "json_root" if path == "" else "json_api"
                return mode, probe_url

            if "text/html" in ct and r.status_code == 200:
                return "html_dashboard", probe_url

        except requests.exceptions.ConnectionError:
            return "unreachable", url
        except Exception:
            continue

    return "unreachable", url


def _parse_powertools_assets(body, asset_nodes):
    """
    Parse asset availability and outage data from a PowerTools JSON response.

    Tries multiple common response shapes:
      Shape A: { "assets": [ { "name": "TOYAH_RN", "available_mw": 100,
                                "capacity_mw": 100, "status": "online" }, ... ] }
      Shape B: { "data": [ ... ] }
      Shape C: [ { "assetId": "TOYAH_RN", "availableMW": 100, ... } ]  (root list)
      Shape D: { "resources": [ ... ] }   (some PowerTools versions)

    Returns list of normalised asset dicts.
    """
    rows = (
        body.get("assets")
        or body.get("data")
        or body.get("resources")
        or body.get("units")
        or (body if isinstance(body, list) else [])
    )

    assets_out = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        # Name / ID — try multiple field names
        name = (
            row.get("name") or row.get("assetName") or row.get("asset_name")
            or row.get("assetId") or row.get("asset_id") or row.get("id") or ""
        )

        # Capacity and available MW
        capacity_mw   = safe_float(
            row.get("capacity_mw") or row.get("capacityMW") or row.get("nameplateCapacity")
            or row.get("nameplate_mw") or row.get("ratedCapacity") or 0
        )
        available_mw  = safe_float(
            row.get("available_mw") or row.get("availableMW") or row.get("availableCapacity")
            or row.get("available_capacity") or row.get("economicMax") or capacity_mw
        )

        # Status string
        status = str(
            row.get("status") or row.get("operatingStatus") or row.get("operating_status")
            or row.get("state") or "unknown"
        ).lower()

        # Outage fields
        outage_type = str(
            row.get("outage_type") or row.get("outageType") or row.get("outage_reason")
            or ("planned" if "planned" in status else
                "forced"  if any(w in status for w in ("forced", "unplanned", "trip")) else
                "none")
        ).lower()

        outage_start  = str(row.get("outage_start") or row.get("outageStart")
                            or row.get("startDate") or "")
        outage_end    = str(row.get("outage_end")   or row.get("outageEnd")
                            or row.get("endDate")   or "")
        outage_mw     = safe_float(row.get("outage_mw") or row.get("outageMW")
                                   or row.get("derated_mw") or 0)
        outage_reason = str(row.get("outage_reason") or row.get("outageReason")
                            or row.get("reason") or "")

        # Availability pct
        avail_pct = round((available_mw / capacity_mw * 100), 1) if capacity_mw else 0.0

        assets_out.append({
            "name":          str(name),
            "capacity_mw":   round(capacity_mw, 1),
            "available_mw":  round(available_mw, 1),
            "availability_pct": avail_pct,
            "status":        status,
            "outage_type":   outage_type,
            "outage_mw":     round(outage_mw, 1),
            "outage_start":  outage_start,
            "outage_end":    outage_end,
            "outage_reason": outage_reason,
            "region":        _node_region(str(name)),
        })

    return assets_out


def _parse_powertools_outages(body):
    """
    Parse a standalone outage schedule endpoint if PowerTools separates
    assets and outages into two endpoints.

    Common shape:
      { "outages": [ { "asset": "TOYAH_RN", "type": "planned",
                       "start": "2026-04-23T06:00", "end": "2026-04-24T18:00",
                       "mw": 50, "reason": "Inverter maintenance" }, ... ] }
    """
    rows = (
        body.get("outages")
        or body.get("outage_schedule")
        or body.get("maintenance")
        or body.get("data")
        or (body if isinstance(body, list) else [])
    )

    outages_out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        outages_out.append({
            "asset":   str(row.get("asset") or row.get("assetName") or row.get("unit") or ""),
            "type":    str(row.get("type")  or row.get("outageType") or "unknown").lower(),
            "start":   str(row.get("start") or row.get("outageStart") or row.get("startDate") or ""),
            "end":     str(row.get("end")   or row.get("outageEnd")   or row.get("endDate")   or ""),
            "mw":      round(safe_float(row.get("mw") or row.get("outageMW") or row.get("capacity_mw") or 0), 1),
            "reason":  str(row.get("reason") or row.get("outageReason") or row.get("description") or ""),
        })

    return outages_out


# Map node names to regions (mirrors REGIONS dict in hen_morning_report.py)
_REGION_MAP = {
    **{n: "West Texas"   for n in ["TOYAH_RN","SADLBACK_RN","FAULKNER_RN","COYOTSPR_RN",
                                    "LONESTAR_RN","RTLSNAKE_BT","CEDRVALE_RN","SBEAN_BESS",
                                    "GOMZ_RN","GRDNE_ESR_RN","JDKNS_RN","SANDLAKE_RN"]},
    **{n: "North Texas"  for n in ["OLNEYTN_RN","DIBOL_RN","FRMRSVLW_RN","MNWL_BESS_RN",
                                    "LFSTH_RN","PAULN_RN","CISC_RN"]},
    **{n: "Coastal"      for n in ["MV_VALV4_RN","WLTC_ESR_RN","MAINLAND_RN","FALFUR_RN",
                                    "PAVLOV_BT_RN","POTEETS_RN","TYNAN_RN"]},
    **{n: "Premium"      for n in ["CATARINA_B1","HOLCOMB_RN1","HAMI_BESS_RN","JUNCTION_RN",
                                    "RUSSEKST_RN","FTDUNCAN_RN"]},
}

def _node_region(name):
    return _REGION_MAP.get(name, "Other")


def collect_powertools_assets():
    """
    Pull asset availability and outage schedule from the PowerTools platform.

    Auto-detects the API shape on first call so no prior knowledge of the
    exact endpoint structure is required — just set POWERTOOLS_URL.

    Authentication is attempted in this order:
      1. Bearer token  (POWERTOOLS_API_KEY set, no username/password)
      2. Basic auth    (POWERTOOLS_USERNAME + POWERTOOLS_PASSWORD set)
      3. Unauthenticated (public endpoint)

    The function tries these endpoint patterns against the base URL:
      /api/assets        — asset list with capacity/availability
      /api/outages       — outage schedule (planned + forced)
      /api/v1/assets     — versioned variant
      /api/v1/outages    — versioned variant
      /assets            — unversioned variant

    Returns:
    {
      "asset_status": {
        "as_of":  "YYYY-MM-DD HH:MM CT",
        "source": "PowerTools",
        "fleet_summary": {
          "total_assets":       32,
          "online":             29,
          "on_outage":           3,
          "total_capacity_mw":  3200,
          "available_mw":       2950,
          "fleet_availability_pct": 92.2,
          "planned_outage_mw":   150,
          "forced_outage_mw":    100,
        },
        "assets": [
          {
            "name":               "TOYAH_RN",
            "region":             "West Texas",
            "capacity_mw":        100.0,
            "available_mw":       100.0,
            "availability_pct":   100.0,
            "status":             "online",
            "outage_type":        "none",
            "outage_mw":          0.0,
            "outage_start":       "",
            "outage_end":         "",
            "outage_reason":      ""
          },
          ...
        ],
        "outage_schedule": [
          {
            "asset":   "RTLSNAKE_BT",
            "type":    "planned",
            "start":   "2026-04-23T06:00",
            "end":     "2026-04-25T18:00",
            "mw":      100.0,
            "reason":  "Battery augmentation"
          },
          ...
        ],
        "detection_mode": "json_api",
        "endpoint_used":  "https://powertools.example.com/api/assets"
      }
    }

    If the URL is unreachable or returns HTML (login wall), the function
    logs a clear message and returns an empty asset_status dict so the
    report continues to run — no crash, no silent failure.
    """
    base_url = os.environ.get("POWERTOOLS_URL", "").rstrip("/")
    api_key  = os.environ.get("POWERTOOLS_API_KEY", "")
    username = os.environ.get("POWERTOOLS_USERNAME", "")
    password = os.environ.get("POWERTOOLS_PASSWORD", "")

    if not base_url:
        print("  SKIP [PowerTools] POWERTOOLS_URL not configured")
        return {"asset_status": {}}

    print(f"  Probing PowerTools at {base_url}...")

    # Build auth headers / params based on what credentials are available
    headers = {"Accept": "application/json"}
    auth    = None
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif username and password:
        auth = (username, password)

    # ── Step 1: Auto-detect endpoint mode ─────────────────────────────────
    mode, detected_url = _powertools_probe(base_url, headers)
    print(f"    Detection result: {mode} → {detected_url}")

    # Check for Power Apps / Microsoft Power Platform URL patterns
    if any(x in base_url for x in ["powerapps.com", "gateway.prod", ".island", "powerautomate"]):
        print("  SKIP [PowerTools] URL appears to be a Microsoft Power Apps dashboard.")
        print("    To integrate: ask IT to create a Power Automate HTTP trigger that")
        print("    returns asset availability JSON, then set POWERTOOLS_URL to that URL.")
        return {"asset_status": {"error": "Power Apps URL — needs HTTP trigger endpoint from IT"}}

    if mode == "html_dashboard":
        print(
            "  WARN [PowerTools] URL returns an HTML login page. "
            "PowerTools appears to be a browser-based dashboard. "
            "Set POWERTOOLS_API_KEY or POWERTOOLS_USERNAME/PASSWORD to "
            "authenticate, or ask your PowerTools admin for an API endpoint URL."
        )
        return {"asset_status": {"detection_mode": mode, "source": "PowerTools",
                                  "error": "HTML dashboard — API credentials needed"}}

    if mode == "auth_required":
        print(
            "  WARN [PowerTools] Server responded with 401/403. "
            "Set POWERTOOLS_API_KEY (Bearer token) or "
            "POWERTOOLS_USERNAME + POWERTOOLS_PASSWORD (Basic auth) in GitHub Secrets."
        )
        return {"asset_status": {"detection_mode": mode, "source": "PowerTools",
                                  "error": "Authentication required"}}

    if mode == "unreachable":
        print(f"  WARN [PowerTools] Could not reach {base_url}. "
              "Check POWERTOOLS_URL and network access from GitHub Actions.")
        return {"asset_status": {"detection_mode": mode, "source": "PowerTools",
                                  "error": "URL unreachable"}}

    # ── Step 2: Pull asset availability ───────────────────────────────────
    asset_endpoint = detected_url
    assets = []

    # Try to find the assets list — probe a few path variants
    asset_paths = [
        detected_url,
        base_url + "/api/assets",
        base_url + "/api/v1/assets",
        base_url + "/assets",
    ]
    for ap in asset_paths:
        try:
            r = requests.get(ap, headers=headers, auth=auth, timeout=15)
            if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                body = r.json()
                parsed = _parse_powertools_assets(body, [])
                if parsed:
                    assets = parsed
                    asset_endpoint = ap
                    print(f"    Assets: {len(assets)} records from {ap}")
                    break
        except Exception as e:
            print(f"    WARN: asset path {ap} — {e}")
            continue

    # ── Step 3: Pull outage schedule ──────────────────────────────────────
    outages = []
    outage_paths = [
        base_url + "/api/outages",
        base_url + "/api/v1/outages",
        base_url + "/outages",
        base_url + "/api/maintenance",
    ]

    # First check if outages are embedded inside asset records already
    embedded_outages = [a for a in assets if a.get("outage_type") not in ("none", "unknown", "")]
    if embedded_outages:
        # Build outage schedule from asset records
        outages = [
            {
                "asset":   a["name"],
                "type":    a["outage_type"],
                "start":   a["outage_start"],
                "end":     a["outage_end"],
                "mw":      a["outage_mw"],
                "reason":  a["outage_reason"],
            }
            for a in embedded_outages
        ]
        print(f"    Outages: {len(outages)} embedded in asset records")
    else:
        # Try dedicated outage endpoint
        for op in outage_paths:
            try:
                r = requests.get(op, headers=headers, auth=auth,
                                 params={"date_from": TODAY_STR, "date_to": IN_15_DAYS},
                                 timeout=15)
                if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                    body = r.json()
                    parsed = _parse_powertools_outages(body)
                    if parsed:
                        outages = parsed
                        print(f"    Outages: {len(outages)} records from {op}")
                        break
            except Exception as e:
                print(f"    WARN: outage path {op} — {e}")
                continue

    # ── Step 4: Build fleet summary ───────────────────────────────────────
    total_cap       = sum(a["capacity_mw"]  for a in assets)
    total_avail     = sum(a["available_mw"] for a in assets)
    on_outage       = [a for a in assets if a["outage_type"] not in ("none", "unknown", "")]
    planned_mw      = sum(a["outage_mw"] for a in assets if a["outage_type"] == "planned")
    forced_mw       = sum(a["outage_mw"] for a in assets
                          if a["outage_type"] in ("forced", "unplanned"))
    fleet_avail_pct = round(total_avail / total_cap * 100, 1) if total_cap else 0.0

    from datetime import datetime, timezone, timedelta
    ct_now = (datetime.now(timezone.utc) + timedelta(hours=-5)).strftime("%Y-%m-%d %H:%M CT")

    return {
        "asset_status": {
            "as_of":   ct_now,
            "source":  "PowerTools",
            "detection_mode":  mode,
            "endpoint_used":   asset_endpoint,
            "fleet_summary": {
                "total_assets":           len(assets),
                "online":                 len(assets) - len(on_outage),
                "on_outage":              len(on_outage),
                "total_capacity_mw":      round(total_cap, 1),
                "available_mw":           round(total_avail, 1),
                "fleet_availability_pct": fleet_avail_pct,
                "planned_outage_mw":      round(planned_mw, 1),
                "forced_outage_mw":       round(forced_mw, 1),
            },
            "assets":          assets,
            "outage_schedule": outages,
        }
    }



# ══════════════════════════════════════════════════════════════════════════════
# 5.  ERCOT PUBLIC FORECASTS — Load, Wind, Solar (24hr hourly + 7-day daily)
# ══════════════════════════════════════════════════════════════════════════════

def collect_ercot_forecasts(token, sub_key):
    """
    Pull ERCOT forward-looking forecasts for portfolio hedging context.

    Endpoints used:
      np3-565-cd/lf_by_model_weather_zone  — Short-Term Load Forecast by weather zone
      np4-732-cd/wpp_hrly_avrg_actl_fcast                  — Wind Power Production hourly forecast
      np4-745-cd/spp_hrly_actual_fcast_geo — Solar PV Generation Forecast by weather zone

    Returns:
    {
      "ercot_forecasts": {
        "generated_at": "ISO",
        "forecast_date": "YYYY-MM-DD",
        "hourly_24hr": {
          "hours":      [0..23],           # HE labels for today+tomorrow
          "timestamps": ["YYYY-MM-DD HH"], # human-readable
          "gross_load": [float, ...],      # GW
          "wind":       [float, ...],      # GW
          "solar":      [float, ...],      # GW
          "net_load":   [float, ...],      # GW = gross - wind - solar
        },
        "daily_7day": {
          "dates":      ["YYYY-MM-DD", ...],
          "gross_load_peak": [float, ...], # GW daily peak
          "wind_avg":        [float, ...], # GW daily average
          "solar_peak":      [float, ...], # GW daily peak
          "net_load_peak":   [float, ...], # GW daily peak net load
        },
        "source": "ERCOT Public API"
      }
    }
    """
    base = "https://api.ercot.com/api/public-reports"
    headers = {
        "Authorization":     f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": sub_key,
    }

    today     = date.today()
    day7_end  = (today + timedelta(days=7)).isoformat()
    day2_end  = (today + timedelta(days=2)).isoformat()
    today_str = today.isoformat()

    def _ercot_get(path, params):
        try:
            url = f"{base}/{path}"
            r   = requests.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            body = r.json()
            # Debug: show top-level keys on first call to understand structure
            if not hasattr(_ercot_get, "_debugged"):
                _ercot_get._debugged = True
                print(f"    DEBUG ERCOT response keys: {list(body.keys())[:8]}")
                data_val = body.get("data")
                print(f"    DEBUG data type: {type(data_val).__name__}, "
                      f"sample: {str(data_val)[:200]}")
            # Handle all known ERCOT response shapes:
            # Shape A: {"data": [[v1,v2,...], ...], "fields": ["f1","f2",...]}
            # Shape B: {"data": [{"f1":v1,...}, ...]}
            # Shape C: {"data": {"rows": [...], "fields": [...]}}  (nested)
            # Shape D: {"_meta": {...}, "data": [...]}
            fields = body.get("fields") or []
            raw    = body.get("data")   or []
            # Shape C — unwrap nested dict
            if isinstance(raw, dict):
                fields = raw.get("fields") or fields
                raw    = raw.get("rows") or raw.get("data") or []
            if not raw:
                return []
            rows = raw
            # Shape A — list-of-lists, convert to dicts
            if isinstance(rows[0], list):
                if fields:
                    rows = [dict(zip(fields, row)) for row in rows]
                else:
                    return []
            return rows
        except Exception as e:
            print(f"  WARN [ERCOT forecast] {path} — {e}")
            return []

    print("  Pulling ERCOT short-term load forecast...")
    load_rows = _ercot_get(
        "np3-565-cd/lf_by_model_weather_zone",
        {"deliveryDateFrom": today_str, "deliveryDateTo": day7_end,
         "size": 5000}
    )

    print("  Pulling ERCOT wind power forecast...")
    wind_rows = _ercot_get(
        "np4-732-cd/wpp_hrly_avrg_actl_fcast",
        {"deliveryDateFrom": today_str, "deliveryDateTo": day7_end,
         "size": 5000}
    )

    print("  Pulling ERCOT solar PV forecast...")
    solar_rows = _ercot_get(
        "np4-745-cd/spp_hrly_actual_fcast_geo",
        {"deliveryDateFrom": today_str, "deliveryDateTo": day7_end,
         "size": 5000}
    )

    # ── Parse load forecast ────────────────────────────────────────────────
    # Fields: deliveryDate, hourEnding, systemTotal (MW) or weatherZone + value
    # We want ERCOT system total — sum all zones per hour
    load_by_dt = {}  # "YYYY-MM-DD HH" → MW
    for row in load_rows:
        dt  = str(row.get("deliveryDate") or row.get("DeliveryDate") or "")[:10]
        he  = str(row.get("hourEnding")   or row.get("HourEnding")   or "0")
        # hourEnding is 1-24, convert to 0-23
        try:
            hour = int(str(he).split(":")[0]) - 1
        except:
            hour = 0
        # System total — try multiple field names
        mw = safe_float(
            row.get("systemTotal")  or row.get("SystemTotal")  or
            row.get("loadForecast") or row.get("LoadForecast") or
            row.get("mtlf")         or row.get("Mtlf")         or
            row.get("total")        or row.get("Total")        or 0
        )
        if dt and mw > 0:
            key = f"{dt} {hour:02d}"
            load_by_dt[key] = load_by_dt.get(key, 0) + mw

    # ── Parse wind forecast ────────────────────────────────────────────────
    wind_by_dt = {}
    for row in wind_rows:
        dt  = str(row.get("deliveryDate") or row.get("DeliveryDate") or "")[:10]
        he  = str(row.get("hourEnding")   or row.get("HourEnding")   or "0")
        try:
            hour = int(str(he).split(":")[0]) - 1
        except:
            hour = 0
        mw = safe_float(
            row.get("genHSL")            or row.get("GenHSL")            or
            row.get("wppHSL")            or row.get("WppHSL")            or
            row.get("systemTotal")       or row.get("SystemTotal")       or
            row.get("windPowerForecast") or row.get("WindPowerForecast") or
            row.get("genForecast")       or row.get("GenForecast")       or 0
        )
        if dt and mw > 0:
            key = f"{dt} {hour:02d}"
            wind_by_dt[key] = wind_by_dt.get(key, 0) + mw

    # ── Parse solar forecast ───────────────────────────────────────────────
    solar_by_dt = {}
    for row in solar_rows:
        dt  = str(row.get("deliveryDate") or row.get("DeliveryDate") or "")[:10]
        he  = str(row.get("hourEnding")   or row.get("HourEnding")   or "0")
        try:
            hour = int(str(he).split(":")[0]) - 1
        except:
            hour = 0
        mw = safe_float(
            row.get("stppf")             or row.get("Stppf")             or
            row.get("pvgrpp")            or row.get("Pvgrpp")            or
            row.get("genHSL")            or row.get("GenHSL")            or
            row.get("genForecast")       or row.get("GenForecast")       or
            row.get("pvGenForecast")     or row.get("PvGenForecast")     or 0
        )
        if dt and mw > 0:
            key = f"{dt} {hour:02d}"
            solar_by_dt[key] = solar_by_dt.get(key, 0) + mw

    # ── Build 24-hour hourly series (today + tomorrow) ─────────────────────
    hourly_keys = sorted(set(
        list(load_by_dt.keys()) + list(wind_by_dt.keys()) + list(solar_by_dt.keys())
    ))[:48]  # cap at 48 hours

    h24_timestamps = []
    h24_load = []
    h24_wind = []
    h24_solar = []
    h24_net   = []

    for key in hourly_keys:
        gl  = round(load_by_dt.get(key, 0) / 1000, 2)   # MW → GW
        wnd = round(wind_by_dt.get(key, 0) / 1000, 2)
        sol = round(solar_by_dt.get(key, 0) / 1000, 2)
        net = round(gl - wnd - sol, 2)
        h24_timestamps.append(key)
        h24_load.append(gl)
        h24_wind.append(wnd)
        h24_solar.append(sol)
        h24_net.append(net)

    # ── Build 7-day daily series ───────────────────────────────────────────
    all_dates = sorted(set(k[:10] for k in
        list(load_by_dt.keys()) + list(wind_by_dt.keys()) + list(solar_by_dt.keys())
    ))[:7]

    d7_dates     = []
    d7_load_peak = []
    d7_wind_avg  = []
    d7_solar_peak= []
    d7_net_peak  = []

    for day in all_dates:
        day_load  = [load_by_dt.get(f"{day} {h:02d}", 0) / 1000 for h in range(24)]
        day_wind  = [wind_by_dt.get(f"{day} {h:02d}", 0) / 1000 for h in range(24)]
        day_solar = [solar_by_dt.get(f"{day} {h:02d}", 0) / 1000 for h in range(24)]
        day_net   = [day_load[h] - day_wind[h] - day_solar[h] for h in range(24)]

        d7_dates.append(day)
        d7_load_peak.append(round(max(day_load), 2)  if any(day_load)  else 0)
        d7_wind_avg.append(round(sum(day_wind) / max(len([x for x in day_wind if x > 0]), 1), 2))
        d7_solar_peak.append(round(max(day_solar), 2) if any(day_solar) else 0)
        d7_net_peak.append(round(max(day_net), 2)    if any(day_net)   else 0)

    # Debug: log sample row keys if no data parsed
    if not load_by_dt and load_rows:
        sample = load_rows[0] if load_rows else {}
        print(f"    DEBUG load row keys: {list(sample.keys())[:10]}")
        print(f"    DEBUG load sample: { {k: sample[k] for k in list(sample.keys())[:5]} }")
    if not wind_by_dt and wind_rows:
        sample = wind_rows[0] if wind_rows else {}
        print(f"    DEBUG wind row keys: {list(sample.keys())[:10]}")
    if not solar_by_dt and solar_rows:
        sample = solar_rows[0] if solar_rows else {}
        print(f"    DEBUG solar row keys: {list(sample.keys())[:10]}")
    print(f"    Load: {len(load_by_dt)} intervals · Wind: {len(wind_by_dt)} · Solar: {len(solar_by_dt)}")
    print(f"    24hr series: {len(h24_timestamps)} hours · 7-day series: {len(d7_dates)} days")

    return {
        "ercot_forecasts": {
            "generated_at":  datetime.utcnow().isoformat() + "Z",
            "forecast_date": today_str,
            "hourly_24hr": {
                "timestamps": h24_timestamps,
                "gross_load": h24_load,
                "wind":       h24_wind,
                "solar":      h24_solar,
                "net_load":   h24_net,
            },
            "daily_7day": {
                "dates":           d7_dates,
                "gross_load_peak": d7_load_peak,
                "wind_avg":        d7_wind_avg,
                "solar_peak":      d7_solar_peak,
                "net_load_peak":   d7_net_peak,
            },
            "source": "ERCOT Public API",
        }
    }

# ══════════════════════════════════════════════════════════════════════════════
# CONVENIENCE — collect all four active integrations in one call
# ══════════════════════════════════════════════════════════════════════════════

def collect_all_integrations(token=None, sub_key=None, asset_nodes=None):
    """
    Calls all four active collectors and returns a single merged dict ready
    to be merged into the `data` dict inside collect_data().

    Usage (at the end of collect_data() in hen_morning_report.py):

        from hen_integrations import collect_all_integrations
        ...
        extras = collect_all_integrations(token=token, sub_key=sub_key,
                                          asset_nodes=NODES)
        data.update(extras)

    Active keys returned:
      data["constraints"]  — list of top-5 ERCOT binding constraints
      data["weather"]      — AG2 15-day weather dict
      data["modo"]         — Modo Energy index dict
      data["asset_status"] — PowerTools fleet availability + outage schedule

    Coming soon (uncomment when access is provisioned):
      data["forecasts"]     — Meteologica 7-day load/wind/solar forecasts
      data["forward_curve"] — Drew internal ERCOT hub forward curve
    """
    out = {}

    if token and sub_key:
        print("\n── Integration 1/4: ERCOT binding constraints ──")
        out.update(collect_ercot_constraints(token, sub_key, asset_nodes))
    else:
        print("  SKIP [ERCOT constraints] — no ERCOT token provided")
        out["constraints"] = []

    print("\n── Integration 2/4: AG2 15-day weather ──")
    out.update(collect_ag2_weather())

    print("\n── Integration 3/4: Modo Energy indices ──")
    out.update(collect_modo_indices())

    print("\n── Integration 4/4: PowerTools asset availability ──")
    out.update(collect_powertools_assets())

    if token and sub_key:
        print("\n── Integration 5/5: ERCOT load/wind/solar forecasts ──")
        out.update(collect_ercot_forecasts(token, sub_key))
    else:
        out["ercot_forecasts"] = {}

    return out


# ══════════════════════════════════════════════════════════════════════════════
# COMING SOON — preserved stubs (uncomment when access is provisioned)
# ══════════════════════════════════════════════════════════════════════════════

# ── Meteologica 7-day forecasts ───────────────────────────────────────────────
# Uncomment this entire block and add to collect_all_integrations() when ready.
#
# METEOLOGICA_BASE = "https://api.meteologica.es/v1"
#
# def collect_meteologica_forecasts():
#     site_id = os.environ.get("METEOLOGICA_SITE_ID", "ERCOT")
#     base_params = {
#         "site_id": site_id,
#         "from":    f"{TODAY_STR}T00:00:00",
#         "to":      f"{(date.today() + timedelta(days=7)).isoformat()}T23:00:00",
#         "interval": "1h",
#     }
#     forecast_keys = {
#         "gross_load": "forecasts/load",   "net_load": "forecasts/net-load",
#         "wind":       "forecasts/wind",   "solar":    "forecasts/solar",
#     }
#     result = {"generated_at": datetime.utcnow().isoformat()+"Z",
#               "horizon_days": 7, "source": "Meteologica"}
#     for key, path in forecast_keys.items():
#         r = requests.get(f"{METEOLOGICA_BASE}/{path}",
#                          headers={"X-API-Key": os.environ.get("METEOLOGICA_API_KEY",""),
#                                   "Accept": "application/json"},
#                          params=base_params, timeout=20)
#         rows = r.json().get("forecasts") or []
#         by_day = defaultdict(dict)
#         for row in rows:
#             dt = datetime.fromisoformat(row["timestamp"].replace("Z","+00:00"))
#             by_day[dt.strftime("%Y-%m-%d")][dt.hour] = round(safe_float(row["value"])/1000, 2)
#         result[key] = {d: {"peak_gw": round(max(h.values()),2),
#                            "hourly":  {str(hr): v for hr,v in sorted(h.items())}}
#                        for d, h in by_day.items()}
#     return {"forecasts": result}


# ── Drew forward curve ────────────────────────────────────────────────────────
# Uncomment this entire block and add to collect_all_integrations() when ready.
#
# def collect_forward_curve():
#     drew_url = os.environ.get("DREW_API_URL","").rstrip("/")
#     hub      = os.environ.get("DREW_HUB","HB_NORTH")
#     if not drew_url:
#         return {"forward_curve": {}}
#     r = requests.get(f"{drew_url}/forward-curve",
#                      headers={"Authorization": f"Bearer {os.environ.get('DREW_API_KEY','')}",
#                               "Accept": "application/json"},
#                      params={"market":"ERCOT","hub":hub,
#                              "products":"bal_month,cal_month,cal_quarter,cal_year",
#                              "as_of": TODAY_STR},
#                      timeout=20)
#     raw = r.json().get("curve") or r.json().get("products") or r.json().get("data") or []
#     products = [{"product": str(row.get("product","")),
#                  "period":  str(row.get("period","")),
#                  "price":   round(safe_float(row.get("price",0)), 2),
#                  "prev_price": round(safe_float(row.get("prev_price", row.get("price",0))), 2),
#                  "change_dod": round(safe_float(row.get("price",0)) -
#                                     safe_float(row.get("prev_price", row.get("price",0))), 2),
#                  "unit": "$/MWh"} for row in raw]
#     return {"forward_curve": {"products": products, "as_of": TODAY_STR,
#                               "hub": hub, "source": "Drew Internal"}}
