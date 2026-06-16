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
  AG2_ACCOUNT               AG2 Trader username (your wsitrader.com login email username)
  AG2_PROFILE               AG2 Trader profile (your wsitrader.com login email address)
  AG2_PASSWORD              AG2 Trader password (your wsitrader.com password)
  MODO_API_KEY              Modo Energy X-Token (from modoenergy.com/profile/developers)
  MODO_INDEX_IDS            Optional — comma-separated "name:id" pairs to skip discovery.
  POWERTOOLS_URL            Full URL to your PowerTools platform
  POWERTOOLS_API_KEY        PowerTools API key or Bearer token (if required)
  POWERTOOLS_USERNAME       PowerTools login username (if auth is form-based)
  POWERTOOLS_PASSWORD       PowerTools login password (if auth is form-based)
"""

import os
import json
import time
import requests
from datetime import date, timedelta, datetime
from collections import defaultdict

# ── Shared date constants ─────────────────────────────────────────────────────
TODAY_STR  = date.today().isoformat()
YESTERDAY  = (date.today() - timedelta(days=1)).isoformat()
IN_15_DAYS = (date.today() + timedelta(days=15)).isoformat()
PRIOR_15   = (date.today() - timedelta(days=15)).isoformat()
DAY_BEFORE = (date.today() - timedelta(days=2)).isoformat()

MODO_DATE             = (date.today() - timedelta(days=62)).isoformat()
MODO_WINDOW_START_1HR = "2026-01-01"
MODO_WINDOW_START_2HR = "2026-01-21"


def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# 1.  ERCOT — Yesterday's top-5 binding constraints + per-asset shift factors
# ══════════════════════════════════════════════════════════════════════════════

def collect_ercot_constraints(token, sub_key, asset_nodes=None):
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
            r = requests.get(f"{BASE}/{path}", headers=headers, params=p, timeout=25)
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

    # ── Pull SCED shadow prices and binding transmission constraints ────────
    # Endpoint: np6-86-cd/shdw_prices_bnd_trns_const
    # Uses SCEDTimestamp range for yesterday (CT = UTC-5)
    print("  Pulling ERCOT SCED shadow prices and binding constraints...")

    YESTERDAY_FROM = YESTERDAY + "T00:00:00"
    YESTERDAY_TO   = YESTERDAY + "T23:59:59"

    sced_rows = []
    page = 1
    while True:
        batch = _ercot_get(
            "np6-86-cd/shdw_prices_bnd_trns_const",
            {
                "SCEDTimestampFrom": YESTERDAY_FROM,
                "SCEDTimestampTo":   YESTERDAY_TO,
                "page":              page,
            },
        )
        if not batch:
            break
        sced_rows.extend(batch)
        if len(batch) < 5000:
            break
        page += 1

    print(f"    {len(sced_rows)} SCED constraint rows fetched")


    # ── Parse rows into per-constraint, per-hour buckets ─────────────────────
    # Each row: SCEDTimestamp, constraintName, contingencyName,
    #           shadowPrice, maxShadowPrice, limit, value, violatedMW,
    #           fromStation, toStation, fromStationkV, toStationkV

    from collections import defaultdict

    # Structure: constraint_data[name][he] = list of shadow prices
    constraint_data = defaultdict(lambda: {
        "by_hour":        defaultdict(list),  # he -> [shadowPrice, ...]
        "contingency":    "",
        "from_station":   "",
        "to_station":     "",
        "from_kv":        0,
        "to_kv":          0,
        "violated_mw":    [],
        "shadow_prices":  [],
    })

    for row in sced_rows:
        if isinstance(row, dict):
            ts       = str(row.get("SCEDTimestamp",    ""))
            c_name   = str(row.get("constraintName",   "")).strip()
            cont     = str(row.get("contingencyName",  "")).strip()
            shadow   = safe_float(row.get("shadowPrice",    0))
            from_st  = str(row.get("fromStation",      "")).strip()
            to_st    = str(row.get("toStation",        "")).strip()
            from_kv  = safe_float(row.get("fromStationkVFrom", 0) or row.get("fromStationkV", 0))
            to_kv    = safe_float(row.get("toStationkVFrom",   0) or row.get("toStationkV",   0))
            viol_mw  = safe_float(row.get("violatedMW",        0))
        elif isinstance(row, list) and len(row) >= 6:
            # Positional format confirmed from API response:
            # [0] SCEDTimestamp, [1] repeatedHourFlag, [2] constraintID,
            # [3] constraintName, [4] contingencyName, [5] shadowPrice,
            # [6] maxShadowPrice, [7] limit, [8] value, [9] violatedMW,
            # [10] fromStation, [11] toStation, [12] fromStationkV, [13] toStationkV
            ts       = str(row[0])
            c_name   = str(row[3]).strip()
            cont     = str(row[4]).strip() if len(row) > 4 else ""
            shadow   = safe_float(row[5])  if len(row) > 5 else 0
            from_st  = str(row[10]).strip() if len(row) > 10 else ""
            to_st    = str(row[11]).strip() if len(row) > 11 else ""
            from_kv  = safe_float(row[12]) if len(row) > 12 else 0
            to_kv    = safe_float(row[13]) if len(row) > 13 else 0
            viol_mw  = safe_float(row[9])  if len(row) > 9  else 0
        else:
            continue

        if not c_name or shadow <= 0:
            continue

        # Derive hour-ending from SCEDTimestamp
        try:
            he = int(ts[11:13]) + 1 if "T" in ts else 0
            if he > 24:
                he = 24
        except Exception:
            he = 0

        cd = constraint_data[c_name]
        cd["by_hour"][he].append(shadow)
        cd["shadow_prices"].append(shadow)
        if viol_mw > 0:
            cd["violated_mw"].append(viol_mw)
        if not cd["contingency"]  and cont:
            cd["contingency"]  = cont
        if not cd["from_station"] and from_st:
            cd["from_station"] = from_st
        if not cd["to_station"]   and to_st:
            cd["to_station"]   = to_st
        if not cd["from_kv"]      and from_kv:
            cd["from_kv"]      = from_kv
        if not cd["to_kv"]        and to_kv:
            cd["to_kv"]        = to_kv

    # ── Rank constraints by total impact (avg shadow × hours binding) ─────────
    def _constraint_score(cd):
        prices = cd["shadow_prices"]
        if not prices:
            return 0
        avg = sum(prices) / len(prices)
        hrs = len(cd["by_hour"])
        return avg * hrs

    ranked_names = sorted(
        constraint_data.keys(),
        key=lambda n: _constraint_score(constraint_data[n]),
        reverse=True
    )[:10]  # keep top 10

    print(f"    {len(constraint_data)} unique constraints · top: {ranked_names[:3]}")

    # ── Build node exposure map ───────────────────────────────────────────────
    # Without a PTDF API we match nodes to constraints by electrical proximity:
    # check if the node name contains the fromStation or toStation substring,
    # or if the node is in a region that commonly correlates with that line.
    # Exposure is flagged as positive/negative based on flow direction inference.

    # ── Real shift factor lookup tables (from Congestion_Proj_4_27_25-5_05_26.xlsx) ──
    # Exact match by full reported constraint name
    CONSTRAINT_SF = {
        'ABNTHWST 138KV ABNTHW_SERDEV1_1': {'CISC_RN': 0.13947},
        'ARGYL-LWSVH 138KV 587__A': {'DIBOL_RN': 0.01469},
        'ASHERTON-CATARINA 138KV ASHERT_CATARI1_1': {'CATARINA_B1': 0.76982, 'HOLCOMB_RN1': 0.17245, 'HAMI_BESS_RN': -0.09, 'FTDUNCAN_RN': -0.116},
        'BCESW-SNDSW 345KV 421__A': {'CATARINA_B1': 0.1, 'HOLCOMB_RN1': 0.11, 'HAMI_BESS_RN': 0.07, 'FTDUNCAN_RN': 0.09, 'JDKNS_RN': -0.04, 'SADLBACK_RN': -0.04, 'MV_VALV4_RN': 0.11, 'CISC_RN': -0.06, 'DIBOL_RN': -0.074, 'OLNEYTN_RN': -0.065},
        'BIG_FOOT-PLEASANT 138KV BIG_FO_PLEASA1_1': {'CATARINA_B1': -0.092, 'HOLCOMB_RN1': -0.0534, 'HAMI_BESS_RN': -0.088, 'FTDUNCAN_RN': -0.098},
        'BLESSING 345KV BLESSING_1382': {'HOLCOMB_RN1': 0.041, 'MV_VALV4_RN': 0.063},
        'BOW 69KV BOW_FMR1': {'OLNEYTN_RN': -0.05},
        'BR-HOC 138KV BR_HOC09_A': {'MAINLAND_RN': 0.09515},
        'BRACKETT-ESCONDID 138KV BRACKE_ESCOND1_1': {'RUSSEKST_RN': -0.04194, 'CATARINA_B1': 0.10133, 'HOLCOMB_RN1': 0.04094, 'HAMI_BESS_RN': -0.32295},
        'BRUNI 138KV BRUNI_69_1': {'CATARINA_B1': 0.01044, 'FTDUNCAN_RN': 0.01044},
        'CARVER-TINSLEY 138KV CARVER_TINSLE1_1': {'RUSSEKST_RN': -0.07, 'CATARINA_B1': 0.09, 'HAMI_BESS_RN': 0.32323, 'FTDUNCAN_RN': 0.21},
        'CATARINA-ASHERTON 138KV ASHERT_CATARI1_1': {'CATARINA_B1': -0.732, 'HOLCOMB_RN1': -0.09013, 'HAMI_BESS_RN': 0.0722, 'MV_VALV4_RN': -0.0514},
        'CMNSW-CMNTP 138KV 651__B': {'CISC_RN': 0.05},
        'CNRSW 345KV CNRSW_MR2H': {'DIBOL_RN': -0.14082},
        'COLETO-VICTORIA 138KV COLETO_VICTOR2_1': {'MV_VALV4_RN': -0.0917},
        'CPSES-MBDSW 345KV 6033__A': {'RUSSEKST_RN': -0.06, 'JUNCTION_RN': -0.045, 'JDKNS_RN': -0.059, 'SADLBACK_RN': -0.058},
        'CRD 345KV CRD_CRD2': {'CISC_RN': 0.06},
        'CRDSW-OLNEY 69KV 6830__B': {'OLNEYTN_RN': 0.78682},
        'CRTRVLLE-HILGR 138KV 16050__B': {'JDKNS_RN': 0.04, 'SADLBACK_RN': 0.04},
        'DOW-OAS 345KV DOWOAS18_A': {'CATARINA_B1': -0.0722, 'HOLCOMB_RN1': -0.0768, 'HAMI_BESS_RN': -0.048, 'FTDUNCAN_RN': -0.0599, 'JUNCTION_RN': -0.034, 'MV_VALV4_RN': -0.0823, 'MAINLAND_RN': 0.1758},
        'DOWNIES-MOORE 138KV 2585_1': {'CATARINA_B1': -0.06, 'HAMI_BESS_RN': -0.15515, 'FTDUNCAN_RN': -0.18205},
        'EINSTEIN-CRTRVLLE 138KV CRTVLE_EINSTEN_1': {'JDKNS_RN': 0.04849, 'SADLBACK_RN': 0.03842},
        'ELGISW-BUTLER 138KV 203T260_1': {'RUSSEKST_RN': -0.02, 'CATARINA_B1': 0.04196, 'HOLCOMB_RN1': 0.04516, 'JDKNS_RN': -0.02, 'SADLBACK_RN': -0.02, 'MV_VALV4_RN': 0.046, 'CISC_RN': -0.027, 'DIBOL_RN': -0.026, 'OLNEYTN_RN': -0.027},
        'ESCONDID-GANSO 138KV ESCOND_GANSO1_1': {'RUSSEKST_RN': 0.05, 'CATARINA_B1': -0.094, 'HOLCOMB_RN1': -0.044, 'HAMI_BESS_RN': 0.4035, 'FTDUNCAN_RN': -0.2614},
        'ESTLD-MRVLY 69KV 6635__G': {'CISC_RN': -0.045, 'OLNEYTN_RN': 0.053},
        'EXCSW-RNKSW 345KV 109__A': {'RUSSEKST_RN': -0.0582, 'HAMI_BESS_RN': -0.039, 'FTDUNCAN_RN': -0.032, 'JUNCTION_RN': -0.0429, 'JDKNS_RN': -0.064, 'SADLBACK_RN': -0.062, 'CISC_RN': -0.066, 'DIBOL_RN': 0.04448, 'OLNEYTN_RN': -0.068},
        'E_PASP': {'CATARINA_B1': -0.08556, 'HOLCOMB_RN1': -0.13778, 'JUNCTION_RN': 0.04, 'MV_VALV4_RN': -0.19625},
        'FMRVL-RYSSW 345KV 400__A': {'DIBOL_RN': 0.05227, 'OLNEYTN_RN': -0.03425},
        'FORTMA-YELWJCKT 138KV FORTMA_YELWJC1_1': {'RUSSEKST_RN': 0.032, 'JUNCTION_RN': 0.5, 'JDKNS_RN': 0.02, 'SADLBACK_RN': 0.02},
        'FTSSW-VENSW 345KV 35050__B': {'CATARINA_B1': -0.0569, 'HOLCOMB_RN1': -0.06, 'HAMI_BESS_RN': -0.037, 'FTDUNCAN_RN': -0.047, 'JUNCTION_RN': -0.041, 'MV_VALV4_RN': -0.06},
        'GANSO-MAVERICK 138KV GANSO_MAVERI1_1': {'CATARINA_B1': -0.09232, 'HOLCOMB_RN1': -0.04424, 'HAMI_BESS_RN': 0.40193},
        'GN-PZ 138KV GN_PZ_08_A': {'MAINLAND_RN': 0.18322},
        'GT-BG 138KV BG_GT_37_A': {'MAINLAND_RN': 0.14257},
        'HAMILTON-MAVERICK 138KV HAMILT_MAVERI1_1': {'CATARINA_B1': 0.072, 'HAMI_BESS_RN': -0.56, 'FTDUNCAN_RN': 0.19612},
        'HCKSW 1KV HCKSW_MR2L': {'RUSSEKST_RN': -0.035, 'JDKNS_RN': -0.04, 'SADLBACK_RN': -0.04, 'OLNEYTN_RN': -0.03},
        'HOLMAN-CISTERN 345KV CKT_3136_1': {'CATARINA_B1': 0.05598, 'HOLCOMB_RN1': 0.05233, 'HAMI_BESS_RN': 0.05428, 'FTDUNCAN_RN': 0.0563, 'JUNCTION_RN': 0.0649, 'MAINLAND_RN': -0.07052},
        'HONDOCK-DEVINESW 69KV HON_DEVI_1': {'CATARINA_B1': -0.05, 'HAMI_BESS_RN': -0.06, 'FTDUNCAN_RN': -0.064},
        'I_FW_N': {'RUSSEKST_RN': 0.09, 'JDKNS_RN': 0.1566, 'SADLBACK_RN': 0.1374, 'DIBOL_RN': -0.04, 'OLNEYTN_RN': -0.06},
        'I_KALO': {'HOLCOMB_RN1': 0.065, 'MV_VALV4_RN': 0.25691},
        'JEWET-BBSES 345KV 50__A': {'RUSSEKST_RN': 0.04, 'CATARINA_B1': -0.04, 'HOLCOMB_RN1': -0.05, 'JDKNS_RN': 0.04, 'SADLBACK_RN': 0.04, 'MV_VALV4_RN': -0.05, 'MAINLAND_RN': -0.1, 'CISC_RN': 0.068, 'DIBOL_RN': 0.12874, 'OLNEYTN_RN': 0.0833},
        'KENDAL-BERGHE 345KV 656T656_1': {'RUSSEKST_RN': -0.13825, 'CATARINA_B1': 0.07977, 'HOLCOMB_RN1': 0.09654, 'JUNCTION_RN': -0.15722, 'JDKNS_RN': -0.13169, 'SADLBACK_RN': -0.14458, 'MV_VALV4_RN': 0.10349},
        'KLNSW-HHSTH 138KV 630__B': {'RUSSEKST_RN': -0.05, 'JDKNS_RN': -0.048, 'SADLBACK_RN': -0.048, 'CISC_RN': -0.04},
        'KLNSW-STAGE 138KV 641__A': {'RUSSEKST_RN': -0.04, 'JDKNS_RN': -0.04, 'SADLBACK_RN': -0.04, 'MV_VALV4_RN': 0.021, 'CISC_RN': -0.03},
        'LAKENASW-SAMATHIS 69KV LAKENA_SAMATH1_1': {'RUSSEKST_RN': 0.07252},
        'LARDVNTH-LASCRUCE 138KV LARDVN_LASCRU1_1': {'RUSSEKST_RN': 0.01, 'CATARINA_B1': 0.17885, 'HOLCOMB_RN1': 0.29736, 'HAMI_BESS_RN': 0.06158, 'MV_VALV4_RN': -0.079},
        'LAREDO-DEL_MAR 138KV DEL_MA_LAREDO1_1': {'HOLCOMB_RN1': 0.06},
        'LASCRUCE-MILO 138KV LASCRU_MILO1_1': {'CATARINA_B1': 0.17261, 'HOLCOMB_RN1': 0.28025, 'HAMI_BESS_RN': 0.07, 'FTDUNCAN_RN': 0.09648, 'MV_VALV4_RN': -0.08},
        'LA_PALMA-HAINE_DR 138KV HAINE__LA_PAL1_1': {'HOLCOMB_RN1': 0.01138, 'MV_VALV4_RN': 0.01856},
        'LHSES-PRCSW 138KV 3660__A': {'DIBOL_RN': -0.05},
        'LNGSW-CONSW 345KV 6056__A': {'RUSSEKST_RN': 0.08, 'JDKNS_RN': 0.213, 'SADLBACK_RN': 0.2},
        'LNGSW-CONSW 345KV 6056__Z': {'RUSSEKST_RN': 0.08513, 'HOLCOMB_RN1': -0.02, 'JUNCTION_RN': -0.02, 'JDKNS_RN': 0.21466, 'SADLBACK_RN': 0.20517, 'MV_VALV4_RN': -0.02, 'MAINLAND_RN': -0.02, 'CISC_RN': -0.02, 'DIBOL_RN': -0.02, 'OLNEYTN_RN': -0.02},
        'LNGSW-PRLSW 345KV 6965__A': {'RUSSEKST_RN': 0.07, 'CATARINA_B1': -0.02, 'HOLCOMB_RN1': -0.02, 'HAMI_BESS_RN': 0.02, 'FTDUNCAN_RN': 0.02, 'JUNCTION_RN': -0.02, 'JDKNS_RN': 0.21329, 'SADLBACK_RN': 0.20412, 'MV_VALV4_RN': -0.02, 'MAINLAND_RN': -0.02, 'CISC_RN': -0.02, 'DIBOL_RN': -0.02, 'OLNEYTN_RN': -0.02},
        'LOBO-LARDVNTH 138KV LARDVN_LOBO2_1': {'CATARINA_B1': 0.21816, 'HOLCOMB_RN1': 0.32386, 'HAMI_BESS_RN': 0.07, 'FTDUNCAN_RN': 0.07, 'MV_VALV4_RN': -0.07},
        'LPLMK-LPLNE 115KV LPLMK_LPLNE_1': {'RUSSEKST_RN': 0.01044, 'HAMI_BESS_RN': 0.00574, 'JDKNS_RN': 0.01594, 'SADLBACK_RN': 0.01487},
        'MADDUX-SAPOWER 138KV MADDUX_SAPOWE1_1': {'RUSSEKST_RN': -0.0695, 'HAMI_BESS_RN': -0.11666, 'FTDUNCAN_RN': -0.07369, 'JUNCTION_RN': -0.33601},
        'MADDUX-TREADWEL 138KV MADDUX_TREADW1_1': {'JUNCTION_RN': 0.39252},
        'MASN-KATEMCY 69KV KATEMC_MASN1_1': {'JUNCTION_RN': 0.251},
        'MAVERICK-HAMILTON 138KV HAMILT_MAVERI1_1': {'RUSSEKST_RN': 0.05256, 'CATARINA_B1': -0.08936, 'HAMI_BESS_RN': 0.40605, 'FTDUNCAN_RN': -0.25822},
        'MAXWELL-HAMILTON 138KV HAMILT_MAXWEL1_1': {'RUSSEKST_RN': -0.05, 'CATARINA_B1': 0.05, 'HAMI_BESS_RN': 0.22977, 'FTDUNCAN_RN': 0.1432},
        'MCCAMY': {'RUSSEKST_RN': -0.24657, 'HAMI_BESS_RN': -0.11282, 'FTDUNCAN_RN': -0.06874},
        'MDO-PHR 345KV MDOPHR99_A': {'CATARINA_B1': -0.091, 'HOLCOMB_RN1': -0.096, 'HAMI_BESS_RN': -0.06, 'FTDUNCAN_RN': -0.077, 'MV_VALV4_RN': -0.1},
        'MGSES-CATSW 345KV 6945__A': {'RUSSEKST_RN': 0.1, 'JDKNS_RN': 0.3, 'SADLBACK_RN': 0.27037, 'MAINLAND_RN': -0.05, 'CISC_RN': -0.08, 'DIBOL_RN': -0.06, 'OLNEYTN_RN': -0.07},
        'MILLER-HENLY 138KV 415T415_1': {'JUNCTION_RN': -0.11},
        'MRVLY-ESTLD 69KV 6635__G': {'CISC_RN': 0.17348},
        'MV_BURNS-MV_HBRG4 138KV BURNS_HEIDLBRG_1': {'MV_VALV4_RN': 0.15343},
        'NLARSW-PILONCIL 138KV NLARSW_PILONC1_1': {'CATARINA_B1': 0.26, 'HOLCOMB_RN1': -0.09, 'HAMI_BESS_RN': 0.08633, 'FTDUNCAN_RN': 0.12, 'MV_VALV4_RN': -0.06},
        'NORTMC-CROSSOVE 138KV CROSSO_NORTMC1_1': {'RUSSEKST_RN': 0.34171, 'HAMI_BESS_RN': 0.09187, 'FTDUNCAN_RN': 0.057, 'JDKNS_RN': -0.028, 'SADLBACK_RN': -0.0235},
        'NVKSW-ANARN 69KV 6840__B': {'OLNEYTN_RN': 0.476},
        'ODESA-ODNTH 138KV 6513__A': {'SADLBACK_RN': 0.08},
        'OZONA-MIDW 69KV MIDW_OZONA1_1': {'RUSSEKST_RN': 0.06},
        'PALOUSE-WOLFCAMP 138KV PALOUS_WOLFCA1_1': {'RUSSEKST_RN': 0.30569},
        'PILONCIL-CATARINA 138KV CATARI_PILONC1_1': {'CATARINA_B1': 0.337, 'HOLCOMB_RN1': -0.098},
        'PILONCIL-NLARSW 138KV NLARSW_PILONC1_1': {'CATARINA_B1': -0.26481, 'HOLCOMB_RN1': 0.23807, 'HAMI_BESS_RN': -0.09445, 'FTDUNCAN_RN': -0.12963, 'MV_VALV4_RN': 0.076},
        'PRLSW-CONSW 345KV 6960__A': {'RUSSEKST_RN': 0.11076, 'HAMI_BESS_RN': 0.0458, 'JDKNS_RN': 0.29043, 'SADLBACK_RN': 0.2337, 'CISC_RN': -0.0525, 'DIBOL_RN': -0.041, 'OLNEYTN_RN': -0.05},
        'RIOHONDO-MV_BURNS 138KV BURNS_RIOHONDO_1': {'MV_VALV4_RN': 0.1976},
        'SAMSW-FVLSW 345KV 35045__A': {'CATARINA_B1': -0.05577, 'HOLCOMB_RN1': -0.05843, 'HAMI_BESS_RN': -0.04, 'FTDUNCAN_RN': -0.04823, 'MV_VALV4_RN': -0.05925, 'CISC_RN': 0.0357, 'DIBOL_RN': 0.04524, 'OLNEYTN_RN': 0.05485},
        'SAMSW-VENSW 345KV 35055__A': {'CATARINA_B1': -0.053, 'HOLCOMB_RN1': -0.056, 'FTDUNCAN_RN': -0.044, 'MV_VALV4_RN': -0.056, 'MAINLAND_RN': -0.04, 'DIBOL_RN': 0.053, 'OLNEYTN_RN': 0.05},
        'SANA 138KV SANA_FMR1': {'CISC_RN': 0.04},
        'SMITHERS-BI 345KV BI_SMR98_A': {'CATARINA_B1': -0.03961, 'HOLCOMB_RN1': -0.04352, 'FTDUNCAN_RN': -0.326, 'MV_VALV4_RN': -0.049},
        'SNDHT-WLFSW 138KV 6345__L': {'JDKNS_RN': -0.44871, 'SADLBACK_RN': 0.07},
        'SNDSW-AUSTRO 345KV 450__A': {'RUSSEKST_RN': -0.06348, 'CATARINA_B1': 0.14059, 'HOLCOMB_RN1': 0.15042, 'HAMI_BESS_RN': 0.088, 'FTDUNCAN_RN': 0.11442, 'JDKNS_RN': -0.07518, 'SADLBACK_RN': -0.07016, 'MV_VALV4_RN': 0.14762, 'CISC_RN': -0.09626, 'DIBOL_RN': -0.09504, 'OLNEYTN_RN': -0.099},
        'SONR-ATSO 69KV ATSO_SONR1_1': {'RUSSEKST_RN': 0.05, 'HAMI_BESS_RN': 0.014},
        'STP-ELMCREEK 345KV STPELM27_1': {'RUSSEKST_RN': 0.042, 'CATARINA_B1': 0.08, 'HOLCOMB_RN1': 0.079, 'HAMI_BESS_RN': 0.067, 'FTDUNCAN_RN': 0.075, 'JUNCTION_RN': 0.058, 'JDKNS_RN': 0.037, 'SADLBACK_RN': 0.039, 'MV_VALV4_RN': 0.052, 'MAINLAND_RN': -0.09, 'CISC_RN': 0.023, 'OLNEYTN_RN': 0.02},
        'STP-WAP 345KV STPWAP39_1': {'CATARINA_B1': -0.085, 'HOLCOMB_RN1': -0.09462, 'HAMI_BESS_RN': -0.05, 'FTDUNCAN_RN': -0.07, 'MV_VALV4_RN': -0.11296, 'MAINLAND_RN': 0.0957},
        'TMPSW-TMPCR 345KV 315__A': {'CATARINA_B1': -0.0932, 'HOLCOMB_RN1': -0.0997, 'HAMI_BESS_RN': -0.05, 'FTDUNCAN_RN': -0.0737, 'JUNCTION_RN': -0.061, 'MV_VALV4_RN': -0.0991, 'MAINLAND_RN': -0.045, 'CISC_RN': 0.071, 'DIBOL_RN': 0.082, 'OLNEYTN_RN': 0.081},
        'TNFXTAIL-FLAT_TOP 138KV 138_FLT_FXT_1': {'JDKNS_RN': 0.016, 'SADLBACK_RN': 0.31332},
        'TNP_ONE-TOKSW 345KV 262_A_1': {'CATARINA_B1': -0.06355, 'HOLCOMB_RN1': -0.06213, 'HAMI_BESS_RN': -0.05766, 'FTDUNCAN_RN': -0.0614, 'JUNCTION_RN': -0.07546, 'MV_VALV4_RN': -0.0542, 'MAINLAND_RN': 0.06594, 'DIBOL_RN': 0.06361},
        'TREADWEL-YELWJCKT 138KV TREADW_YELWJC1_1': {'CATARINA_B1': 0.015, 'HOLCOMB_RN1': 0.015, 'JUNCTION_RN': 0.46755, 'MV_VALV4_RN': 0.015},
        'TWINBU-HARGROVE 138KV HARGRO_TWINBU1_1': {'RUSSEKST_RN': 0.31665, 'HAMI_BESS_RN': 0.0679, 'FTDUNCAN_RN': 0.04386, 'JDKNS_RN': 0.01781, 'SADLBACK_RN': 0.02485},
        'UVALDE-READING 138KV READIN_UVALDE1_1': {'CATARINA_B1': -0.08093, 'HAMI_BESS_RN': -0.20464, 'FTDUNCAN_RN': -0.08935},
        'VALEXP': {'MV_VALV4_RN': -0.98239},
        'VEALMOOR-KOCHTAP 138KV 15060__B': {'RUSSEKST_RN': 0.02331, 'HAMI_BESS_RN': 0.01012, 'JDKNS_RN': 0.0524, 'SADLBACK_RN': 0.0447},
        'WCRYSTS-CARIZOS 69KV WCR_CARI_1': {'CATARINA_B1': -0.05, 'HAMI_BESS_RN': 0.126, 'FTDUNCAN_RN': 0.19},
        'WESTEX': {'RUSSEKST_RN': -0.66, 'CATARINA_B1': 0.07514, 'HOLCOMB_RN1': 0.13067, 'HAMI_BESS_RN': -0.23, 'FTDUNCAN_RN': -0.07, 'JUNCTION_RN': -0.3, 'JDKNS_RN': -0.718, 'SADLBACK_RN': -0.716, 'MV_VALV4_RN': 0.15332, 'MAINLAND_RN': 0.17937, 'CISC_RN': -0.513, 'DIBOL_RN': 0.19396, 'OLNEYTN_RN': -0.558},
        'WOLFCAMP-SANTARIT 138KV SANTAR_WOLFCA1_1': {'RUSSEKST_RN': 0.31739},
        'W_BATESV-UVALDE 138KV UVALDE_W_BATE1_1': {'CATARINA_B1': -0.14975, 'HOLCOMB_RN1': -0.06, 'HAMI_BESS_RN': 0.27526, 'FTDUNCAN_RN': 0.29993},
        'YELWJCKT-FORTMA 138KV FORTMA_YELWJC1_1': {'JUNCTION_RN': -0.5},
        'YELWJCKT-HEXT 69KV HEXT_YELWJC1_1': {'JUNCTION_RN': 0.059},
        'ZEN-THW 345KV THWZEN71_A': {'RUSSEKST_RN': -0.072, 'JUNCTION_RN': -0.053, 'JDKNS_RN': -0.077, 'SADLBACK_RN': -0.075, 'MAINLAND_RN': 0.12782, 'CISC_RN': -0.84, 'DIBOL_RN': -0.1, 'OLNEYTN_RN': -0.08},
        'ZEN-THW 345KV THWZEN98_A': {'RUSSEKST_RN': -0.072, 'JUNCTION_RN': -0.046, 'JDKNS_RN': -0.077, 'SADLBACK_RN': -0.075, 'MAINLAND_RN': 0.11115, 'CISC_RN': -0.08559, 'DIBOL_RN': -0.107, 'OLNEYTN_RN': -0.0898},
    }

    # Station token -> {node: sf} for fuzzy matching when exact name not found
    STATION_SF = {
        'HARGROVE': {'RUSSEKST_RN': 0.31665, 'HAMI_BESS_RN': 0.0679, 'FTDUNCAN_RN': 0.04386, 'JDKNS_RN': 0.01781, 'SADLBACK_RN': 0.02485},
        'HARGRO': {'RUSSEKST_RN': 0.31665, 'HAMI_BESS_RN': 0.0679, 'FTDUNCAN_RN': 0.04386, 'JDKNS_RN': 0.01781, 'SADLBACK_RN': 0.02485},
        'TWINBU': {'RUSSEKST_RN': 0.31665, 'HAMI_BESS_RN': 0.0679, 'FTDUNCAN_RN': 0.04386, 'JDKNS_RN': 0.01781, 'SADLBACK_RN': 0.02485},
        'LAKENA': {'RUSSEKST_RN': 0.07252},
        'LAKENASW': {'RUSSEKST_RN': 0.07252},
        'SAMATH1': {'RUSSEKST_RN': 0.07252},
        'SAMATHIS': {'RUSSEKST_RN': 0.07252},
        'GREENL': {'JDKNS_RN': 0.04, 'SADLBACK_RN': 0.04},
        'WEAVER': {'JDKNS_RN': 0.04, 'SADLBACK_RN': 0.04},
        'FRONTERA': {'CATARINA_B1': 0.01044, 'FTDUNCAN_RN': 0.01044},
        'LARDVN': {'RUSSEKST_RN': 0.01, 'CATARINA_B1': 0.21816, 'HOLCOMB_RN1': 0.32386, 'HAMI_BESS_RN': 0.07, 'MV_VALV4_RN': -0.079, 'FTDUNCAN_RN': 0.07},
        'LARDVNTH': {'RUSSEKST_RN': 0.01, 'CATARINA_B1': 0.21816, 'HOLCOMB_RN1': 0.32386, 'HAMI_BESS_RN': 0.07, 'MV_VALV4_RN': -0.079, 'FTDUNCAN_RN': 0.07},
        'LASCRU': {'CATARINA_B1': 0.17261, 'HOLCOMB_RN1': 0.28025, 'HAMI_BESS_RN': 0.07, 'FTDUNCAN_RN': 0.09648, 'MV_VALV4_RN': -0.08},
        'LASCRUCE': {'RUSSEKST_RN': 0.01, 'CATARINA_B1': 0.17885, 'HOLCOMB_RN1': 0.29736, 'HAMI_BESS_RN': 0.07, 'MV_VALV4_RN': -0.08, 'FTDUNCAN_RN': 0.09648},
        'CATARINA': {'CATARINA_B1': 0.76982, 'HOLCOMB_RN1': 0.17245, 'HAMI_BESS_RN': -0.09, 'FTDUNCAN_RN': -0.116},
        'CATARI': {'CATARINA_B1': 0.337, 'HOLCOMB_RN1': -0.098},
        'ASHERTON': {'CATARINA_B1': 0.76982, 'HOLCOMB_RN1': 0.17245, 'HAMI_BESS_RN': -0.09, 'FTDUNCAN_RN': -0.116},
        'MAVERICK': {'CATARINA_B1': -0.09232, 'HAMI_BESS_RN': -0.56, 'FTDUNCAN_RN': -0.25822, 'HOLCOMB_RN1': -0.04424, 'RUSSEKST_RN': 0.05256},
        'HAMILTON': {'CATARINA_B1': -0.08936, 'HAMI_BESS_RN': -0.56, 'FTDUNCAN_RN': -0.25822, 'RUSSEKST_RN': 0.05256},
        'YELWJCKT': {'RUSSEKST_RN': 0.032, 'JUNCTION_RN': 0.5, 'JDKNS_RN': 0.02, 'SADLBACK_RN': 0.02},
        'YELWJC1': {'RUSSEKST_RN': 0.032, 'JUNCTION_RN': 0.5, 'JDKNS_RN': 0.02, 'SADLBACK_RN': 0.02},
        'FORTMA': {'RUSSEKST_RN': 0.032, 'JUNCTION_RN': 0.5, 'JDKNS_RN': 0.02, 'SADLBACK_RN': 0.02},
        'TREADWEL': {'CATARINA_B1': 0.015, 'HOLCOMB_RN1': 0.015, 'JUNCTION_RN': 0.46755, 'MV_VALV4_RN': 0.015},
        'TREADW': {'CATARINA_B1': 0.015, 'HOLCOMB_RN1': 0.015, 'JUNCTION_RN': 0.46755, 'MV_VALV4_RN': 0.015},
        'MADDUX': {'JUNCTION_RN': 0.39252, 'RUSSEKST_RN': -0.0695, 'HAMI_BESS_RN': -0.11666, 'FTDUNCAN_RN': -0.07369},
        'WESTEX': {'RUSSEKST_RN': -0.66, 'CATARINA_B1': 0.07514, 'HOLCOMB_RN1': 0.13067, 'HAMI_BESS_RN': -0.23, 'FTDUNCAN_RN': -0.07, 'JUNCTION_RN': -0.3, 'JDKNS_RN': -0.718, 'SADLBACK_RN': -0.716, 'MV_VALV4_RN': 0.15332, 'MAINLAND_RN': 0.17937, 'CISC_RN': -0.513, 'DIBOL_RN': 0.19396, 'OLNEYTN_RN': -0.558},
        'MGSES': {'RUSSEKST_RN': 0.1, 'JDKNS_RN': 0.3, 'SADLBACK_RN': 0.27037, 'MAINLAND_RN': -0.05, 'CISC_RN': -0.08, 'DIBOL_RN': -0.06, 'OLNEYTN_RN': -0.07},
        'CATSW': {'RUSSEKST_RN': 0.1, 'JDKNS_RN': 0.3, 'SADLBACK_RN': 0.27037, 'MAINLAND_RN': -0.05, 'CISC_RN': -0.08, 'DIBOL_RN': -0.06, 'OLNEYTN_RN': -0.07},
        'OLNEY': {'OLNEYTN_RN': 0.78682},
        'CRDSW': {'OLNEYTN_RN': 0.78682},
        'PALOUSE': {'RUSSEKST_RN': 0.30569},
        'PALOUS': {'RUSSEKST_RN': 0.30569},
        'WOLFCAMP': {'RUSSEKST_RN': 0.31739},
        'WOLFCA1': {'RUSSEKST_RN': 0.31739},
        'LNGSW': {'RUSSEKST_RN': 0.08513, 'JDKNS_RN': 0.21466, 'SADLBACK_RN': 0.20517},
        'PRLSW': {'RUSSEKST_RN': 0.11076, 'JDKNS_RN': 0.29043, 'SADLBACK_RN': 0.2337},
        'SNDHT': {'JDKNS_RN': -0.44871, 'SADLBACK_RN': 0.07},
        'WLFSW': {'JDKNS_RN': -0.44871, 'SADLBACK_RN': 0.07},
        'NORTMC': {'RUSSEKST_RN': 0.34171, 'HAMI_BESS_RN': 0.09187, 'FTDUNCAN_RN': 0.057, 'JDKNS_RN': -0.028, 'SADLBACK_RN': -0.0235},
        'CROSSOVE': {'RUSSEKST_RN': 0.34171, 'HAMI_BESS_RN': 0.09187, 'FTDUNCAN_RN': 0.057, 'JDKNS_RN': -0.028, 'SADLBACK_RN': -0.0235},
        'ESCONDID': {'RUSSEKST_RN': 0.05, 'CATARINA_B1': 0.10133, 'HOLCOMB_RN1': -0.044, 'HAMI_BESS_RN': 0.4035, 'FTDUNCAN_RN': -0.2614},
        'GANSO': {'RUSSEKST_RN': 0.05, 'CATARINA_B1': -0.094, 'HAMI_BESS_RN': 0.4035, 'FTDUNCAN_RN': -0.2614},
        'LOBO': {'CATARINA_B1': 0.21816, 'HOLCOMB_RN1': 0.32386, 'HAMI_BESS_RN': 0.07, 'FTDUNCAN_RN': 0.07, 'MV_VALV4_RN': -0.07},
    }

    import re as _re

    def _node_exposure(c_name, from_st, to_st, nodes):
        """Look up real shift factors from spreadsheet data.
        Returns {node: shift_factor} for nodes with material exposure."""
        # 1. Try exact match on full constraint name
        if c_name in CONSTRAINT_SF:
            return {n: sf for n, sf in CONSTRAINT_SF[c_name].items() if n in nodes}

        # 2. Try fuzzy match on station tokens from constraint name + from/to stations
        combined = c_name + " " + from_st + " " + to_st
        tokens   = _re.split(r"[\s_\-]+", combined.upper())
        merged   = {}
        for token in tokens:
            token = token.strip()
            if len(token) <= 3:
                continue
            if token in STATION_SF:
                for node, sf in STATION_SF[token].items():
                    if node in nodes:
                        # Keep highest abs value if token conflicts
                        if node not in merged or abs(sf) > abs(merged[node]):
                            merged[node] = sf
        return merged

    # ── Build hourly summary per constraint ───────────────────────────────────
    # hourly_summary[constraint_name][he] = {avg_shadow, max_shadow, intervals}
    constraints = []
    for c_name in ranked_names:
        cd     = constraint_data[c_name]
        prices = cd["shadow_prices"]
        if not prices:
            continue

        avg_shadow  = round(sum(prices) / len(prices), 2)
        peak_shadow = round(max(prices), 2)
        hours_bind  = len(cd["by_hour"])
        avg_viol    = round(sum(cd["violated_mw"]) / len(cd["violated_mw"]), 1) if cd["violated_mw"] else 0

        # Per-hour summary: HE01–HE24
        hourly = {}
        for he in range(1, 25):
            he_prices = cd["by_hour"].get(he, [])
            if he_prices:
                hourly[he] = {
                    "avg_shadow":  round(sum(he_prices) / len(he_prices), 2),
                    "max_shadow":  round(max(he_prices), 2),
                    "intervals":   len(he_prices),
                }

        # Flow direction
        from_st = cd["from_station"]
        to_st   = cd["to_station"]
        c_upper = c_name.upper()
        if any(kw in c_upper for kw in ["SOUTH","S_","_S_","WST","WEST"]):
            direction = "S→N"
        elif any(kw in c_upper for kw in ["NORTH","N_","_N_"]):
            direction = "N→S"
        elif any(kw in c_upper for kw in ["WEST","PAN","W_"]):
            direction = "W→E"
        elif any(kw in c_upper for kw in ["EAST","E_"]):
            direction = "E→W"
        else:
            direction = "—"

        # Node exposure
        node_exposure = _node_exposure(c_name, from_st, to_st, asset_nodes)

        constraints.append({
            "name":           c_name,
            "contingency":    cd["contingency"],
            "from_station":   from_st,
            "to_station":     to_st,
            "from_kv":        cd["from_kv"],
            "to_kv":          cd["to_kv"],
            "avg_shadow":     avg_shadow,
            "peak_shadow":    peak_shadow,
            "hours_binding":  hours_bind,
            "avg_violated_mw": avg_viol,
            "flow_direction": direction,
            "hourly":         hourly,        # keyed by int HE
            "node_exposure":  node_exposure, # {node: score}
            "shift_factors":  {},            # placeholder for future PTDF API
        })

    return {"constraints": constraints, "data_date": YESTERDAY, "source": "ERCOT"}


# ══════════════════════════════════════════════════════════════════════════════
# 2.  AG2 — 15-day city temperature & precip forecast for major ERCOT metros
# ══════════════════════════════════════════════════════════════════════════════

AG2_BASE = "https://www.wsitrader.com/Services/CSVDownloadService.svc"

AG2_ERCOT_CITIES = {
    "Abilene, TX", "Austin, TX", "Corpus Christi, TX", "Dallas Fort Worth, TX",
    "Galveston, TX", "Houston Iah, TX", "Lubbock, TX", "Midland, TX",
    "San Antonio, TX", "Waco, TX", "Wichita Falls, TX", "Brownsville, TX",
    "Laredo Afb, TX", "Victoria, TX",
}


def _ag2_auth_params():
    return {
        "Account":  os.environ.get("AG2_ACCOUNT", ""),
        "Profile":  os.environ.get("AG2_PROFILE", ""),
        "Password": os.environ.get("AG2_PASSWORD", ""),
    }


def _ag2_csv_get(endpoint, extra_params, timeout=25):
    url    = f"{AG2_BASE}/{endpoint}"
    params = {**_ag2_auth_params(), **extra_params}
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"  WARN [AG2 Trader] {endpoint} — {e}")
        return ""


def _parse_ag2_csv(csv_text):
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
    acct = os.environ.get("AG2_ACCOUNT", "")
    if not acct:
        print("  SKIP [AG2 Trader] AG2_ACCOUNT not set")
        return {"weather": {}}

    print("  Pulling AG2 Trader 15-day city forecasts for ERCOT metros...")

    minmax_csv  = _ag2_csv_get("GetCityTableForecast", {
        "IsCustom": "false", "CurrentTabName": "MinMax", "TempUnits": "F",
        "Id": "allcities", "Region": "NA",
    })
    minmax_rows = _parse_ag2_csv(minmax_csv)

    pop_csv  = _ag2_csv_get("GetCityTableForecast", {
        "IsCustom": "false", "CurrentTabName": "POP", "TempUnits": "F",
        "Id": "allcities", "Region": "NA",
    })
    pop_rows = _parse_ag2_csv(pop_csv)

    cities_out = {}
    city_days  = {}
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
        hi = int(safe_float(row.get("MaxTemp") or row.get("Max") or row.get("High") or 0))
        lo = int(safe_float(row.get("MinTemp") or row.get("Min") or row.get("Low") or 0))
        if dt:
            city_days.setdefault(canonical, {})[dt] = {"high": hi, "low": lo}

    pop_by_city = {}
    for row in pop_rows:
        city = str(row.get("City") or row.get("Station") or row.get("Location") or "").strip()
        city_key = city.lower()
        if city_key not in ag2_lower:
            continue
        canonical = ag2_lower[city_key]
        dt      = str(row.get("Date") or row.get("date") or "")[:10]
        pop_val = int(safe_float(row.get("POP") or row.get("Precip") or row.get("PoP") or 0))
        if dt:
            pop_by_city.setdefault(canonical, {})[dt] = pop_val

    for city_name, days_data in city_days.items():
        pop_for_city = pop_by_city.get(city_name, {})
        days_list    = []
        for dt in sorted(days_data.keys())[:15]:
            d = days_data[dt]
            days_list.append({
                "date": dt, "high": d["high"], "low": d["low"],
                "precip_pct": pop_for_city.get(dt, 0),
            })
        if days_list:
            cities_out[city_name] = {"days": days_list}

    n_days = len(next(iter(cities_out.values()))["days"]) if cities_out else 0
    print(f"    AG2: {len(cities_out)} cities · {n_days} days each")
    if not cities_out and minmax_rows:
        sample = minmax_rows[:3]
        print(f"    DEBUG — sample CSV columns: {list(sample[0].keys())}")
        print(f"    DEBUG — sample City values: {[r.get('City') or r.get('Station') or '?' for r in sample]}")

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

HEN_CUSTOM_INDICES = {
    "1hr_without_hen":         {"name": "2026 - 1Hr Without HEN",      "id": 4752, "start": MODO_WINDOW_START_1HR},
    "hen_2026":                {"name": "HEN 2026",                    "id": 4872, "start": MODO_WINDOW_START_1HR},
    "2hr_without_fort_duncan": {"name": "2-hour Without Fort Duncan",  "id": 4891, "start": MODO_WINDOW_START_2HR},
    "fort_duncan":             {"name": "Fort Duncan",                 "id": 5006, "start": MODO_WINDOW_START_2HR},
}


def _modo_headers():
    return {"X-Token": os.environ.get("MODO_API_KEY", ""), "Accept": "application/json"}


def _modo_get(path, params=None, timeout=25):
    try:
        r = requests.get(f"{MODO_BASE}/{path}", headers=_modo_headers(),
                         params=params or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  WARN [Modo] {path} — {e}")
        return {}


def _modo_paginate(path, params=None):
    all_results = []
    params = dict(params or {})
    params.setdefault("limit", 10000)
    cursor = None
    while True:
        if cursor:
            params["cursor"] = cursor
        body    = _modo_get(path, params=params)
        results = body.get("results") or body.get("data") or []
        all_results.extend(results)
        next_url = body.get("next")
        if not next_url:
            break
        import urllib.parse as urlparse
        qs     = urlparse.parse_qs(urlparse.urlparse(next_url).query)
        cursor = qs.get("cursor", [None])[0]
        if not cursor:
            break
    return all_results


def _modo_resolve_index_ids():
    hardcoded = {k: meta["id"] for k, meta in HEN_CUSTOM_INDICES.items() if "id" in meta}
    if hardcoded:
        print(f"    Modo IDs from index config: {hardcoded}")
        return hardcoded

    id_override = os.environ.get("MODO_INDEX_IDS", "").strip()
    if id_override:
        resolved = {}
        for pair in id_override.split(","):
            pair = pair.strip()
            if ":" not in pair:
                continue
            name, id_str = pair.rsplit(":", 1)
            name = name.strip()
            for key, meta in HEN_CUSTOM_INDICES.items():
                if meta["name"].lower() == name.lower():
                    try:
                        resolved[key] = int(id_str.strip())
                    except ValueError:
                        pass
        if resolved:
            print(f"    Modo IDs from MODO_INDEX_IDS secret: {resolved}")
            return resolved

    print("    Discovering Modo custom index IDs via /pub/v1/indices/...")
    all_indices   = _modo_paginate("indices/", params={"limit": 500})
    resolved      = {}
    display_to_key = {meta["name"].lower(): k for k, meta in HEN_CUSTOM_INDICES.items()}

    for idx in all_indices:
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
    return resolved


def _modo_index_window_revenue(index_id, start_date, end_date):
    def _fetch(extra_params=None):
        params = {
            "interval_start": f"{start_date}T00:00:00",
            "interval_end":   f"{end_date}T23:59:59",
            "granularity":            "daily",
            "capacity_normalisation": "mw",
            "time_basis":             "year",
            "limit":                  10000,
        }
        if extra_params:
            params.update(extra_params)
        body        = _modo_get(f"indices/{index_id}/revenue/timeseries/", params=params)
        results_obj = body.get("results") or {}
        if isinstance(results_obj, dict):
            records = results_obj.get("records") or []
            units   = results_obj.get("units", "")
        else:
            records = results_obj if isinstance(results_obj, list) else []
            units   = ""
        return records, units

    records, units = _fetch({"breakdown": "market"})
    if not records:
        records, units = _fetch()
    if not records:
        return {}

    total            = 0.0
    market_breakdown = {}
    seen_dates       = set()
    for row in records:
        if not isinstance(row, dict):
            continue
        rev    = safe_float(row.get("revenue") or 0)
        market = str(row.get("market") or "total")
        dt     = str(row.get("interval_start") or "")[:10]
        market_breakdown[market] = round(market_breakdown.get(market, 0.0) + rev, 2)
        total += rev
        if dt:
            seen_dates.add(dt)

    n_days = len(seen_dates) or len(records)
    if market_breakdown and len(market_breakdown) > 1:
        market_avgs = {k: round(v / n_days, 2) for k, v in market_breakdown.items()}
        avg = round(sum(market_avgs.values()), 2)
    else:
        avg         = round(total / n_days, 2) if n_days else 0.0
        market_avgs = market_breakdown

    print(f"      → {n_days} days · {len(records)} records · units: {units} · total: {avg:.2f}")
    return {"revenue_mw_year": avg, "n_days": n_days, "market_breakdown": market_avgs}


def collect_modo_indices():
    api_key = os.environ.get("MODO_API_KEY", "")
    if not api_key:
        print("  SKIP [Modo] MODO_API_KEY not set")
        return {"modo": {}}

    print(f"  Pulling Modo Energy custom indices (end: {MODO_DATE})...")
    index_ids = _modo_resolve_index_ids()
    if not index_ids:
        print("  WARN [Modo] No index IDs resolved — skipping revenue pull")
        return {"modo": {"data_date": MODO_DATE, "source": "Modo Energy",
                         "error": "No index IDs resolved"}}

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
    probe_paths = ["", "/api/assets", "/api/v1/assets", "/assets",
                   "/api/outages", "/api/v1/outages", "/data/assets"]
    for path in probe_paths:
        probe_url = url.rstrip("/") + path
        try:
            r  = requests.get(probe_url, headers=headers, timeout=10)
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
    rows = (
        body.get("assets") or body.get("data") or body.get("resources") or
        body.get("units") or (body if isinstance(body, list) else [])
    )
    assets_out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = (
            row.get("name") or row.get("assetName") or row.get("asset_name") or
            row.get("assetId") or row.get("asset_id") or row.get("id") or ""
        )
        capacity_mw  = safe_float(row.get("capacity_mw") or row.get("capacityMW") or
                                   row.get("nameplateCapacity") or row.get("nameplate_mw") or 0)
        available_mw = safe_float(row.get("available_mw") or row.get("availableMW") or
                                   row.get("availableCapacity") or row.get("economicMax") or capacity_mw)
        status       = str(row.get("status") or row.get("operatingStatus") or
                           row.get("state") or "unknown").lower()
        outage_type  = str(
            row.get("outage_type") or row.get("outageType") or
            ("planned" if "planned" in status else
             "forced"  if any(w in status for w in ("forced", "unplanned", "trip")) else "none")
        ).lower()
        outage_start  = str(row.get("outage_start") or row.get("outageStart") or row.get("startDate") or "")
        outage_end    = str(row.get("outage_end")   or row.get("outageEnd")   or row.get("endDate")   or "")
        outage_mw     = safe_float(row.get("outage_mw") or row.get("outageMW") or row.get("derated_mw") or 0)
        outage_reason = str(row.get("outage_reason") or row.get("outageReason") or row.get("reason") or "")
        avail_pct     = round((available_mw / capacity_mw * 100), 1) if capacity_mw else 0.0
        assets_out.append({
            "name": str(name), "capacity_mw": round(capacity_mw, 1),
            "available_mw": round(available_mw, 1), "availability_pct": avail_pct,
            "status": status, "outage_type": outage_type, "outage_mw": round(outage_mw, 1),
            "outage_start": outage_start, "outage_end": outage_end,
            "outage_reason": outage_reason, "region": _node_region(str(name)),
        })
    return assets_out


def _parse_powertools_outages(body):
    rows = (
        body.get("outages") or body.get("outage_schedule") or body.get("maintenance") or
        body.get("data") or (body if isinstance(body, list) else [])
    )
    outages_out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        outages_out.append({
            "asset":  str(row.get("asset") or row.get("assetName") or row.get("unit") or ""),
            "type":   str(row.get("type")  or row.get("outageType") or "unknown").lower(),
            "start":  str(row.get("start") or row.get("outageStart") or row.get("startDate") or ""),
            "end":    str(row.get("end")   or row.get("outageEnd")   or row.get("endDate")   or ""),
            "mw":     round(safe_float(row.get("mw") or row.get("outageMW") or 0), 1),
            "reason": str(row.get("reason") or row.get("outageReason") or row.get("description") or ""),
        })
    return outages_out


_REGION_MAP = {
    **{n: "West Texas"  for n in ["TOYAH_RN","SADLBACK_RN","FAULKNER_RN","COYOTSPR_RN",
                                   "LONESTAR_RN","RTLSNAKE_BT","CEDRVALE_RN","SBEAN_BESS",
                                   "GOMZ_RN","GRDNE_ESR_RN","JDKNS_RN","SANDLAKE_RN"]},
    **{n: "North Texas" for n in ["OLNEYTN_RN","DIBOL_RN","FRMRSVLW_RN","MNWL_BESS_RN",
                                   "LFSTH_RN","PAULN_RN","CISC_RN"]},
    **{n: "Coastal"     for n in ["MV_VALV4_RN","WLTC_ESR_RN","MAINLAND_RN","FALFUR_RN",
                                   "PAVLOV_BT_RN","POTEETS_RN","TYNAN_RN"]},
    **{n: "Premium"     for n in ["CATARINA_B1","HOLCOMB_RN1","HAMI_BESS_RN","JUNCTION_RN",
                                   "RUSSEKST_RN","FTDUNCAN_RN"]},
}

def _node_region(name):
    return _REGION_MAP.get(name, "Other")


def collect_powertools_assets():
    base_url = os.environ.get("POWERTOOLS_URL", "").rstrip("/")
    api_key  = os.environ.get("POWERTOOLS_API_KEY", "")
    username = os.environ.get("POWERTOOLS_USERNAME", "")
    password = os.environ.get("POWERTOOLS_PASSWORD", "")

    if not base_url:
        print("  SKIP [PowerTools] POWERTOOLS_URL not configured")
        return {"asset_status": {}}

    print(f"  Probing PowerTools at {base_url}...")

    headers = {"Accept": "application/json"}
    auth    = None
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    elif username and password:
        auth = (username, password)

    mode, detected_url = _powertools_probe(base_url, headers)
    print(f"    Detection result: {mode} → {detected_url}")

    if any(x in base_url for x in ["powerapps.com", "gateway.prod", ".island", "powerautomate"]):
        print("  SKIP [PowerTools] URL appears to be a Microsoft Power Apps dashboard.")
        return {"asset_status": {"error": "Power Apps URL — needs HTTP trigger endpoint from IT"}}

    if mode == "html_dashboard":
        print("  WARN [PowerTools] URL returns an HTML login page. Set credentials.")
        return {"asset_status": {"detection_mode": mode, "source": "PowerTools",
                                  "error": "HTML dashboard — API credentials needed"}}

    if mode == "auth_required":
        print("  WARN [PowerTools] Server responded with 401/403. Set credentials.")
        return {"asset_status": {"detection_mode": mode, "source": "PowerTools",
                                  "error": "Authentication required"}}

    if mode == "unreachable":
        print(f"  WARN [PowerTools] Could not reach {base_url}.")
        return {"asset_status": {"detection_mode": mode, "source": "PowerTools",
                                  "error": "URL unreachable"}}

    asset_endpoint = detected_url
    assets         = []

    for ap in [detected_url, base_url+"/api/assets", base_url+"/api/v1/assets", base_url+"/assets"]:
        try:
            r = requests.get(ap, headers=headers, auth=auth, timeout=15)
            if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                parsed = _parse_powertools_assets(r.json(), [])
                if parsed:
                    assets = parsed; asset_endpoint = ap
                    print(f"    Assets: {len(assets)} records from {ap}")
                    break
        except Exception as e:
            print(f"    WARN: asset path {ap} — {e}")

    outages          = []
    embedded_outages = [a for a in assets if a.get("outage_type") not in ("none", "unknown", "")]
    if embedded_outages:
        outages = [{"asset": a["name"], "type": a["outage_type"], "start": a["outage_start"],
                    "end": a["outage_end"], "mw": a["outage_mw"], "reason": a["outage_reason"]}
                   for a in embedded_outages]
        print(f"    Outages: {len(outages)} embedded in asset records")
    else:
        for op in [base_url+"/api/outages", base_url+"/api/v1/outages",
                   base_url+"/outages", base_url+"/api/maintenance"]:
            try:
                r = requests.get(op, headers=headers, auth=auth,
                                 params={"date_from": TODAY_STR, "date_to": IN_15_DAYS},
                                 timeout=15)
                if r.status_code == 200 and "application/json" in r.headers.get("Content-Type", ""):
                    parsed = _parse_powertools_outages(r.json())
                    if parsed:
                        outages = parsed
                        print(f"    Outages: {len(outages)} records from {op}")
                        break
            except Exception as e:
                print(f"    WARN: outage path {op} — {e}")

    total_cap       = sum(a["capacity_mw"]  for a in assets)
    total_avail     = sum(a["available_mw"] for a in assets)
    on_outage       = [a for a in assets if a["outage_type"] not in ("none", "unknown", "")]
    planned_mw      = sum(a["outage_mw"] for a in assets if a["outage_type"] == "planned")
    forced_mw       = sum(a["outage_mw"] for a in assets if a["outage_type"] in ("forced","unplanned"))
    fleet_avail_pct = round(total_avail / total_cap * 100, 1) if total_cap else 0.0

    from datetime import timezone
    ct_now = (datetime.now(timezone.utc) + timedelta(hours=-5)).strftime("%Y-%m-%d %H:%M CT")

    return {
        "asset_status": {
            "as_of": ct_now, "source": "PowerTools",
            "detection_mode": mode, "endpoint_used": asset_endpoint,
            "fleet_summary": {
                "total_assets": len(assets), "online": len(assets) - len(on_outage),
                "on_outage": len(on_outage), "total_capacity_mw": round(total_cap, 1),
                "available_mw": round(total_avail, 1), "fleet_availability_pct": fleet_avail_pct,
                "planned_outage_mw": round(planned_mw, 1), "forced_outage_mw": round(forced_mw, 1),
            },
            "assets": assets, "outage_schedule": outages,
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5.  ERCOT PUBLIC FORECASTS — Load, Wind, Solar (24hr hourly + 7-day daily)
# ══════════════════════════════════════════════════════════════════════════════

def collect_ercot_forecasts(token, sub_key):
    base    = "https://api.ercot.com/api/public-reports"
    headers = {"Authorization": f"Bearer {token}", "Ocp-Apim-Subscription-Key": sub_key}

    today         = date.today()
    day7_end      = (today + timedelta(days=7)).isoformat()
    yesterday_str = (today - timedelta(days=1)).isoformat()
    today_str     = today.isoformat()

    def _ercot_get(path, params):
        try:
            r    = requests.get(f"{base}/{path}", headers=headers, params=params, timeout=30)
            r.raise_for_status()
            body = r.json()
            if not hasattr(_ercot_get, "_debugged"):
                _ercot_get._debugged = True
                print(f"    DEBUG ERCOT response keys: {list(body.keys())[:8]}")
                print(f"    DEBUG data type: {type(body.get('data')).__name__}, "
                      f"sample: {str(body.get('data'))[:200]}")
            fields_raw = body.get("fields") or []
            raw        = body.get("data")   or []
            if isinstance(raw, dict):
                fields_raw = raw.get("fields") or fields_raw
                raw        = raw.get("rows") or raw.get("data") or []
            if not raw:
                return []
            fields = []
            for f in fields_raw:
                fields.append(str(f.get("name") or f.get("label") or "") if isinstance(f, dict) else str(f))
            if not hasattr(_ercot_get, "_fields_logged"):
                _ercot_get._fields_logged = True
                print(f"    DEBUG fields ({len(fields)}): {fields[:15]}")
            if raw and isinstance(raw[0], list):
                if fields:
                    raw = [dict(zip(fields, row)) for row in raw]
                else:
                    return []
            return raw
        except Exception as e:
            print(f"  WARN [ERCOT forecast] {path} — {e}")
            return []

    print("  Pulling ERCOT short-term load forecast...")
    load_rows  = _ercot_get("np3-565-cd/lf_by_model_weather_zone",
                             {"deliveryDateFrom": yesterday_str, "deliveryDateTo": day7_end, "size": 5000})
    print("  Pulling ERCOT wind power forecast...")
    wind_rows  = _ercot_get("np4-732-cd/wpp_hrly_avrg_actl_fcast",
                             {"deliveryDateFrom": yesterday_str, "deliveryDateTo": day7_end, "size": 5000})
    print("  Pulling ERCOT solar PV forecast...")
    solar_rows = _ercot_get("np4-745-cd/spp_hrly_actual_fcast_geo",
                             {"deliveryDateFrom": yesterday_str, "deliveryDateTo": day7_end, "size": 5000})

    load_by_dt = {}
    for row in load_rows:
        in_use = row.get("inUseFlag") or row.get("InUseFlag")
        if in_use is False:
            continue
        dt = str(row.get("deliveryDate") or row.get("DeliveryDate") or "")[:10]
        he = str(row.get("hourEnding")   or row.get("HourEnding")   or "0")
        try:
            hour = int(str(he).split(":")[0]) - 1
        except:
            hour = 0
        mw = safe_float(row.get("systemTotal") or row.get("SystemTotal") or
                        row.get("loadForecast") or row.get("mtlf") or row.get("total") or 0)
        if dt and mw > 0:
            load_by_dt[f"{dt} {hour:02d}"] = mw

    wind_by_dt = {}
    for row in wind_rows:
        dt = str(row.get("deliveryDate") or row.get("DeliveryDate") or "")[:10]
        he = str(row.get("hourEnding")   or row.get("HourEnding")   or "0")
        try:
            hour = int(str(he).split(":")[0]) - 1
        except:
            hour = 0
        mw = safe_float(
            (row.get("STWPFSystemWide") if row.get("STWPFSystemWide") is not None else
             row.get("genSystemWide")   if row.get("genSystemWide")   is not None else
             row.get("COPHSLSystemWide") if row.get("COPHSLSystemWide") is not None else 0)
        )
        key = f"{dt} {hour:02d}"
        if dt and mw > 0 and key not in wind_by_dt:
            wind_by_dt[key] = mw

    solar_by_dt = {}
    for row in solar_rows:
        dt = str(row.get("deliveryDate") or row.get("DeliveryDate") or "")[:10]
        he = str(row.get("hourEnding")   or row.get("HourEnding")   or "0")
        try:
            hour = int(str(he).split(":")[0]) - 1
        except:
            hour = 0
        mw = safe_float(row.get("STPPFSystemWide") or row.get("PVGRPPSystemWide") or
                        row.get("genSystemWide") or 0)
        key = f"{dt} {hour:02d}"
        if dt and mw > 0 and key not in solar_by_dt:
            solar_by_dt[key] = mw

    hourly_keys = sorted(set(
        list(load_by_dt.keys()) + list(wind_by_dt.keys()) + list(solar_by_dt.keys())
    ))[:216]

    h24_timestamps, h24_load, h24_wind, h24_solar, h24_net = [], [], [], [], []
    for key in hourly_keys:
        gl  = round(load_by_dt.get(key, 0)  / 1000, 2)
        wnd = round(wind_by_dt.get(key, 0)  / 1000, 2)
        sol = round(solar_by_dt.get(key, 0) / 1000, 2)
        h24_timestamps.append(key); h24_load.append(gl); h24_wind.append(wnd)
        h24_solar.append(sol);      h24_net.append(round(gl - wnd - sol, 2))

    all_dates = sorted(set(k[:10] for k in
        list(load_by_dt.keys()) + list(wind_by_dt.keys()) + list(solar_by_dt.keys())))[:9]

    d7_dates, d7_load_peak, d7_wind_avg, d7_solar_peak, d7_net_peak = [], [], [], [], []
    for day in all_dates:
        day_load  = [load_by_dt.get(f"{day} {h:02d}", 0)  / 1000 for h in range(24)]
        day_wind  = [wind_by_dt.get(f"{day} {h:02d}", 0)  / 1000 for h in range(24)]
        day_solar = [solar_by_dt.get(f"{day} {h:02d}", 0) / 1000 for h in range(24)]
        day_net   = [day_load[h] - day_wind[h] - day_solar[h] for h in range(24)]
        d7_dates.append(day)
        d7_load_peak.append(round(max(day_load), 2)  if any(day_load)  else 0)
        d7_wind_avg.append(round(sum(day_wind) / max(len([x for x in day_wind if x > 0]), 1), 2))
        d7_solar_peak.append(round(max(day_solar), 2) if any(day_solar) else 0)
        d7_net_peak.append(round(max(day_net), 2)    if any(day_net)   else 0)

    print(f"    Load: {len(load_by_dt)} intervals · Wind: {len(wind_by_dt)} · Solar: {len(solar_by_dt)}")
    print(f"    24hr series: {len(h24_timestamps)} hours · 7-day series: {len(d7_dates)} days")

    return {
        "ercot_forecasts": {
            "generated_at":  datetime.utcnow().isoformat() + "Z",
            "forecast_date": today_str,
            "hourly_24hr": {
                "timestamps": h24_timestamps, "gross_load": h24_load,
                "wind": h24_wind, "solar": h24_solar, "net_load": h24_net,
            },
            "daily_7day": {
                "dates": d7_dates, "gross_load_peak": d7_load_peak,
                "wind_avg": d7_wind_avg, "solar_peak": d7_solar_peak,
                "net_load_peak": d7_net_peak,
            },
            "source": "ERCOT Public API",
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# 6.  AS PRICES — DA vs RT Ancillary Service clearing prices  ← FIXED v3
#
#  DA endpoint confirmed working from logs:
#    np4-188-cd/dam_clear_price_for_cap
#    Long format: one row per (date, hourEnding, ancillaryType), price in MCPC
#
#  RT: all previously tried endpoints 404. Trying additional candidates.
#  Dashboard shows DA prices while RT endpoint is being located.
# ══════════════════════════════════════════════════════════════════════════════

def collect_as_prices(token, subscription_key, lookback_days=5):
    today     = date.today()
    start     = today - timedelta(days=lookback_days + 1)
    end       = today - timedelta(days=1)
    start_str = start.isoformat()
    end_str   = end.isoformat()

    base    = "https://api.ercot.com/api/public-reports"
    headers = {
        "Authorization":             f"Bearer {token}",
        "Ocp-Apim-Subscription-Key": subscription_key,
    }
    AS_TYPES = ["REGUP", "REGDN", "RRS", "NSPIN", "ECRS"]

    # Maps ancillaryType field values to canonical names.
    # Confirmed from logs: ERCOT returns "RRS", "NSPIN", "REGUP", "REGDN", "ECRS"
    AS_TYPE_MAP = {
        "REGUP": "REGUP", "REG-UP": "REGUP", "REGULATION_UP": "REGUP",
        "REGDN": "REGDN", "REG-DN": "REGDN", "REG-DOWN": "REGDN", "REGULATION_DOWN": "REGDN",
        "RRS":   "RRS",   "RESPONSIVE_RESERVE": "RRS",
        "NSPIN": "NSPIN", "NON-SPIN": "NSPIN", "NONSPIN": "NSPIN", "NON_SPIN": "NSPIN",
        "ECRS":  "ECRS",  "ERCRS": "ECRS",
    }

    def _normalize_field(name):
        if isinstance(name, dict):
            return str(name.get("name") or name.get("label") or name.get("column") or "")
        return str(name).strip()

    def _get(path, params, label=""):
        try:
            r = requests.get(f"{base}/{path}", headers=headers,
                             params={**params, "size": 5000}, timeout=30)
            r.raise_for_status()
            body       = r.json()
            raw        = body.get("data") or []
            fields_raw = body.get("fields") or []
            if isinstance(raw, dict):
                fields_raw = raw.get("fields") or fields_raw
                raw        = raw.get("rows") or raw.get("data") or []
            if not raw:
                print(f"    [{label}] 0 rows — keys: {list(body.keys())}")
                return []
            fields = [_normalize_field(f) for f in fields_raw]
            print(f"    [{label}] {len(raw)} rows · fields: {fields[:20]}")
            if raw and isinstance(raw[0], list):
                if not fields:
                    print(f"    [{label}] WARN: list-of-lists but no fields")
                    return []
                raw = [dict(zip(fields, row)) for row in raw]
            if raw:
                print(f"    [{label}] sample: { {k: raw[0][k] for k in list(raw[0].keys())[:8]} }")
            return raw
        except Exception as e:
            print(f"    WARN [{label}] {path} -- {e}")
            return []

    def _parse_date_he(row):
        # Try standard deliveryDate + hourEnding fields first (DA format)
        dt = str(
            row.get("deliveryDate") or row.get("DeliveryDate") or
            row.get("delivery_date") or row.get("date") or ""
        )[:10]
        he_raw = (
            row.get("hourEnding") or row.get("HourEnding") or
            row.get("hour_ending") or row.get("Hour") or
            row.get("hour") or row.get("settlementInterval") or ""
        )

        # SCED format: SCEDTimestamp = "YYYY-MM-DDTHH:MM:SS"
        # Extract date and derive hour-ending from the timestamp hour
        if not dt or not he_raw:
            ts = str(
                row.get("SCEDTimestamp") or row.get("sced_timestamp") or
                row.get("timestamp") or row.get("Timestamp") or ""
            )
            if "T" in ts:
                dt     = ts[:10]
                # SCED timestamps are interval START times; hour-ending = start hour + 1
                ts_hour = int(ts[11:13])
                he_raw  = str(ts_hour + 1)  # HE 1-24

        try:
            he_int = int(str(he_raw).split(":")[0])
        except (ValueError, TypeError):
            he_int = 0

        # Clamp to valid HE range 1-24
        if he_int < 1:
            he_int = 1
        if he_int > 24:
            he_int = 24

        return dt, he_int

    def _parse_long_format(rows):
        """Long format: one row per (date, he/interval, ancillaryType) with price in MCPC.
        Buckets multiple intervals per hour and averages — handles both hourly DA
        (1 row per slot) and 5-minute SCED RT (12 rows per slot) correctly."""
        buckets = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for row in rows:
            dt, he = _parse_date_he(row)
            if not dt or not (start_str <= dt <= end_str):
                continue
            raw_type = str(
                row.get("ancillaryType") or row.get("AncillaryType") or
                row.get("ASType") or row.get("asType") or row.get("type") or ""
            ).strip().upper()
            canonical = AS_TYPE_MAP.get(raw_type)
            if not canonical:
                continue
            price = row.get("MCPC") or row.get("mcpc") or row.get("price") or row.get("Price")
            if price is None:
                continue
            try:
                buckets[dt][he][canonical].append(round(float(price), 2))
            except (TypeError, ValueError):
                continue
        # Average all intervals within each hour
        return {
            dt: {
                he: {at: round(sum(vals) / len(vals), 2) for at, vals in types.items()}
                for he, types in hours.items()
            }
            for dt, hours in buckets.items()
        }

    def _parse_wide_format(rows):
        """Wide format: one row per (date, he), AS types as columns."""
        AS_WIDE = {
            "REGUP": ["REGUP", "regUp", "regup", "RegUp"],
            "REGDN": ["REGDN", "regDn", "regdn", "RegDn", "REGDOWN", "regDown"],
            "RRS":   ["RRS",   "rrs",   "Rrs"],
            "NSPIN": ["NSPIN", "nonSpin", "nonspin", "NonSpin", "NONSPIN"],
            "ECRS":  ["ECRS",  "ecrs",   "Ecrs"],
        }
        buckets = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for row in rows:
            dt, he = _parse_date_he(row)
            if not dt or not (start_str <= dt <= end_str):
                continue
            for at, aliases in AS_WIDE.items():
                for alias in aliases:
                    v = row.get(alias)
                    if v is not None:
                        try:
                            buckets[dt][he][at].append(round(float(v), 2))
                        except (TypeError, ValueError):
                            pass
                        break
        return {
            dt: {
                he: {at: round(sum(vals) / len(vals), 2) for at, vals in types.items()}
                for he, types in hours.items()
            }
            for dt, hours in buckets.items()
        }

    # ── Pull DA (confirmed working: long format, ancillaryType + MCPC) --------
    print(f"  Pulling AS DA clearing prices ({start_str} -> {end_str})...")
    da_rows = _get("np4-188-cd/dam_clear_price_for_cap",
                   {"deliveryDateFrom": start_str, "deliveryDateTo": end_str},
                   label="DA-AS")
    da = _parse_long_format(da_rows)

    # ── Pull RT: try every known ERCOT AS RT endpoint -------------------------

    # RT SCED clears every 5 minutes — use SCEDTimestamp params (not deliveryDate)
    # 5 AS types x ~288 intervals/day x 6 days = ~8,640 rows — paginate to get all
    print(f"  Pulling AS RT clearing prices (SCED 5-min, {start_str} -> {end_str})...")
    rt_rows = []
    page = 1
    while True:
        batch = _get(
            "np6-332-cd/rt_clear_price_cap_sced",
            {
                "SCEDTimestampFrom": start_str + "T00:00:00",
                "SCEDTimestampTo":   end_str   + "T23:59:59",
                "page": page,
            },
            label=f"RT-AS-p{page}"
        )
        if not batch:
            break
        rt_rows.extend(batch)
        print(f"    RT page {page}: {len(batch)} rows · running total: {len(rt_rows)}")
        if len(batch) < 5000:
            break  # last page
        page += 1
    if not rt_rows:
        print("  !! RT AS endpoint returned 0 rows -- will show DA-only data.")
    else:
        print(f"  RT total rows fetched: {len(rt_rows)}")

    # Parse RT: detect long vs wide format
    if rt_rows:
        sample = rt_rows[0] if rt_rows else {}
        if ("ancillaryType" in sample or "AncillaryType" in sample
                or "ASType" in sample or "asType" in sample):
            rt = _parse_long_format(rt_rows)
            print("    RT parsed as long format")
        else:
            rt = _parse_wide_format(rt_rows)
            print("    RT parsed as wide format")
    else:
        rt = {}

    # ── Diagnostics -----------------------------------------------------------
    da_dates = sorted(da.keys())
    rt_dates = sorted(rt.keys())
    print(f"    DA parsed: {len(da_dates)} dates, {sum(len(v) for v in da.values())} hour-slots")
    print(f"    RT parsed: {len(rt_dates)} dates, {sum(len(v) for v in rt.values())} hour-slots")
    if da_dates:
        sd = da_dates[-1]
        sh = next(iter(da[sd]), None)
        if sh is not None:
            print(f"    DA value check ({sd} HE{sh}): {da[sd][sh]}")

    # ── Build output series ---------------------------------------------------
    all_dates = sorted(set(list(da.keys()) + list(rt.keys())))
    result    = {at: [] for at in AS_TYPES}

    for d in all_dates:
        for he in range(1, 25):
            da_prices = da.get(d, {}).get(he, {})
            rt_prices = rt.get(d, {}).get(he, {})
            for at in AS_TYPES:
                dv = da_prices.get(at)
                rv = rt_prices.get(at)
                result[at].append({
                    "date":   d,
                    "he":     he,
                    "da":     dv,
                    "rt":     rv,
                    "spread": round(dv - rv, 2) if dv is not None and rv is not None else None,
                })

    for at in AS_TYPES:
        non_null = sum(1 for r in result[at] if r["spread"] is not None)
        da_only  = sum(1 for r in result[at] if r["da"] is not None and r["rt"] is None)
        print(f"    {at}: {non_null} spreads · {da_only} DA-only")

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "start_date":   start_str,
        "end_date":     end_str,
        "as_types":     AS_TYPES,
        "series":       result,
        "source":       "ERCOT Public API",
    }



def collect_all_integrations(token=None, sub_key=None, asset_nodes=None):
    """
    Calls all active collectors and returns a single merged dict ready
    to be merged into `data` inside collect_data() in hen_morning_report.py.

    Usage:
        extras = collect_all_integrations(token=token, sub_key=sub_key,
                                          asset_nodes=NODES)
        data.update(extras)
    """
    out = {}

    if token and sub_key:
        print("\n── Integration 1/6: ERCOT binding constraints ──")
        out.update(collect_ercot_constraints(token, sub_key, asset_nodes))
    else:
        print("  SKIP [ERCOT constraints] — no ERCOT token provided")
        out["constraints"] = []

    print("\n── Integration 2/6: AG2 15-day weather ──")
    out.update(collect_ag2_weather())

    print("\n── Integration 3/6: Modo Energy indices ──")
    out.update(collect_modo_indices())

    print("\n── Integration 4/6: PowerTools asset availability ──")
    out.update(collect_powertools_assets())

    if token and sub_key:
        print("\n── Integration 5/6: ERCOT load/wind/solar forecasts ──")
        out.update(collect_ercot_forecasts(token, sub_key))
    else:
        out["ercot_forecasts"] = {}

    print("\n── Integration 6/6: AS DA vs RT clearing prices ──")
    try:
        out["as_prices"] = collect_as_prices(token, sub_key, lookback_days=5)
    except Exception as e:
        print(f"  WARN [AS prices] {e}")
        out["as_prices"] = {"error": str(e)}

    return out


# ══════════════════════════════════════════════════════════════════════════════
# COMING SOON — preserved stubs (uncomment when access is provisioned)
# ══════════════════════════════════════════════════════════════════════════════

# ── Meteologica 7-day forecasts ───────────────────────────────────────────────
# Uncomment and add to collect_all_integrations() when ready.
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
# Uncomment and add to collect_all_integrations() when ready.
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
