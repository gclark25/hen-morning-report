"""
HEN — Daily ERCOT Morning Report
=================================
Pulls live ERCOT public API data, builds an HTML report,
and emails it via SendGrid.

Designed to run at 6:00 AM CT via GitHub Actions cron.
Builds directly on the validated ercot_public_test.py data pipeline.

REQUIRED ENVIRONMENT VARIABLES:
  ERCOT_USERNAME          apiexplorer.ercot.com email
  ERCOT_PASSWORD          apiexplorer.ercot.com password
  ERCOT_SUBSCRIPTION_KEY  API Explorer primary key
  ERCOT_NODES             comma-separated settlement point names (your 32 sites)
  SENDGRID_API_KEY        SendGrid API key (sendgrid.com → Settings → API Keys)
  FROM_EMAIL              verified sender address in SendGrid
  TO_EMAILS               comma-separated recipient list

OPTIONAL:
  S3_BUCKET               if set, archives report HTML + JSON to S3 each morning
"""

import os
import sys
import json
import time
import requests
from datetime import date, timedelta
from urllib.parse import quote

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

def extract_price(row):
    """Extract price from either a list row or dict row."""
    if isinstance(row, list):
        nums = [x for x in row if isinstance(x, (int, float))
                and not isinstance(x, bool) and x != 0]
        return nums[-1] if nums else None
    elif isinstance(row, dict):
        p = safe_float(row.get("settlementPointPrice") or
                       row.get("spp") or row.get("price") or 0)
        return p if p != 0 else None
    return None

# ── DATA COLLECTION ───────────────────────────────────────────────────────────

def collect_data(token, sub_key):
    data = {}
    print("  Pulling gross load...")
    try:
        rows = ercot_get("np6-345-cd/act_sys_load_by_wzn", token, sub_key)
        by_day = {}
        for row in rows:
            if not isinstance(row, list) or len(row) < 3:
                continue
            d = str(row[0])[:10]
            nums = [x for x in row[1:] if isinstance(x, (int, float))
                    and not isinstance(x, bool)]
            val = nums[-1] if nums else 0
            if d and val:
                by_day.setdefault(d, []).append(float(val))
        data["gross_load"] = {d: round(max(v) / 1000, 1)
                              for d, v in by_day.items() if v}
        print(f"    {len(data['gross_load'])} days of load data")
    except Exception as e:
        print(f"    WARN: load failed — {e}")
        data["gross_load"] = {}

    print("  Pulling wind generation...")
    try:
        rows = ercot_get("np4-732-cd/wpp_hrly_avrg_actl_fcast", token, sub_key,
                         {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY})
        by_day = {}
        for row in rows:
            if not isinstance(row, list) or len(row) < 4:
                continue
            d, val = str(row[1])[:10], safe_float(row[3])
            if d and val:
                by_day.setdefault(d, []).append(val)
        data["wind"] = {d: round(max(v) / 1000, 1) for d, v in by_day.items() if v}
        print(f"    {len(data['wind'])} days of wind data")
    except Exception as e:
        print(f"    WARN: wind failed — {e}")
        data["wind"] = {}

    print("  Pulling solar generation...")
    try:
        rows = ercot_get("np4-737-cd/spp_hrly_avrg_actl_fcast", token, sub_key,
                         {"deliveryDateFrom": WEEK_AGO, "deliveryDateTo": YESTERDAY})
        by_day = {}
        for row in rows:
            if not isinstance(row, list) or len(row) < 4:
                continue
            d, val = str(row[1])[:10], safe_float(row[3])
            if d and val:
                by_day.setdefault(d, []).append(val)
        data["solar"] = {d: round(max(v) / 1000, 1) for d, v in by_day.items() if v}
        print(f"    {len(data['solar'])} days of solar data")
    except Exception as e:
        print(f"    WARN: solar failed — {e}")
        data["solar"] = {}

    print(f"  Pulling RT + DA prices for {len(NODES)} nodes...")
    rt, da = {}, {}
    for node in NODES:
        time.sleep(2)
        try:
            rows = ercot_get("np6-905-cd/spp_node_zone_hub", token, sub_key,
                             {"settlementPoint": node,
                              "deliveryDateFrom": YESTERDAY,
                              "deliveryDateTo": YESTERDAY})
            prices = [p for row in rows for p in [extract_price(row)] if p]
            if prices:
                rt[node] = {
                    "avg": round(sum(prices) / len(prices), 2),
                    "max": round(max(prices), 2),
                    "min": round(min(prices), 2),
                }
        except Exception as e:
            print(f"    WARN: RT {node} — {e}")
        time.sleep(2)
        try:
            rows = ercot_get("np4-190-cd/dam_stlmnt_pnt_prices", token, sub_key,
                             {"settlementPoint": node,
                              "deliveryDateFrom": YESTERDAY,
                              "deliveryDateTo": YESTERDAY})
            prices = [p for row in rows for p in [extract_price(row)] if p]
            if prices:
                da[node] = {
                    "avg": round(sum(prices) / len(prices), 2),
                    "max": round(max(prices), 2),
                }
        except Exception as e:
            print(f"    WARN: DA {node} — {e}")

    data["rt"] = rt
    data["da"] = da
    print(f"    RT: {len(rt)} nodes  DA: {len(da)} nodes")

    # DART spreads
    common = set(rt) & set(da)
    data["dart"] = {
        n: round(rt[n]["avg"] - da[n]["avg"], 2)
        for n in common
    }
    return data

# ── REPORT BUILDER ────────────────────────────────────────────────────────────

def build_report(data):
    rt    = data.get("rt", {})
    da    = data.get("da", {})
    dart  = data.get("dart", {})
    load  = data.get("gross_load", {})
    wind  = data.get("wind", {})
    solar = data.get("solar", {})

    # Fleet summary metrics
    all_rt_avg  = [v["avg"] for v in rt.values()] if rt else [0]
    fleet_avg   = round(sum(all_rt_avg) / len(all_rt_avg), 2) if all_rt_avg else 0
    fleet_max   = round(max(v["max"] for v in rt.values()), 2) if rt else 0
    spike_nodes = [n for n, v in rt.items() if v["max"] > 100]
    neg_nodes   = [n for n, v in rt.items() if v["min"] < 0]
    best_dart   = max(dart, key=dart.get) if dart else None
    worst_dart  = min(dart, key=dart.get) if dart else None

    # ERCOT fundamentals rows (last 7 shared days)
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
          <td>{d}</td>
          <td>{g:.1f}</td>
          <td>{w:.1f}</td>
          <td>{s:.1f}</td>
          <td>{net:.1f}</td>
        </tr>"""

    # Price table rows sorted by RT avg descending
    price_rows = ""
    sorted_nodes = sorted(rt.keys(), key=lambda n: rt[n]["avg"], reverse=True)
    for node in sorted_nodes:
        r  = rt.get(node, {})
        d  = da.get(node, {})
        sp = dart.get(node)
        r_avg = r.get("avg", 0)
        r_max = r.get("max", 0)
        r_min = r.get("min", 0)
        d_avg = d.get("avg", 0)
        spike_cls = ' class="spike"' if r_max > 100 else ""
        neg_cls   = ' class="neg"'   if r_min < 0   else ""
        dart_cls  = ' class="rt-prem"' if (sp or 0) > 5 else (' class="da-prem"' if (sp or 0) < -5 else "")
        sp_str    = f"+${sp:.2f}" if sp and sp > 0 else (f"-${abs(sp):.2f}" if sp else "—")
        price_rows += f"""
        <tr>
          <td class="node-name">{node}</td>
          <td{spike_cls}>${r_avg:.2f}</td>
          <td{neg_cls}>${r_min:.2f}</td>
          <td{spike_cls}>${r_max:.2f}</td>
          <td>${d_avg:.2f}</td>
          <td{dart_cls}>{sp_str}</td>
        </tr>"""

    # DART summary callouts
    best_str  = f"{best_dart} +${dart[best_dart]:.2f}/MWh above DA" if best_dart else "N/A"
    worst_str = f"{worst_dart} ${dart[worst_dart]:.2f}/MWh below DA" if worst_dart else "N/A"
    spike_str = ", ".join(spike_nodes) if spike_nodes else "None"
    neg_str   = ", ".join(neg_nodes)   if neg_nodes   else "None"

    # Day of week for subject line context
    dow = date.today().strftime("%A")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>HEN Morning Report — {YESTERDAY}</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f5f7;margin:0;padding:24px 16px;color:#1a1a2e}}
  .outer{{max-width:800px;margin:0 auto}}
  .header{{background:#0a3d2e;border-radius:10px 10px 0 0;padding:20px 28px;display:flex;justify-content:space-between;align-items:center}}
  .header-left h1{{margin:0;font-size:20px;color:#fff;font-weight:600}}
  .header-left p{{margin:4px 0 0;font-size:12px;color:#7fc8a0}}
  .header-right{{text-align:right}}
  .header-right .date{{font-size:13px;color:#b8dfc8;font-family:monospace}}
  .header-right .gen-time{{font-size:11px;color:#5a9e78;margin-top:2px}}
  .body{{background:#fff;padding:0 0 24px}}
  .kpi-strip{{display:grid;grid-template-columns:repeat(4,1fr);gap:0;border-bottom:1px solid #eee}}
  .kpi{{padding:16px 20px;border-right:1px solid #eee}}
  .kpi:last-child{{border-right:none}}
  .kpi-label{{font-size:10px;font-weight:600;color:#888;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px}}
  .kpi-value{{font-size:22px;font-weight:600;color:#1a1a2e;font-family:monospace}}
  .kpi-sub{{font-size:11px;color:#888;margin-top:3px}}
  .section{{padding:20px 24px 0}}
  .section-title{{font-size:11px;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.07em;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #f0f0f0}}
  table{{width:100%;border-collapse:collapse;font-size:12px}}
  th{{background:#f8f9fa;padding:7px 10px;text-align:right;font-weight:600;color:#555;font-size:11px;border-bottom:2px solid #e8e8e8}}
  th:first-child{{text-align:left}}
  td{{padding:7px 10px;text-align:right;border-bottom:1px solid #f4f4f4;color:#2a2a3e}}
  td:first-child{{text-align:left}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#fafbfc}}
  .node-name{{font-family:monospace;font-size:11px;color:#444}}
  .spike{{color:#b33000;font-weight:600}}
  .neg{{color:#0066cc}}
  .rt-prem{{color:#1a7a3f;font-weight:600}}
  .da-prem{{color:#7a3a1a}}
  .charging-window td{{background:#f0faf4}}
  .callout-grid{{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:14px}}
  .callout{{background:#f8f9fa;border-radius:6px;padding:12px 14px;border-left:3px solid #ddd}}
  .callout.green{{border-left-color:#1a7a3f}}
  .callout.amber{{border-left-color:#c87800}}
  .callout.red{{border-left-color:#b33000}}
  .callout.blue{{border-left-color:#0055aa}}
  .callout-label{{font-size:10px;font-weight:700;color:#888;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px}}
  .callout-value{{font-size:12px;color:#1a1a2e;font-family:monospace}}
  .footer{{background:#f8f9fa;border-radius:0 0 10px 10px;padding:12px 24px;display:flex;justify-content:space-between;align-items:center;border-top:1px solid #eee}}
  .footer-left{{font-size:11px;color:#aaa}}
  .footer-right{{font-size:11px;color:#aaa;font-family:monospace}}
  @media(max-width:600px){{
    .kpi-strip{{grid-template-columns:1fr 1fr}}
    .callout-grid{{grid-template-columns:1fr}}
    .header{{flex-direction:column;gap:8px}}
  }}
</style>
</head>
<body>
<div class="outer">

  <div class="header">
    <div class="header-left">
      <h1>Hunt Energy Network</h1>
      <p>ERCOT Commercial Morning Report</p>
    </div>
    <div class="header-right">
      <div class="date">{dow}, {YESTERDAY}</div>
      <div class="gen-time">Generated {TODAY_STR} · Data through 24:00 CT</div>
    </div>
  </div>

  <div class="body">

    <!-- KPI STRIP -->
    <div class="kpi-strip">
      <div class="kpi">
        <div class="kpi-label">Fleet avg RT price</div>
        <div class="kpi-value">${fleet_avg:.2f}</div>
        <div class="kpi-sub">$/MWh · {len(rt)} nodes</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Fleet peak RT price</div>
        <div class="kpi-value">${fleet_max:.2f}</div>
        <div class="kpi-sub">$/MWh highest interval</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Spike events</div>
        <div class="kpi-value">{len(spike_nodes)}</div>
        <div class="kpi-sub">nodes &gt;$100/MWh</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Negative price nodes</div>
        <div class="kpi-value">{len(neg_nodes)}</div>
        <div class="kpi-sub">charging opportunities</div>
      </div>
    </div>

    <!-- ERCOT FUNDAMENTALS -->
    <div class="section">
      <div class="section-title">ERCOT fundamentals — 7-day lookback (GW, daily peak)</div>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Gross load</th>
            <th>Wind</th>
            <th>Solar</th>
            <th>Net load</th>
          </tr>
        </thead>
        <tbody>{fund_rows if fund_rows else '<tr><td colspan="5" style="text-align:center;color:#aaa;padding:20px">Data not available — ERCOT publishes after 8 AM CT</td></tr>'}
        </tbody>
      </table>
      <div style="font-size:10px;color:#aaa;margin-top:6px">Highlighted rows = net load &lt;30 GW (charging window signal)</div>
    </div>

    <!-- DART PRICE TABLE -->
    <div class="section" style="margin-top:20px">
      <div class="section-title">RT vs DA prices by node — {YESTERDAY}</div>
      <table>
        <thead>
          <tr>
            <th>Node</th>
            <th>RT avg</th>
            <th>RT min</th>
            <th>RT max</th>
            <th>DA avg</th>
            <th>DART spread</th>
          </tr>
        </thead>
        <tbody>{price_rows if price_rows else '<tr><td colspan="6" style="text-align:center;color:#aaa;padding:20px">No price data available</td></tr>'}
        </tbody>
      </table>
      <div style="font-size:10px;color:#aaa;margin-top:6px">
        Green DART = RT premium (asset earned above DA) · Brown = DA premium · Red = spike &gt;$100 · Blue = negative price
      </div>
    </div>

    <!-- CALLOUT GRID -->
    <div class="section" style="margin-top:20px">
      <div class="section-title">Key signals — {YESTERDAY}</div>
      <div class="callout-grid">
        <div class="callout green">
          <div class="callout-label">Best DART performer</div>
          <div class="callout-value">{best_str}</div>
        </div>
        <div class="callout amber">
          <div class="callout-label">Largest DA premium</div>
          <div class="callout-value">{worst_str}</div>
        </div>
        <div class="callout red">
          <div class="callout-label">Price spike nodes (&gt;$100/MWh)</div>
          <div class="callout-value">{spike_str}</div>
        </div>
        <div class="callout blue">
          <div class="callout-label">Negative price nodes (charging)</div>
          <div class="callout-value">{neg_str}</div>
        </div>
      </div>
    </div>

  </div>

  <div class="footer">
    <div class="footer-left">Hunt Energy Network · Commercial Operations · Confidential</div>
    <div class="footer-right">Data: ERCOT Public API · {TODAY_STR}</div>
  </div>

</div>
</body>
</html>"""
    return html

# ── EMAIL VIA SENDGRID ────────────────────────────────────────────────────────

def send_email(html, subject, from_addr, to_addrs, api_key):
    """Send HTML email via SendGrid's REST API — no SDK required."""
    payload = {
        "personalizations": [
            {
                "to": [{"email": addr} for addr in to_addrs],
                "subject": subject,
            }
        ],
        "from": {"email": from_addr, "name": "HEN Morning Report"},
        "content": [{"type": "text/html", "value": html}],
    }
    r = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid {r.status_code}: {r.text[:300]}")
    print(f"  Email sent to {len(to_addrs)} recipient(s) (SendGrid {r.status_code})")

# ── S3 ARCHIVE (optional — requires AWS setup) ────────────────────────────────

def archive_to_s3(html, data_json, bucket):
    try:
        import boto3
    except ImportError:
        print("  SKIP: boto3 not installed — S3 archive skipped")
        return
    s3 = boto3.client("s3")
    yr, mo, dy = YESTERDAY[:4], YESTERDAY[5:7], YESTERDAY[8:]
    s3.put_object(
        Bucket=bucket,
        Key=f"reports/{yr}/{mo}/{dy}/morning-report.html",
        Body=html.encode("utf-8"),
        ContentType="text/html",
    )
    s3.put_object(
        Bucket=bucket,
        Key=f"raw-data/{YESTERDAY}/ercot-public.json",
        Body=json.dumps(data_json, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  Archived to s3://{bucket}/reports/{yr}/{mo}/{dy}/")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\nHEN Morning Report — {YESTERDAY}")
    print(f"Nodes: {len(NODES)}")

    # Read env
    username   = os.environ.get("ERCOT_USERNAME", "")
    password   = os.environ.get("ERCOT_PASSWORD", "")
    sub_key    = os.environ.get("ERCOT_SUBSCRIPTION_KEY", "")
    sg_api_key = os.environ.get("SENDGRID_API_KEY", "")
    from_addr  = os.environ.get("FROM_EMAIL", "")
    to_raw     = os.environ.get("TO_EMAILS", "")
    s3_bucket  = os.environ.get("S3_BUCKET", "")
    to_addrs   = [e.strip() for e in to_raw.split(",") if e.strip()]

    missing = []
    if not username:   missing.append("ERCOT_USERNAME")
    if not password:   missing.append("ERCOT_PASSWORD")
    if not sub_key:    missing.append("ERCOT_SUBSCRIPTION_KEY")
    if not sg_api_key: missing.append("SENDGRID_API_KEY")
    if not from_addr:  missing.append("FROM_EMAIL")
    if not to_addrs:   missing.append("TO_EMAILS")
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    # Authenticate
    print("\n1. Authenticating with ERCOT...")
    try:
        token = get_token(username, password, sub_key)
        print("   Token obtained.")
    except Exception as e:
        print(f"   FAILED: {e}")
        sys.exit(1)

    # Collect data
    print("\n2. Collecting ERCOT data...")
    data = collect_data(token, sub_key)

    # Build report
    print("\n3. Building HTML report...")
    html = build_report(data)
    with open("morning_report.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("   Report written to morning_report.html")

    # Archive to S3 if configured (skipped gracefully if AWS not set up)
    if s3_bucket:
        print(f"\n4. Archiving to S3 ({s3_bucket})...")
        try:
            archive_to_s3(html, data, s3_bucket)
        except Exception as e:
            print(f"   WARN: S3 archive failed — {e} (report will still send)")

    # Send email via SendGrid
    print("\n5. Sending email via SendGrid...")
    dow     = date.today().strftime("%A")
    subject = f"HEN Morning Report — {dow} {YESTERDAY}"
    try:
        send_email(html, subject, from_addr, to_addrs, sg_api_key)
    except Exception as e:
        print(f"   FAILED to send email: {e}")
        sys.exit(1)

    print(f"\nDone. Report delivered for {YESTERDAY}.\n")

if __name__ == "__main__":
    main()
