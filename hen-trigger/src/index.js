// Starts the "HEN Daily Morning Report" GitHub Actions workflow at 6:00 AM
// Central, every day.
//
// Why this exists: GitHub's own `schedule:` trigger only promises "roughly on
// time" -- this workflow's scheduled runs were starting 2-5 hours late. A
// Cloudflare cron fires on the minute, and a workflow_dispatch API call starts
// the run within seconds. The pipeline itself still runs on GitHub's runners.
//
// Cloudflare crons are UTC-only; wrangler.toml registers both 11:00 and 12:00
// UTC and this handler only acts on whichever one is 6 AM in Chicago, so it
// stays correct across daylight saving changes with no edits twice a year.

const TARGET_HOUR_CT = 6;
const MAX_ATTEMPTS   = 3;

function chicagoHour(ms) {
  return Number(new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago", hour: "numeric", hourCycle: "h23",
  }).format(new Date(ms)));
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function dispatchWorkflow(env) {
  const url = `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}` +
              `/actions/workflows/${env.GH_WORKFLOW}/dispatches`;
  const init = {
    method: "POST",
    headers: {
      Authorization:          `Bearer ${env.GH_TOKEN}`,
      Accept:                 "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent":           "hen-morning-report-trigger",
      "Content-Type":         "application/json",
    },
    body: JSON.stringify({ ref: env.GH_REF || "main" }),
  };
  const delayMs = Number(env.RETRY_DELAY_MS ?? 5000);
  let lastError;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    let res = null;
    try {
      res = await fetch(url, init);
    } catch (err) {                       // network error: worth retrying
      lastError = err;
    }
    if (res) {
      if (res.status === 204) {           // GitHub's success response
        console.log(`Dispatched ${env.GH_WORKFLOW} on ${env.GH_REF || "main"} (attempt ${attempt})`);
        return;
      }
      const detail = await res.text();
      lastError = new Error(`GitHub dispatch failed: HTTP ${res.status} ${detail}`);
      // 4xx (bad/expired token, wrong repo or workflow name, workflow lacking
      // workflow_dispatch) won't fix itself on retry -- fail immediately.
      if (res.status < 500 && res.status !== 429) throw lastError;
    }
    if (attempt < MAX_ATTEMPTS) {
      console.log(`Attempt ${attempt} failed (${lastError.message}) -- retrying`);
      await sleep(delayMs * attempt);
    }
  }
  throw lastError;
}

export default {
  async scheduled(event, env, ctx) {
    const hour = chicagoHour(event.scheduledTime);
    // FORCE_RUN=1 exists only for the one-time local test (.dev.vars); it is
    // not set in production.
    if (hour !== TARGET_HOUR_CT && env.FORCE_RUN !== "1") {
      console.log(`Skip: ${new Date(event.scheduledTime).toISOString()} is ${hour}:00 in Chicago, ` +
                  `not ${TARGET_HOUR_CT}:00`);
      return;
    }
    await dispatchWorkflow(env);
  },
};
