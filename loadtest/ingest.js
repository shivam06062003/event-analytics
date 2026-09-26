// Ingestion load test: POST /v1/batch with realistic batches.
//   BATCH=100      events per request
//   VUS=40         concurrent clients (closed model: max throughput)
//   RATE=250       or: fixed requests/second (open model: latency at a load)
// Run with `make loadtest` (quotas are disabled for the run).
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://api:8000";
const BATCH = parseInt(__ENV.BATCH || "100", 10);
const NAMES = ["page_viewed", "page_viewed", "page_viewed", "button_clicked", "signup", "purchase"];
const PATHS = ["/", "/pricing", "/docs", "/blog", "/app"];
const HEADERS = { Authorization: `Bearer ${__ENV.WRITE_KEY}`, "Content-Type": "application/json" };

const scenario = __ENV.RATE
  ? { executor: "constant-arrival-rate", rate: parseInt(__ENV.RATE, 10), timeUnit: "1s",
      duration: __ENV.DURATION || "30s", preAllocatedVUs: 50, maxVUs: 200 }
  : { executor: "constant-vus", vus: parseInt(__ENV.VUS || "40", 10), duration: __ENV.DURATION || "30s" };

export const options = {
  scenarios: { ingest: scenario },
  thresholds: { "http_req_failed{name:batch}": ["rate<0.01"] },
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
};

// Unique, valid UUIDs without generating 36 random characters per event: a
// random prefix per virtual user + a counter. (Generating fully random UUIDs
// made k6 itself the bottleneck: it used as much CPU as the API under test.)
const PREFIX = "xxxxxxxx-xxxx-4xxx-yxxx-".replace(/[xy]/g, (c) => {
  const r = (Math.random() * 16) | 0;
  return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
});
let counter = 0;
function uuid() {
  counter += 1;
  return PREFIX + counter.toString(16).padStart(12, "0");
}

export default function () {
  const now = new Date().toISOString();
  const batch = [];
  for (let i = 0; i < BATCH; i++) {
    const user = `user-${Math.floor(Math.random() * 1000)}`;
    batch.push({
      event_id: uuid(),
      event: NAMES[Math.floor(Math.random() * NAMES.length)],
      user_id: user,
      timestamp: now,
      properties: { path: PATHS[i % PATHS.length], value: i },
    });
  }
  const res = http.post(`${BASE}/v1/batch`, JSON.stringify({ sent_at: now, batch }), {
    headers: HEADERS,
    tags: { name: "batch" },
  });
  check(res, { "202 all accepted": (r) => r.status === 202 && r.json("accepted") === BATCH });
}
