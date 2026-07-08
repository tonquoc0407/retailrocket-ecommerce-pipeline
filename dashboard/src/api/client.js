// all backend calls go through here so components never touch fetch directly.
// base is empty in dev (vite proxy handles it); set VITE_API_BASE for a built deploy.
const BASE = import.meta.env.VITE_API_BASE || "";

async function get(path) {
  const res = await fetch(BASE + path);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

async function post(path, body) {
  const res = await fetch(BASE + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

export function getFunnel({ categoryId, from, to } = {}) {
  const q = new URLSearchParams();
  if (categoryId) q.set("category_id", categoryId);
  if (from) q.set("from", from);
  if (to) q.set("to", to);
  const qs = q.toString();
  return get("/funnel-stats" + (qs ? "?" + qs : ""));
}

export function getTopItems({ limit = 10, minViews = 20 } = {}) {
  return get(`/top-items?limit=${limit}&min_views=${minViews}`);
}

export function getRecommendations(itemId, { method = "als", n = 10 } = {}) {
  return get(`/recommend/${itemId}?method=${method}&n=${n}`);
}

export function getPipelineHealth() {
  return get("/pipeline-health");
}

export function predictAbandon(features) {
  return post("/predict-abandon", features);
}
