import { useState } from "react";
import { getRecommendations } from "../api/client";

export default function RecommendDemo() {
  const [itemId, setItemId] = useState("");
  const [method, setMethod] = useState("als");
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  async function submit(e) {
    e.preventDefault();
    if (!itemId) return;
    setLoading(true);
    setError(null);
    try {
      setResult(await getRecommendations(itemId, { method, n: 10 }));
    } catch (err) {
      setError(err.message);
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card">
      <h2>Recommend demo</h2>
      <form onSubmit={submit} className="row">
        <input
          type="number" placeholder="item id" value={itemId}
          onChange={(e) => setItemId(e.target.value)}
        />
        <select value={method} onChange={(e) => setMethod(e.target.value)}>
          <option value="als">als</option>
          <option value="item2vec">item2vec</option>
        </select>
        <button type="submit" disabled={loading}>{loading ? "…" : "Get"}</button>
      </form>

      {error && <p className="err">{error}</p>}
      {result && (
        <>
          <p className="hint">
            source: <b>{result.source}</b>{result.method ? ` (${result.method})` : ""}
          </p>
          <table>
            <thead><tr><th>Rank</th><th>Item</th><th>Score</th></tr></thead>
            <tbody>
              {result.items.map((r) => (
                <tr key={r.rank}>
                  <td>{r.rank}</td><td>{r.rec_item_id}</td><td>{r.score.toFixed(3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {result.items.length === 0 && <p className="hint">No recommendations for this item.</p>}
        </>
      )}
    </div>
  );
}
