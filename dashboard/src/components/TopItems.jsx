import { useEffect, useState } from "react";
import { getTopItems } from "../api/client";

export default function TopItems() {
  const [items, setItems] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    getTopItems({ limit: 10 }).then(setItems).catch((e) => setError(e.message));
  }, []);

  if (error) return <p className="err">Failed to load top items: {error}</p>;

  return (
    <div className="card">
      <h2>Top items by conversion</h2>
      <p className="hint">Highest purchase/view rate (min 20 views).</p>
      <table>
        <thead>
          <tr><th>Item</th><th>Category</th><th>Views</th><th>Purchases</th><th>Conv. rate</th></tr>
        </thead>
        <tbody>
          {items.map((it) => (
            <tr key={it.itemid}>
              <td>{it.itemid}</td>
              <td>{it.categoryid ?? "—"}</td>
              <td>{it.views}</td>
              <td>{it.purchases}</td>
              <td>{(it.item_purchase_rate * 100).toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
