import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import { getFunnel } from "../api/client";

// funnel rows come per category+day; aggregate to totals per category for the bar chart
function byCategory(rows) {
  const acc = {};
  for (const r of rows) {
    const c = (acc[r.category_id] ||= { category_id: r.category_id, views: 0, carts: 0, purchases: 0 });
    c.views += r.views;
    c.carts += r.carts;
    c.purchases += r.purchases;
  }
  return Object.values(acc).sort((a, b) => b.views - a.views).slice(0, 12);
}

export default function FunnelChart() {
  const [data, setData] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    getFunnel().then((rows) => setData(byCategory(rows))).catch((e) => setError(e.message));
  }, []);

  if (error) return <p className="err">Failed to load funnel: {error}</p>;

  return (
    <div className="card">
      <h2>Funnel by category</h2>
      <p className="hint">Top categories by views — view / cart / purchase counts.</p>
      <ResponsiveContainer width="100%" height={320}>
        <BarChart data={data}>
          <XAxis dataKey="category_id" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Bar dataKey="views" fill="#4e79a7" />
          <Bar dataKey="carts" fill="#f28e2b" />
          <Bar dataKey="purchases" fill="#59a14f" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
