import { useEffect, useState } from "react";
import { getPipelineHealth } from "../api/client";

function fmtTime(ts) {
  return new Date(ts).toLocaleString();
}

export default function PipelineHealth() {
  const [runs, setRuns] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    getPipelineHealth().then(setRuns).catch((e) => setError(e.message));
  }, []);

  if (error) return <p className="err">Failed to load pipeline health: {error}</p>;

  return (
    <div className="card">
      <h2>Pipeline health</h2>
      <p className="hint">Latest run per task.</p>
      <table>
        <thead>
          <tr><th>Task</th><th>Status</th><th>Rows</th><th>Duration (s)</th><th>Started</th></tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.task_name}>
              <td>{r.task_name}</td>
              <td><span className={r.status === "success" ? "ok" : "bad"}>{r.status}</span></td>
              <td>{r.rows_processed ?? "—"}</td>
              <td>{r.duration_seconds != null ? r.duration_seconds.toFixed(1) : "—"}</td>
              <td>{fmtTime(r.started_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
