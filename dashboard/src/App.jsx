import { useState } from "react";
import FunnelChart from "./components/FunnelChart.jsx";
import TopItems from "./components/TopItems.jsx";
import RecommendDemo from "./components/RecommendDemo.jsx";
import PipelineHealth from "./components/PipelineHealth.jsx";

const TABS = {
  overview: "Overview",
  recommend: "Recommend",
  health: "Pipeline health",
};

export default function App() {
  const [tab, setTab] = useState("overview");

  return (
    <div className="app">
      <header>
        <h1>RetailRocket Intelligence</h1>
        <nav>
          {Object.entries(TABS).map(([key, label]) => (
            <button
              key={key}
              className={tab === key ? "active" : ""}
              onClick={() => setTab(key)}
            >
              {label}
            </button>
          ))}
        </nav>
      </header>

      <main>
        {tab === "overview" && (
          <>
            <FunnelChart />
            <TopItems />
          </>
        )}
        {tab === "recommend" && <RecommendDemo />}
        {tab === "health" && <PipelineHealth />}
      </main>
    </div>
  );
}
