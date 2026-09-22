"use client";

import { useEffect, useState } from "react";
import Chart from "@/components/Chart";

export default function Dashboard() {
  const [data, setData] = useState<any>(null);
  const [history, setHistory] = useState<any>(null);
  const [alerts, setAlerts] = useState<any[]>([]);

  // Connect to SSE for real-time prices
  useEffect(() => {
    const sse = new EventSource("http://localhost:8000/api/data/stream");
    sse.onmessage = (e) => {
      try {
        const parsed = JSON.parse(e.data);
        if (parsed.status === "success") {
          setData(parsed.data);
        }
      } catch (err) {}
    };
    return () => sse.close();
  }, []);

  // Connect to SSE for alerts
  useEffect(() => {
    const sse = new EventSource("http://localhost:8000/api/alerts/stream");
    sse.onmessage = (e) => {
      try {
        const parsed = JSON.parse(e.data);
        setAlerts((prev) => [parsed, ...prev].slice(0, 10)); // keep last 10
      } catch (err) {}
    };
    return () => sse.close();
  }, []);

  // Fetch History for Chart
  useEffect(() => {
    fetch("http://localhost:8000/api/history?symbols=BTCUSDT,ETHUSDT")
      .then((res) => res.json())
      .then((res) => {
        if (res.status === "success") {
          setHistory(res.data);
        }
      });
  }, []);

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"].map((sym) => {
          const price = data?.prices?.[sym] || 0;
          const pred = data?.predictions?.[sym];
          const trend = pred?.trend === "UP" ? "text-green-500" : "text-red-500";
          
          return (
            <div key={sym} className="p-6 rounded-2xl bg-zinc-900/50 border border-zinc-800 shadow-xl backdrop-blur-sm hover:border-cyan-500/50 transition-colors">
              <h4 className="text-zinc-400 font-medium text-sm tracking-wide">{sym}</h4>
              <p className="text-3xl font-bold mt-2">${price.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 4 })}</p>
              {pred && (
                <div className="mt-4 pt-4 border-t border-zinc-800/50 flex justify-between text-xs">
                  <span className="text-zinc-500">AI Predict</span>
                  <span className={`font-semibold ${trend}`}>
                    ${pred.predicted_price} ({pred.trend})
                  </span>
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-6">
          <div className="p-6 rounded-2xl bg-zinc-900/50 border border-zinc-800 shadow-xl">
            {history?.["BTCUSDT"] ? (
              <Chart data={history["BTCUSDT"]} symbol="BTCUSDT" />
            ) : (
              <div className="h-[300px] flex items-center justify-center text-zinc-500">Loading Chart...</div>
            )}
          </div>
        </div>
        
        <div className="space-y-6">
          <div className="p-6 rounded-2xl bg-zinc-900/50 border border-zinc-800 shadow-xl h-full">
            <h3 className="text-lg font-semibold mb-4 text-cyan-500">Live Alerts</h3>
            <div className="space-y-3">
              {alerts.length === 0 ? (
                <p className="text-sm text-zinc-500">No recent alerts.</p>
              ) : (
                alerts.map((alert, i) => (
                  <div key={i} className="p-3 rounded-lg bg-zinc-800/50 border border-zinc-700/50">
                    <div className="flex justify-between items-center mb-1">
                      <span className="text-xs font-bold text-zinc-300">{alert.symbol}</span>
                      <span className={`text-[10px] px-2 py-0.5 rounded-full ${alert.alert_type.includes("PUMP") ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>
                        {alert.alert_type}
                      </span>
                    </div>
                    <p className="text-xs text-zinc-400">{alert.message}</p>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
