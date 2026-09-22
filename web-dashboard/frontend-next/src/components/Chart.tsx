"use client";

import { useEffect, useRef } from "react";
import { createChart, ColorType } from "lightweight-charts";

export default function Chart({ data, symbol }: { data: any[], symbol: string }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current?.clientWidth });
    };

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#a1a1aa", // zinc-400
      },
      grid: {
        vertLines: { color: "#27272a" }, // zinc-800
        horzLines: { color: "#27272a" },
      },
      width: chartContainerRef.current.clientWidth,
      height: 300,
    });

    chart.timeScale().fitContent();

    const newSeries = chart.addAreaSeries({
      lineColor: "#06b6d4", // cyan-500
      topColor: "rgba(6, 182, 212, 0.4)",
      bottomColor: "rgba(6, 182, 212, 0.0)",
    });
    
    // Sort data ascending by time for lightweight charts
    const sortedData = [...data].sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());
    
    const formattedData = sortedData.map(d => ({
      time: new Date(d.time).getTime() / 1000 as any, // Unix timestamp in seconds
      value: d.price
    }));

    newSeries.setData(formattedData);

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, [data]);

  return (
    <div className="w-full">
      <h3 className="text-lg font-semibold mb-2">{symbol}</h3>
      <div ref={chartContainerRef} className="w-full h-[300px]" />
    </div>
  );
}
