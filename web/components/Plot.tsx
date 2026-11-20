"use client";
import { useEffect, useRef } from "react";

// Shared dark theme (same colours as the Streamlit PLOTLY_THEME)
export const THEME = {
  plot_bgcolor: "#161b22",
  paper_bgcolor: "#0d1117",
  font: { color: "#e6edf3", family: "Inter, sans-serif" },
  xaxis: { gridcolor: "#21262d", zerolinecolor: "#21262d" },
  yaxis: { gridcolor: "#21262d", zerolinecolor: "#21262d" },
  margin: { l: 50, r: 20, t: 40, b: 40 },
};

// Thin wrapper that loads Plotly in the browser only and redraws when data changes
export default function Plot({ data, layout, height = 320 }: { data: any[]; layout?: any; height?: number }) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let alive = true;
    let el = ref.current;
    (async () => {
      const Plotly: any = (await import("plotly.js-dist-min")).default;
      if (!alive || !el) return;
      Plotly.react(el, data, { ...THEME, height, autosize: true, ...layout }, { responsive: true, displayModeBar: false });
    })();
    return () => {
      alive = false;
    };
  }, [data, layout, height]);

  useEffect(() => {
    const el = ref.current;
    return () => {
      if (el) import("plotly.js-dist-min").then((m: any) => m.default.purge(el));
    };
  }, []);

  return <div ref={ref} style={{ width: "100%", minHeight: height }} />;
}
