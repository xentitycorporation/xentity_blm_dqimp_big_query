const CSV_URL =
  "https://raw.githubusercontent.com/xentitycorporation/xentity_blm_dqimp_big_query/main/web-app/Waterfall_Counts_Decimal.csv";

function monthLabel(m) {
  const s = String(m);
  const yyyy = s.slice(0, 4);
  const mm = s.slice(4, 6);
  return `${yyyy}-${mm}`;
}
function parseNumber(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function render(rows) {
  const data = rows
    .filter(r => r.Month && r.Type)
    .map(r => ({
      Month: parseNumber(r.Month),
      Type: String(r.Type).trim(),
      Value: parseNumber(r.Value),
      Sort: parseNumber(r.Sort),
    }))
    .sort((a, b) => a.Sort - b.Sort);

  const months = [...new Set(data.map(d => d.Month))].sort((a, b) => a - b);
  const latestMonth = months[months.length - 1];

  const snapshotRows = data.filter(d => d.Type.toLowerCase() === "snapshot total");
  const latestSnapshot = snapshotRows.find(d => d.Month === latestMonth);

  const prevSnapshotMonth = [...new Set(snapshotRows.map(d => d.Month))]
    .filter(m => m < latestMonth)
    .sort((a, b) => b - a)[0];

  const prevSnapshot = snapshotRows.find(d => d.Month === prevSnapshotMonth);

  const contrib = data
    .filter(d => d.Month === latestMonth)
    .filter(d => d.Type.toLowerCase() !== "snapshot total");

  if (!latestSnapshot || !prevSnapshot) {
    Plotly.newPlot("chart", [{
      type: "scatter",
      x: [0],
      y: [0],
      text: ["Missing Snapshot Total rows to build waterfall."],
      mode: "text"
    }], { title: "Waterfall" }, { responsive: true });
    return;
  }

  const startVal = prevSnapshot.Value;
  const endVal = latestSnapshot.Value;

  const x = [
    `Start (${monthLabel(prevSnapshotMonth)})`,
    ...contrib.map(c => c.Type),
    `End (${monthLabel(latestMonth)})`
  ];

  const measure = ["absolute", ...contrib.map(() => "relative"), "total"];
  const y = [startVal, ...contrib.map(c => c.Value), endVal];

  const trace = {
    type: "waterfall",
    x, y, measure,
    textposition: "outside",
    connector: { line: { width: 1 } }
  };

  const layout = {
    title: `Out-of-Sync Waterfall — ${monthLabel(latestMonth)}`,
    margin: { t: 60, r: 20, b: 80, l: 60 },
    yaxis: { title: "Count" },
    xaxis: { automargin: true }
  };

  Plotly.newPlot("chart", [trace], layout, { responsive: true });
}

Papa.parse(CSV_URL, {
  download: true,
  header: true,
  dynamicTyping: true,
  complete: (results) => render(results.data),
  error: (err) => {
    console.error(err);
    Plotly.newPlot("chart", [{
      type: "scatter",
      x: [0],
      y: [0],
      text: ["Failed to load CSV. Check CSV_URL + repo permissions."],
      mode: "text"
    }], { title: "Waterfall" }, { responsive: true });
  }
});
