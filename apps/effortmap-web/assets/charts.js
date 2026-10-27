/* Charts, as SVG, with no library.
 *
 * The house rule for every page in this repository is nothing fetched at run time and
 * no build step, so a charting library is not available and is not wanted: there are
 * two chart shapes here and both are horizontal bars.
 *
 * The mark specs are the ones the Streamlit version follows, so the two front ends
 * read as one product: thin bars with 4px rounded data-ends anchored to the baseline,
 * a 2px surface gap between adjacent fills, recessive axes, a legend whenever there is
 * more than one series, and direct value labels — never a number on every point of a
 * dense chart, but these are short lists where the value belongs beside the bar.
 *
 * Colour never carries identity alone. Every chart here is drawn directly above a
 * table of the same numbers, and each bar carries its own `<title>`, so the reading is
 * available without seeing hue at all.
 */

const NS = "http://www.w3.org/2000/svg";

const el = (name, attrs = {}, text = null) => {
  const node = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, String(v));
  if (text !== null) node.textContent = text;
  return node;
};

/** Round a maximum up to a pleasant axis bound, so ticks land on readable values. */
function niceMax(value) {
  if (!(value > 0)) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const step = [1, 2, 2.5, 5, 10].find((s) => value <= s * magnitude) ?? 10;
  return step * magnitude;
}

/**
 * Draw a horizontal bar chart into `container`.
 *
 * `rows`: `{ label, value, display, series, title }[]` — `series` picks the colour and
 * the legend entry, `display` is the text drawn at the end of the bar, `title` is the
 * native hover text.
 * `series`: `{ key -> { color, label } }`, in the fixed order the legend shows. Fixed
 * so that filtering a category out never repaints the survivors.
 */
export function barChart(container, { rows, series, axisLabel, rowHeight = 30 }) {
  container.textContent = "";
  if (!rows.length) return;

  const keys = Object.keys(series);
  const showLegend = keys.length > 1;

  const padLeft = Math.min(220, 8 * Math.max(...rows.map((r) => r.label.length)) + 16);
  const padRight = 76;              // room for the direct value label
  const padTop = showLegend ? 30 : 8;
  const padBottom = 34;             // axis ticks + label
  const plotHeight = rows.length * rowHeight;
  const width = 900;                // viewBox units; the SVG scales to its container
  const plotWidth = width - padLeft - padRight;
  const height = padTop + plotHeight + padBottom;

  const svg = el("svg", {
    viewBox: `0 0 ${width} ${height}`,
    width: "100%",
    height,
    role: "img",
    "aria-label": axisLabel ? `Bar chart, ${axisLabel}` : "Bar chart",
  });

  const max = niceMax(Math.max(...rows.map((r) => r.value), 0));
  const x = (v) => padLeft + (v / max) * plotWidth;

  // --- axis: four ticks, recessive, behind the marks ---
  for (let i = 0; i <= 4; i++) {
    const value = (max / 4) * i;
    const px = x(value);
    svg.appendChild(
      el("line", {
        x1: px, x2: px, y1: padTop, y2: padTop + plotHeight,
        class: "chart-grid",
      }),
    );
    svg.appendChild(
      el("text", { x: px, y: padTop + plotHeight + 16, class: "chart-tick" },
        formatTick(value, max)),
    );
  }
  if (axisLabel) {
    svg.appendChild(
      el("text", {
        x: padLeft + plotWidth / 2, y: padTop + plotHeight + 30, class: "chart-axis-label",
      }, axisLabel),
    );
  }

  // --- bars ---
  const gap = 2;                    // the surface gap between adjacent fills
  const barHeight = Math.max(8, rowHeight - gap * 2 - 6);
  rows.forEach((row, i) => {
    const y = padTop + i * rowHeight + (rowHeight - barHeight) / 2;
    const length = Math.max(row.value > 0 ? 3 : 0, x(row.value) - padLeft);
    const color = series[row.series]?.color ?? series[keys[0]].color;

    svg.appendChild(
      el("text", { x: padLeft - 10, y: y + barHeight / 2 + 4, class: "chart-row-label" },
        row.label),
    );

    const bar = el("rect", {
      x: padLeft, y, width: length, height: barHeight,
      rx: 4, ry: 4,                 // 4px rounded data-end
      fill: color, class: "chart-bar",
    });
    bar.appendChild(el("title", {}, row.title ?? `${row.label}: ${row.display}`));
    svg.appendChild(bar);

    if (row.display) {
      svg.appendChild(
        el("text", { x: padLeft + length + 8, y: y + barHeight / 2 + 4, class: "chart-value" },
          row.display),
      );
    }
  });

  // --- legend, only when there is more than one series ---
  // Along the top, which is safe here because this SVG has an explicit height rather
  // than being fit-autosized: the collapse that bites a fit-sized chart cannot happen.
  if (showLegend) {
    let cursor = padLeft;
    for (const key of keys) {
      svg.appendChild(el("rect", { x: cursor, y: 8, width: 11, height: 11, rx: 2, fill: series[key].color }));
      const label = el("text", { x: cursor + 17, y: 18, class: "chart-legend" }, series[key].label);
      svg.appendChild(label);
      cursor += 17 + series[key].label.length * 7.2 + 20;
    }
  }

  container.appendChild(svg);
}

function formatTick(value, max) {
  if (max >= 10) return String(Math.round(value));
  if (max >= 1) return value.toFixed(1);
  return value.toFixed(2);
}
