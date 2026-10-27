/* Shaping a planner result into the rows the page draws.
 *
 * A direct port of `apps/effortmap/effortmap.py`, and deliberately the same split:
 * everything here is a pure function over the planner's JSON, returning plain arrays
 * and objects. No DOM, no wasm, no fetch — so `test/effortmap.test.mjs` runs it under
 * plain node with no dependencies and no browser.
 *
 * Unlike the Python version this reads the wire JSON directly rather than an SDK's
 * dataclasses. There is no SDK here to read: the wasm module hands back exactly the
 * `PlanResponse` that `freshet-embed` serializes, so the tagged enums (`reason`,
 * `unit`, `basis`) are unwrapped by the small accessors below.
 *
 * The vocabulary is the planner's, unchanged: cost is in compute units, one CU is one
 * full rebuild of one `table` model, and nothing here converts to time because nothing
 * upstream measures time.
 */

/** The planner stores cost as an integer in thousandths of a compute unit, so sums
 *  and comparisons stay exact and a `view` (50 mCU) is not rounded to zero. */
export const MILLICU_PER_CU = 1000;

/** Categorical slots 1 and 2 of the reference palette, light and dark. Two hues is the
 *  whole categorical need — "did I change this, or is it blast radius?" — and the pair
 *  validates on every check in both modes (worst CVD ΔE 24.7 light / 26.8 dark against
 *  an ΔE>=8 target). Identity is never carried by colour alone: the same split is a
 *  text column in the table beside every chart. */
export const SERIES = {
  light: { direct: "#2a78d6", downstream: "#eb6834" },
  dark: { direct: "#3987e5", downstream: "#d95926" },
};

/** What each rate-card basis means, in the words the planner would use. Shown in the
 *  table so the cost column is never a bare number the reader has to trust. */
export const BASIS_LABEL = {
  table_full: "full table rebuild",
  view_full: "view redefinition",
  incremental_full: "full incremental refresh",
  incremental_partitions: "partition-scoped",
  unknown_model: "not in the project",
};

/** Whole compute units, for display only. The millicu integer is the exact figure. */
export const cu = (millicu) => millicu / MILLICU_PER_CU;

// ── unwrapping the tagged enums ───────────────────────────────────────────────
// serde emits a unit variant as a bare string and a data-carrying one as a
// single-key object. These three accessors are the only places that knows that.

/** The upstream model this one was attributed to, or null if it changed itself. */
export function becauseOf(model) {
  const r = model.reason;
  return r && typeof r === "object" && r.downstream ? r.downstream.because_of : null;
}

/** The changed columns that pulled this model in. Empty if it changed itself. */
export function viaColumns(model) {
  const r = model.reason;
  return r && typeof r === "object" && r.downstream ? r.downstream.via_columns || [] : [];
}

/** The partitions this model rebuilds, or null for a whole-model rebuild. */
export function partitionsOf(model) {
  const u = model.unit;
  return u && typeof u === "object" && u.partitions ? u.partitions : null;
}

/** A basis as a bare string, flattening the one variant that carries data. */
export function basisName(basis) {
  return basis && typeof basis === "object" ? Object.keys(basis)[0] : String(basis);
}

/** How many partitions were charged for, when that is the basis. */
export function partitionCount(basis) {
  return basis && typeof basis === "object" && basis.incremental_partitions
    ? basis.incremental_partitions.count
    : null;
}

export const isDirectlyChanged = (model) => model.reason === "directly_changed";

// ── the rows the page draws ───────────────────────────────────────────────────

/** One row per model in the plan: what rebuilds, why, and what it costs.
 *
 *  Sorted by cost descending so the models that dominate the rebuild are at the top,
 *  which is the question this table exists to answer. Ties break by name, so the order
 *  is stable across renders and a reader's eye does not have to re-find a row.
 *
 *  A plan without cost still produces rows — the cost fields are simply null, so the
 *  dependency answer is readable on its own. */
export function effortRows(plan) {
  const byModel = new Map((plan.cost?.per_model ?? []).map((m) => [m.model, m]));
  const rows = (plan.models ?? []).map((model) => {
    const cost = byModel.get(model.name) ?? null;
    const partitions = partitionsOf(model);
    return {
      model: model.name,
      origin: isDirectlyChanged(model) ? "changed" : "downstream",
      becauseOf: becauseOf(model) ?? "",
      viaColumns: viaColumns(model).join(", "),
      unit: partitions === null ? "whole model" : `${partitions.length} partitions`,
      partitions: partitions ?? [],
      millicu: cost ? cost.millicu : null,
      cu: cost ? cu(cost.millicu) : null,
      basis: cost ? basisName(cost.basis) : "",
      basisLabel: cost ? BASIS_LABEL[basisName(cost.basis)] ?? basisName(cost.basis) : "",
    };
  });
  rows.sort((a, b) => (b.millicu ?? 0) - (a.millicu ?? 0) || a.model.localeCompare(b.model));
  return rows;
}

/** One row per concurrency wave: how much work it holds and what it costs.
 *
 *  This is the shape of the rebuild, not its duration — nothing here measures time. A
 *  plan one item deep and forty wide is a very different operational proposition from
 *  one forty deep and one wide, and this is where that shows. */
export function waveRows(plan) {
  if (!plan.queue) return [];
  const bySeq = new Map(plan.queue.items.map((i) => [i.seq, i]));
  return plan.queue.waves.map((wave, index) => {
    const items = wave.map((s) => bySeq.get(s)).filter(Boolean);
    const millicu = items.reduce((sum, i) => sum + i.millicu, 0);
    return {
      wave: index,
      items: items.length,
      models: [...new Set(items.map((i) => i.model))].sort(),
      millicu,
      cu: cu(millicu),
    };
  });
}

/** One row per work item, in execution order — the queue a runner would drain. */
export function queueRows(plan) {
  if (!plan.queue) return [];
  return plan.queue.items.map((i) => ({
    seq: i.seq,
    model: i.model,
    partition: i.partition ?? "",
    target: i.partition == null ? i.model : `${i.model}[${i.partition}]`,
    wave: i.wave,
    dependsOn: (i.depends_on ?? []).join(", "),
    millicu: i.millicu,
    cu: cu(i.millicu),
  }));
}

/** One row per partition-scoped model: how its data was broken down.
 *
 *  Only models the planner actually scoped appear. A model rebuilding whole is not a
 *  breakdown of one, it is the absence of a breakdown, and listing it here would imply
 *  partition scoping was considered and produced a single chunk. */
export function breakdownRows(plan) {
  return (plan.models ?? [])
    .map((model) => ({ model, partitions: partitionsOf(model) }))
    .filter(({ partitions }) => partitions && partitions.length)
    .map(({ model, partitions }) => ({
      model: model.name,
      partitions: partitions.length,
      first: partitions[0],
      last: partitions[partitions.length - 1],
      keys: partitions.slice(),
    }));
}

/** The headline figures, as a stat row rather than a chart.
 *
 *  `totalCu` is null when cost was not requested rather than 0, so the page can say
 *  "not requested" instead of printing a zero that reads as "free". `avoidedCu` — the
 *  rebuild work pruning saved against rebuilding everything downstream of the change
 *  (contract v5's `unpruned_millicu`) — is null the same way, and also when the plan
 *  came from a producer older than v5, rather than a zero that claims "no saving". */
export function summary(plan) {
  const models = plan.models ?? [];
  const unpruned = plan.cost?.unpruned_millicu;
  const avoidedMillicu =
    plan.cost && typeof unpruned === "number"
      ? Math.max(0, unpruned - plan.cost.total_millicu)
      : null;
  return {
    models: models.length,
    directlyChanged: models.filter(isDirectlyChanged).length,
    downstream: models.filter((m) => !isDirectlyChanged(m)).length,
    totalMillicu: plan.cost ? plan.cost.total_millicu : null,
    totalCu: plan.cost ? cu(plan.cost.total_millicu) : null,
    avoidedMillicu,
    avoidedCu: avoidedMillicu === null ? null : cu(avoidedMillicu),
    workItems: plan.queue ? plan.queue.items.length : null,
    depth: plan.queue ? plan.queue.waves.length : null,
    width: plan.queue ? Math.max(0, ...plan.queue.waves.map((w) => w.length)) : null,
  };
}

/** The `n` costliest models, descending. Ties break by name, so the order is stable. */
export function heaviest(plan, n = 5) {
  return [...(plan.cost?.per_model ?? [])]
    .sort((a, b) => b.millicu - a.millicu || a.model.localeCompare(b.model))
    .slice(0, n);
}

/** The queue as newline-delimited JSON — one work item per line.
 *
 *  NDJSON rather than a JSON array because this is what gets piped into a runner: a
 *  consumer reads it a line at a time without holding the whole plan in memory, and
 *  appending is concatenation. Order is `seq` order, which is dependency-safe to drain
 *  serially. Keys are sorted so two exports of the same plan diff cleanly. */
export function queueNdjson(plan) {
  return queueRows(plan)
    .map((row) =>
      JSON.stringify(
        Object.fromEntries(Object.keys(row).sort().map((k) => [k, row[k]])),
      ),
    )
    .join("\n");
}

/** The two categorical hues for the current mode. */
export const seriesColors = (dark) => (dark ? SERIES.dark : SERIES.light);

/** Fixed category order, so a filter that drops one never repaints the other. */
export const ORIGIN_DOMAIN = Object.freeze(["changed", "downstream"]);

/** Parse a `{table: [columns]}` document, tolerating an empty box. */
export function sourcesFromJson(text) {
  if (!text || !text.trim()) return {};
  const parsed = JSON.parse(text);
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("sources must be a JSON object of {table: [columns]}");
  }
  return parsed;
}
