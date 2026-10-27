//! Lineage soundness, checked against a real SQL engine.
//!
//! The planner's one unforgivable error is an **under-rebuild**: skipping a model
//! whose data changed. Unit tests pin the patterns we thought of; this test asks
//! PostgreSQL. Each case generates a random project — joins, filters, aggregates,
//! `DISTINCT`, `UNION`, import CTEs, scalar and `IN` subqueries, windows,
//! `ORDER BY … LIMIT`, cross joins, set-returning functions — over random data, builds every model, then
//! changes one model and builds everything again. Every model whose rows actually
//! changed must be in the plan the planner computes for that change.
//!
//! Two kinds of change, because they exercise different halves of the planner:
//!
//! * **definition** — a constant anywhere in the model moves (a filter bound, a
//!   `LIMIT`, an expression): its rows may change, so the planner is told *every*
//!   column changed, as `freshet-api` does for a changed fingerprint.
//! * **value-only** — only one projected expression moves, which cannot change the
//!   model's rows: the planner is told *that column* changed. This is the path
//!   column-level pruning takes, and where an unsound pruning shows up.
//!
//! ```sh
//! FRESHET_PG_URL=postgresql://postgres@127.0.0.1:5432/postgres \
//!   cargo test -p freshet-sql --test pg_oracle -- --ignored --nocapture
//! ```
//!
//! `FRESHET_ORACLE_CASES` (default 300) and `FRESHET_ORACLE_SEED` pick the cases.
//! Needs `psql` on `PATH`; each case works in its own schema and drops it.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::process::{Command, Stdio};

use freshet_core::{plan_backfill, Project};
use freshet_sql::{build_project, Catalog};

// ---- a tiny deterministic RNG (no dependency for a test) ----------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn int(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

// ---- the generated project -----------------------------------------------------

/// A relation a model can read: a source or an earlier model. Every relation has
/// integer columns `k` and `v`; some also have `id` and `w`.
#[derive(Clone, Debug)]
struct Rel {
    name: String,
    cols: Vec<&'static str>,
}

/// One model's shape. `a`/`b` index earlier relations; `c` is the constant a
/// mutation moves. `value` marks shapes whose `c` changes one projected value and
/// never the model's rows.
#[derive(Clone, Debug)]
enum Shape {
    Project {
        a: usize,
        c: i64,
    },
    Filter {
        a: usize,
        c: i64,
    },
    Join {
        a: usize,
        b: usize,
        left: bool,
        c: i64,
        bound: i64,
    },
    Semi {
        a: usize,
        b: usize,
        c: i64,
    },
    Agg {
        a: usize,
        c: i64,
        having: Option<i64>,
    },
    Distinct {
        a: usize,
    },
    Union {
        a: usize,
        b: usize,
        all: bool,
        c: i64,
    },
    ImportCte {
        a: usize,
        c: i64,
    },
    Scalar {
        a: usize,
        b: usize,
        c: i64,
    },
    TopN {
        a: usize,
        n: i64,
    },
    Window {
        a: usize,
        c: i64,
    },
    CrossCount {
        a: usize,
        b: usize,
        c: i64,
    },
    /// A set-returning function in the projection: each input row becomes
    /// `abs(v) % 3 + 1` rows, so the model's row count depends on `v`.
    Series {
        a: usize,
        c: i64,
    },
    /// Reads only `k` and the row count: a change that moves rows but not `k` values
    /// reaches it only through the row set.
    KeyCount {
        a: usize,
        c: i64,
    },
    /// Reads only `k`, one row per input row.
    KeysOnly {
        a: usize,
        c: i64,
    },
}

impl Shape {
    fn random(rng: &mut Rng, upto: usize) -> Shape {
        // Half the time, read the relation just built: long chains are where a change
        // has to survive several hops of lineage, and where a lost row set shows up.
        let a = if rng.chance(50) {
            upto - 1
        } else {
            rng.below(upto)
        };
        let b = rng.below(upto);
        let c = rng.int(-3, 8);
        match rng.below(15) {
            0 => Shape::Project { a, c },
            1 => Shape::Filter { a, c },
            2 => Shape::Join {
                a,
                b,
                left: rng.chance(40),
                c,
                bound: rng.int(-1, 9),
            },
            3 => Shape::Semi { a, b, c },
            4 => Shape::Agg {
                a,
                c,
                having: rng.chance(40).then(|| rng.int(0, 20)),
            },
            5 => Shape::Distinct { a },
            6 => Shape::Union {
                a,
                b,
                all: rng.chance(50),
                c,
            },
            7 => Shape::ImportCte { a, c },
            8 => Shape::Scalar { a, b, c },
            9 => Shape::TopN {
                a,
                n: rng.int(1, 6),
            },
            10 => Shape::Window { a, c },
            11 => Shape::CrossCount { a, b, c },
            12 => Shape::Series { a, c },
            13 => Shape::KeyCount { a, c },
            _ => Shape::KeysOnly { a, c },
        }
    }

    fn cols(&self, rels: &[Rel]) -> Vec<&'static str> {
        match self {
            Shape::Project { a, .. } if rels[*a].cols.contains(&"id") => vec!["id", "k", "v", "w"],
            Shape::Filter { a, .. } | Shape::Semi { a, .. } => rels[*a].cols.clone(),
            Shape::Distinct { .. }
            | Shape::Union { .. }
            | Shape::ImportCte { .. }
            | Shape::TopN { .. }
            | Shape::CrossCount { .. }
            | Shape::KeyCount { .. }
            | Shape::KeysOnly { .. } => vec!["k", "v"],
            _ => vec!["k", "v", "w"],
        }
    }

    fn sql(&self, rels: &[Rel]) -> String {
        let n = |i: &usize| rels[*i].name.as_str();
        let list = |i: &usize| rels[*i].cols.join(", ");
        match self {
            Shape::Project { a, c } => {
                let id = if rels[*a].cols.contains(&"id") { "id, " } else { "" };
                format!("select {id}k, v, v + {c} as w from {}", n(a))
            }
            Shape::Filter { a, c } => format!("select {} from {} where v > {c}", list(a), n(a)),
            Shape::Join { a, b, left, c, bound } => format!(
                "select x.k, x.v, {} as w from {} x {} join {} y on x.k = y.k where x.v <= {bound}",
                if *left { format!("coalesce(y.v, 0) + {c}") } else { format!("y.v + {c}") },
                n(a),
                if *left { "left" } else { "inner" },
                n(b),
            ),
            Shape::Semi { a, b, c } => format!(
                "select {} from {} where k in (select k from {} where v > {c})",
                list(a),
                n(a),
                n(b)
            ),
            Shape::Agg { a, c, having } => format!(
                "select k, sum(v) + {c} as v, count(*) as w from {} group by k{}",
                n(a),
                having.map(|h| format!(" having sum(v) > {h}")).unwrap_or_default()
            ),
            Shape::Distinct { a } => format!("select distinct k, v from {}", n(a)),
            Shape::Union { a, b, all, c } => format!(
                "select k, v + {c} as v from {} union{} select k, v from {}",
                n(a),
                if *all { " all" } else { "" },
                n(b)
            ),
            Shape::ImportCte { a, c } => format!(
                "with {0} as (select k, v from {0} where v > {c}) select k, v * 2 as v from {0}",
                n(a)
            ),
            Shape::Scalar { a, b, c } => format!(
                "select k, v, (select max(v) + {c} from {}) as w from {}",
                n(b),
                n(a)
            ),
            Shape::TopN { a, n: limit } => {
                format!("select k, v from {} order by v desc, k limit {limit}", n(a))
            }
            Shape::Window { a, c } => format!(
                "select k, v, row_number() over (partition by k order by v) + {c} as w from {}",
                n(a)
            ),
            Shape::CrossCount { a, b, c } => format!(
                "select x.k, count(*) + {c} as v from {} x cross join {} y where x.v <= y.v group by x.k",
                n(a),
                n(b)
            ),
            Shape::Series { a, c } => format!(
                "select k, v, generate_series(1, abs(v) % 3 + 1) + {c} as w from {}",
                n(a)
            ),
            Shape::KeyCount { a, c } => {
                format!("select k, count(*) + {c} as v from {} group by k", n(a))
            }
            Shape::KeysOnly { a, c } => format!("select k, k * 2 + {c} as v from {}", n(a)),
        }
    }

    /// The output column `c` alone moves, when changing `c` cannot change the rows.
    fn value_column(&self) -> Option<&'static str> {
        match self {
            Shape::Project { .. }
            | Shape::Join { .. }
            | Shape::Scalar { .. }
            | Shape::Window { .. }
            | Shape::Series { .. } => Some("w"),
            Shape::Agg { .. }
            | Shape::CrossCount { .. }
            | Shape::KeyCount { .. }
            | Shape::KeysOnly { .. } => Some("v"),
            Shape::Union { all: true, .. } => Some("v"),
            _ => None,
        }
    }

    /// The same shape with its constant moved.
    fn mutated(&self, rng: &mut Rng) -> Shape {
        let mut s = self.clone();
        let bump =
            |x: &mut i64, rng: &mut Rng| *x += if rng.chance(50) { 1 } else { -1 } * rng.int(1, 3);
        match &mut s {
            Shape::Project { c, .. }
            | Shape::Filter { c, .. }
            | Shape::Semi { c, .. }
            | Shape::Union { c, .. }
            | Shape::ImportCte { c, .. }
            | Shape::Scalar { c, .. }
            | Shape::Window { c, .. }
            | Shape::CrossCount { c, .. }
            | Shape::Series { c, .. }
            | Shape::KeyCount { c, .. }
            | Shape::KeysOnly { c, .. } => bump(c, rng),
            Shape::Join { c, bound, .. } => {
                if rng.chance(50) {
                    bump(c, rng)
                } else {
                    bump(bound, rng)
                }
            }
            Shape::Agg { c, having, .. } => match having {
                Some(h) if rng.chance(50) => bump(h, rng),
                _ => bump(c, rng),
            },
            Shape::TopN { n, .. } => *n = (*n + rng.int(1, 3)).min(9),
            Shape::Distinct { .. } => {}
        }
        s
    }

    /// Moves only `c`, for a value-only change.
    fn with_value_bumped(&self) -> Shape {
        let mut s = self.clone();
        match &mut s {
            Shape::Project { c, .. }
            | Shape::Join { c, .. }
            | Shape::Scalar { c, .. }
            | Shape::Window { c, .. }
            | Shape::Agg { c, .. }
            | Shape::CrossCount { c, .. }
            | Shape::Union { c, .. }
            | Shape::Series { c, .. }
            | Shape::KeyCount { c, .. }
            | Shape::KeysOnly { c, .. } => *c += 5,
            _ => unreachable!("not a value-only shape"),
        }
        s
    }
}

const SOURCES: [(&str, &[&str]); 2] = [
    ("src1", &["id", "k", "v", "w"]),
    ("src2", &["id", "k", "v"]),
];

struct Case {
    shapes: Vec<Shape>,
    data: Vec<String>,
}

impl Case {
    fn random(rng: &mut Rng) -> Case {
        let mut rels: Vec<Rel> = SOURCES
            .iter()
            .map(|(n, cols)| Rel {
                name: n.to_string(),
                cols: cols.to_vec(),
            })
            .collect();
        let models = 3 + rng.below(8);
        let mut shapes = Vec::new();
        for i in 0..models {
            let shape = Shape::random(rng, rels.len());
            rels.push(Rel {
                name: format!("m{i}"),
                cols: shape.cols(&rels),
            });
            shapes.push(shape);
        }
        Case {
            shapes,
            data: random_data(rng),
        }
    }

    fn rels(&self, shapes: &[Shape]) -> Vec<Rel> {
        let mut rels: Vec<Rel> = SOURCES
            .iter()
            .map(|(n, cols)| Rel {
                name: n.to_string(),
                cols: cols.to_vec(),
            })
            .collect();
        for (i, s) in shapes.iter().enumerate() {
            let cols = s.cols(&rels);
            rels.push(Rel {
                name: format!("m{i}"),
                cols,
            });
        }
        rels
    }

    fn models(&self, shapes: &[Shape]) -> Vec<(String, String)> {
        let rels = self.rels(shapes);
        shapes
            .iter()
            .enumerate()
            .map(|(i, s)| (format!("m{i}"), s.sql(&rels[..SOURCES.len() + i])))
            .collect()
    }
}

/// `insert` statements filling the sources with a few random rows each.
fn random_data(rng: &mut Rng) -> Vec<String> {
    let mut data = Vec::new();
    for (name, cols) in SOURCES {
        let rows: Vec<String> = (0..6 + rng.below(8))
            .map(|id| {
                let mut vals = vec![
                    id.to_string(),
                    rng.int(0, 3).to_string(),
                    rng.int(0, 9).to_string(),
                ];
                if cols.len() == 4 {
                    vals.push(rng.int(0, 4).to_string());
                }
                format!("({})", vals.join(", "))
            })
            .collect();
        data.push(format!("insert into {name} values {}", rows.join(", ")));
    }
    data
}

fn sources() -> Vec<Rel> {
    SOURCES
        .iter()
        .map(|(n, cols)| Rel {
            name: n.to_string(),
            cols: cols.to_vec(),
        })
        .collect()
}

// ---- running it ----------------------------------------------------------------

/// Build every model in a fresh schema and return each one's content hash.
fn build_and_hash(
    url: &str,
    schema: &str,
    data: &[String],
    models: &[(String, String)],
) -> BTreeMap<String, String> {
    let mut script = format!(
        "\\set ON_ERROR_STOP 1\nset client_min_messages = warning;\ndrop schema if exists {schema} cascade;\n\
         create schema {schema};\nset search_path = {schema};\n\
         create table src1 (id int, k int, v int, w int);\ncreate table src2 (id int, k int, v int);\n"
    );
    for d in data {
        script.push_str(d);
        script.push_str(";\n");
    }
    for (name, sql) in models {
        script.push_str(&format!("create table {name} as {sql};\n"));
    }
    for (name, _) in models {
        script.push_str(&format!(
            "select '{name}', coalesce(md5(string_agg(t::text, '|' order by t::text)), '-') from {name} t;\n"
        ));
    }
    script.push_str(&format!("drop schema {schema} cascade;\n"));

    let mut child = Command::new("psql")
        .args([url, "-X", "-q", "-A", "-t", "-F", "\t", "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("psql on PATH");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "psql failed:\n{}\n--- script ---\n{script}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| l.split_once('\t'))
        .map(|(m, h)| (m.to_string(), h.to_string()))
        .collect()
}

fn catalog() -> Catalog {
    SOURCES
        .iter()
        .map(|(n, cols)| (n.to_string(), cols.iter().map(|c| c.to_string()).collect()))
        .collect()
}

#[test]
#[ignore = "needs a live PostgreSQL (FRESHET_PG_URL) and psql"]
fn every_model_whose_rows_change_is_in_the_plan() {
    let url = std::env::var("FRESHET_PG_URL").expect("set FRESHET_PG_URL");
    let cases: usize = std::env::var("FRESHET_ORACLE_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let seed: u64 = std::env::var("FRESHET_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5eed_f00d);
    let schema = format!("freshet_oracle_{}", std::process::id());

    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let (mut checked, mut value_only, mut changed_total, mut planned_total) = (0, 0, 0, 0);
    let mut failures: Vec<String> = Vec::new();

    for case_no in 0..cases {
        let case = Case::random(&mut rng);
        let before_models = case.models(&case.shapes);
        let project: Project = build_project(&before_models, &catalog()).unwrap_or_else(|e| {
            panic!("case {case_no}: build_project failed: {e:#}\n{before_models:#?}")
        });

        // Which model changes, and how.
        let target = rng.below(case.shapes.len());
        let name = format!("m{target}");
        let value_col = case.shapes[target]
            .value_column()
            .filter(|_| rng.chance(50));
        let mut after = case.shapes.clone();
        after[target] = match value_col {
            Some(_) => case.shapes[target].with_value_bumped(),
            None => case.shapes[target].mutated(&mut rng),
        };
        let after_models = case.models(&after);

        let before = build_and_hash(&url, &schema, &case.data, &before_models);
        let after_hash = build_and_hash(&url, &schema, &case.data, &after_models);
        let changed: BTreeSet<&String> = before
            .keys()
            .filter(|m| before.get(*m) != after_hash.get(*m))
            .collect();

        let columns: BTreeSet<String> = match value_col {
            Some(c) => BTreeSet::from([c.to_string()]),
            None => project.get(&name).unwrap().output_columns(),
        };
        let plan = plan_backfill(&project, &BTreeMap::from([(name.clone(), columns.clone())]));
        let planned: BTreeSet<&String> = plan.models.iter().collect();

        let missed: Vec<&&String> = changed.iter().filter(|m| !planned.contains(**m)).collect();
        if !missed.is_empty() {
            failures.push(format!(
                "case {case_no}: changed {name} ({}) — rows changed in {missed:?} but the plan was {:?}\n  before: {before_models:#?}\n  after {name}: {}\n",
                match value_col { Some(c) => format!("value-only, column {c}"), None => "definition".into() },
                plan.models,
                after_models[target].1
            ));
        }
        checked += 1;
        value_only += usize::from(value_col.is_some());
        changed_total += changed.len();
        planned_total += planned.len();
    }

    eprintln!(
        "oracle: {checked} cases ({value_only} value-only); models whose rows changed: {changed_total}, \
         models planned: {planned_total} ({:.0}% of planned rebuilds were needed)",
        100.0 * changed_total as f64 / planned_total.max(1) as f64
    );
    assert!(
        failures.is_empty(),
        "{} under-rebuild(s):\n{}",
        failures.len(),
        failures
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// One instance of every shape, reading `a` (and `b` where it reads two relations).
fn every_shape(a: usize, b: usize) -> Vec<Shape> {
    vec![
        Shape::Project { a, c: 2 },
        Shape::Filter { a, c: 3 },
        Shape::Join {
            a,
            b,
            left: false,
            c: 1,
            bound: 6,
        },
        Shape::Join {
            a,
            b,
            left: true,
            c: 1,
            bound: 6,
        },
        Shape::Semi { a, b, c: 4 },
        Shape::Agg {
            a,
            c: 1,
            having: None,
        },
        Shape::Agg {
            a,
            c: 1,
            having: Some(6),
        },
        Shape::Distinct { a },
        Shape::Union {
            a,
            b,
            all: true,
            c: 2,
        },
        Shape::Union {
            a,
            b,
            all: false,
            c: 2,
        },
        Shape::ImportCte { a, c: 3 },
        Shape::Scalar { a, b, c: 1 },
        Shape::TopN { a, n: 3 },
        Shape::Window { a, c: 1 },
        Shape::CrossCount { a, b, c: 1 },
        Shape::Series { a, c: 1 },
        Shape::KeyCount { a, c: 1 },
        Shape::KeysOnly { a, c: 1 },
    ]
}

/// Every three-model chain `src1 → x → y → z` of every shape, its root changed.
///
/// The random test samples; this enumerates. A lineage gap that needs one particular
/// shape feeding another — a set-returning projection read by a model that reads only
/// its key, say — turns up once in thousands of random projects, and here in every
/// run. The root changes both ways (a definition change, and a value-only one where
/// the shape has a value column), so the change crosses two hops of lineage.
#[test]
#[ignore = "needs a live PostgreSQL (FRESHET_PG_URL) and psql"]
fn every_three_model_chain_is_sound() {
    const BATCH: usize = 400;
    let url = std::env::var("FRESHET_PG_URL").expect("set FRESHET_PG_URL");
    let schema = format!("freshet_chains_{}", std::process::id());
    let mut rng = Rng(0x0dd5_eed5_c4a1_7501);
    let data = random_data(&mut rng);

    // (root, changed columns, [before sql; 3], [after sql; 3], names)
    struct Chain {
        names: [String; 3],
        before: [String; 3],
        after: [String; 3],
        root_columns: Option<BTreeSet<String>>,
    }
    let mut chains: Vec<Chain> = Vec::new();
    let shapes = every_shape(0, 0);
    for value_only in [false, true] {
        for (i, x) in shapes.iter().enumerate() {
            let x = with_inputs(x, 0, 1);
            if value_only && x.value_column().is_none() {
                continue;
            }
            for (j, y) in shapes.iter().enumerate() {
                for (k, z) in shapes.iter().enumerate() {
                    let n = chains.len();
                    let names = [
                        format!("c{n}_x{i}"),
                        format!("c{n}_y{j}"),
                        format!("c{n}_z{k}"),
                    ];
                    let chain = [x.clone(), with_inputs(y, 2, 1), with_inputs(z, 3, 1)];
                    let changed_x = if value_only {
                        x.with_value_bumped()
                    } else {
                        x.mutated(&mut rng)
                    };
                    let mutated = [changed_x, chain[1].clone(), chain[2].clone()];
                    chains.push(Chain {
                        before: chain_sql(&names, &chain),
                        after: chain_sql(&names, &mutated),
                        root_columns: value_only
                            .then(|| BTreeSet::from([x.value_column().unwrap().to_string()])),
                        names,
                    });
                }
            }
        }
    }

    let mut failures: Vec<String> = Vec::new();
    let (mut changed_total, mut planned_total) = (0usize, 0usize);
    for batch in chains.chunks(BATCH) {
        let flat = |pick: fn(&Chain) -> &[String; 3]| -> Vec<(String, String)> {
            batch
                .iter()
                .flat_map(|c| c.names.iter().cloned().zip(pick(c).iter().cloned()))
                .collect()
        };
        let before_models = flat(|c| &c.before);
        let after_models = flat(|c| &c.after);
        let project = build_project(&before_models, &catalog()).expect("every chain builds");
        let before = build_and_hash(&url, &schema, &data, &before_models);
        let after = build_and_hash(&url, &schema, &data, &after_models);

        for c in batch {
            let root = &c.names[0];
            let columns = c
                .root_columns
                .clone()
                .unwrap_or_else(|| project.get(root).unwrap().output_columns());
            let plan = plan_backfill(&project, &BTreeMap::from([(root.clone(), columns)]));
            let changed: Vec<&String> = c
                .names
                .iter()
                .filter(|m| before.get(*m) != after.get(*m))
                .collect();
            let missed: Vec<&&String> = changed
                .iter()
                .filter(|m| !plan.models.contains(m))
                .collect();
            if !missed.is_empty() {
                failures.push(format!(
                    "{} ({}): rows changed in {missed:?}, plan {:?}\n  before: {:#?}\n  after root: {}",
                    root,
                    if c.root_columns.is_some() { "value-only" } else { "definition" },
                    plan.models,
                    c.before,
                    c.after[0]
                ));
            }
            changed_total += changed.len();
            planned_total += plan.models.len();
        }
    }

    eprintln!(
        "chains: {} checked; models whose rows changed: {changed_total}, planned: {planned_total}",
        chains.len()
    );
    assert!(
        failures.is_empty(),
        "{} under-rebuild(s):\n{}",
        failures.len(),
        failures
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// The same shape reading relations `a` and `b`.
fn with_inputs(shape: &Shape, a_: usize, b_: usize) -> Shape {
    let mut s = shape.clone();
    match &mut s {
        Shape::Project { a, .. }
        | Shape::Filter { a, .. }
        | Shape::Agg { a, .. }
        | Shape::Distinct { a }
        | Shape::ImportCte { a, .. }
        | Shape::TopN { a, .. }
        | Shape::Window { a, .. }
        | Shape::Series { a, .. }
        | Shape::KeyCount { a, .. }
        | Shape::KeysOnly { a, .. } => *a = a_,
        Shape::Join { a, b, .. }
        | Shape::Semi { a, b, .. }
        | Shape::Union { a, b, .. }
        | Shape::Scalar { a, b, .. }
        | Shape::CrossCount { a, b, .. } => {
            *a = a_;
            *b = b_;
        }
    }
    s
}

/// SQL for a chain: `names[i]` reads `names[i - 1]` (the first reads `src1`).
fn chain_sql(names: &[String; 3], chain: &[Shape; 3]) -> [String; 3] {
    let mut rels = sources();
    let mut sql: Vec<String> = Vec::new();
    for (name, shape) in names.iter().zip(chain) {
        sql.push(shape.sql(&rels));
        let cols = shape.cols(&rels);
        rels.push(Rel {
            name: name.clone(),
            cols,
        });
    }
    [sql[0].clone(), sql[1].clone(), sql[2].clone()]
}
