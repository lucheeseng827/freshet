use super::*;
use freshet_core::{plan_backfill_scoped, BackfillUnit, Column, ColumnRef, Materialization, Model};
use freshet_state::StateSnapshot;

fn col(name: &str, ups: &[(&str, &str)]) -> Column {
    Column::new(name, ups.iter().map(|(m, c)| ColumnRef::new(*m, *c)).collect())
}

fn incr(name: &str, deps: &[&str], cols: Vec<Column>) -> Model {
    Model {
        name: name.into(),
        sql: format!("select * from {name}"),
        materialization: Materialization::Incremental,
        columns: cols,
        depends_on: deps.iter().map(|s| s.to_string()).collect(),
    }
}

fn keys(ks: &[&str]) -> BTreeSet<Partition> {
    ks.iter().map(|k| k.to_string()).collect()
}

fn restate(model: &str, cols: &[&str], parts: &[&str]) -> BTreeMap<String, ChangeSpec> {
    BTreeMap::from([(
        model.to_string(),
        ChangeSpec {
            columns: cols.iter().map(|c| c.to_string()).collect(),
            partitions: PartitionChange::Partitions(keys(parts)),
        },
    )])
}

fn partitions_of(plan: &BackfillPlan, model: &str) -> Option<BTreeSet<Partition>> {
    match plan.unit.get(model)? {
        BackfillUnit::Partitions(ps) => Some(ps.iter().cloned().collect()),
        BackfillUnit::FullModel => None,
    }
}

/// `(model, grain, [(upstream, window)])`.
type ModelEntry<'a> = (&'a str, Option<Grain>, &'a [(&'a str, u32)]);

fn cfg(entries: &[ModelEntry<'_>]) -> IncrementalConfig {
    IncrementalConfig {
        models: entries
            .iter()
            .map(|(m, g, edges)| {
                (
                    m.to_string(),
                    ModelPartitionConfig {
                        grain: *g,
                        edges: edges.iter().map(|(u, w)| (u.to_string(), EdgePolicy { window: *w })).collect(),
                        replace: None,
                    },
                )
            })
            .collect(),
    }
}

fn chain() -> Project {
    Project::from_models([
        incr("raw", &[], vec![col("a", &[])]),
        incr("mart", &["raw"], vec![col("a", &[("raw", "a")])]),
    ])
}

// --- the domain ------------------------------------------------------------------

#[test]
fn every_grain_round_trips_its_canonical_keys() {
    let cases: &[(Grain, &[&str])] = &[
        (Grain::Hour, &["1970-01-01T00", "2026-06-20T13", "2024-02-29T23", "1969-12-31T23"]),
        (Grain::Day, &["1970-01-01", "2026-06-23", "2000-02-29", "1999-12-31"]),
        (Grain::Week, &["2026-W25", "2020-W53", "2021-W01", "1970-W01", "2015-W53"]),
        (Grain::Month, &["2026-06", "1970-01", "1969-12", "2000-02"]),
        (Grain::Integer { step: 1000 }, &["0", "3000", "-2000"]),
    ];
    for (grain, ks) in cases {
        let d = CalendarDomain::new(*grain);
        for k in *ks {
            let n = d.ordinal(&k.to_string()).unwrap_or_else(|| panic!("{grain:?} rejected {k}"));
            assert_eq!(d.partition_at(n), *k, "{grain:?} round trip");
            assert_eq!(d.ordinal(&d.partition_at(n + 1)), Some(n + 1), "{grain:?} successor of {k}");
        }
    }
}

#[test]
fn hour_keys_accept_the_common_spellings_but_only_whole_hours() {
    let h = CalendarDomain::new(Grain::Hour);
    let canonical = h.ordinal(&"2026-06-20T13".into());
    for alt in ["2026-06-20 13", "2026-06-20T13:00", "2026-06-20 13:00:00"] {
        assert_eq!(h.ordinal(&alt.into()), canonical, "{alt}");
    }
    for bad in ["2026-06-20T13:30", "2026-06-20T24", "2026-06-20", "2026-06-20T1", "2026-06-20Té3"] {
        assert_eq!(h.ordinal(&bad.into()), None, "{bad} names no whole hour");
    }
}

#[test]
fn iso_weeks_belong_to_the_year_of_their_thursday() {
    let w = CalendarDomain::new(Grain::Week);
    // 2021-01-01 was a Friday, so it sits in 2020's week 53; 2021 has no week 53.
    assert!(w.ordinal(&"2020-W53".into()).is_some());
    assert_eq!(w.ordinal(&"2021-W53".into()), None);
    assert_eq!(w.ordinal(&"2026-W00".into()), None);
    // The day 2021-01-01 is covered by 2020-W53.
    let day = CalendarDomain::day();
    let span = day.span(&"2021-01-01".into()).unwrap();
    assert_eq!(w.keys_covering(span, 10), Some(vec!["2020-W53".to_string()]));
}

#[test]
fn impossible_dates_and_non_keys_are_refused_not_rolled_over() {
    let d = CalendarDomain::day();
    for bad in ["2026-02-30", "2026-13-01", "not-a-date", "", "2026-6"] {
        assert_eq!(d.ordinal(&bad.into()), None, "{bad}");
    }
    assert_eq!(CalendarDomain::new(Grain::Month).ordinal(&"2026-13".into()), None);
    assert_eq!(CalendarDomain::new(Grain::Integer { step: 0 }).ordinal(&"5".into()), None, "a step of 0 names no buckets");
}

/// A year no calendar key means is refused before any ordinal arithmetic, so it can
/// neither overflow (a panic in a debug build) nor wrap onto another partition.
#[test]
fn an_absurd_year_is_refused_before_it_can_overflow() {
    let huge = "768614336404564651";
    for (grain, key) in [
        (Grain::Month, format!("{huge}-01")),
        (Grain::Day, format!("{huge}-01-01")),
        (Grain::Week, format!("{huge}-W01")),
        (Grain::Month, format!("{}-01", i64::MAX)),
        (Grain::Day, "10000-01-01".to_string()),
    ] {
        let d = CalendarDomain::new(grain);
        assert_eq!(d.ordinal(&key), None, "{grain:?} {key}");
        assert_eq!(d.normalize(&key), None, "{grain:?} {key}");
    }
    // The edges of the range still work.
    assert!(CalendarDomain::day().ordinal(&"9999-12-31".into()).is_some());
    assert!(CalendarDomain::new(Grain::Month).ordinal(&"0001-01".into()).is_some());
}

#[test]
fn lookback_reads_backwards_and_a_change_affects_forwards() {
    let d = CalendarDomain::day();
    assert_eq!(d.lookback_set(&"2026-03-01".into(), 3), Some(keys(&["2026-02-27", "2026-02-28", "2026-03-01"])));
    assert_eq!(d.affected_by(&"2026-02-27".into(), 3), Some(keys(&["2026-02-27", "2026-02-28", "2026-03-01"])));
    assert_eq!(d.affected_by(&"2026-06-20".into(), 1), Some(keys(&["2026-06-20"])), "window 1 is identity");
}

#[test]
fn a_day_maps_to_its_month_and_a_month_to_its_days() {
    let (day, month) = (CalendarDomain::day(), CalendarDomain::new(Grain::Month));
    assert_eq!(map_across_grains(&day, &month, &"2026-02-14".into(), 100), Some(vec!["2026-02".to_string()]));
    let days = map_across_grains(&month, &day, &"2024-02".into(), 100).unwrap();
    assert_eq!(days.len(), 29, "leap February");
    assert_eq!((days.first().unwrap().as_str(), days.last().unwrap().as_str()), ("2024-02-01", "2024-02-29"));
}

#[test]
fn a_week_that_straddles_two_months_maps_to_both() {
    // 2026-W27 runs Mon 2026-06-29 .. Sun 2026-07-05.
    let (week, month) = (CalendarDomain::new(Grain::Week), CalendarDomain::new(Grain::Month));
    assert_eq!(map_across_grains(&week, &month, &"2026-W27".into(), 10), Some(vec!["2026-06".into(), "2026-07".into()]));
}

#[test]
fn grains_with_nothing_in_common_and_oversized_refinements_are_refused() {
    let (day, ints) = (CalendarDomain::day(), CalendarDomain::new(Grain::Integer { step: 10 }));
    assert_eq!(map_across_grains(&day, &ints, &"2026-06-20".into(), 100), None, "a day has no integer bucket");
    let (coarse, fine) = (CalendarDomain::new(Grain::Integer { step: 1_000_000 }), CalendarDomain::new(Grain::Integer { step: 1 }));
    assert_eq!(map_across_grains(&coarse, &fine, &"0".into(), 10_000), None, "a million keys is a full rebuild");
    assert_eq!(map_across_grains(&fine, &coarse, &"123".into(), 10), Some(vec!["0".to_string()]));
}

// --- planning ------------------------------------------------------------------------

#[test]
fn with_nothing_declared_the_plan_is_byte_identical_to_v1() {
    let project = chain();
    // Deliberately non-canonical key: v1 passes keys through untouched, and so must we.
    let seed = restate("raw", &["a"], &["2026-6-5", "not-a-date"]);
    let v1 = plan_backfill_scoped(&project, &seed);
    let v2 = plan_incremental(&project, &IncrementalConfig::default(), &seed, Grain::Day);
    assert_eq!(v1.models, v2.models);
    assert_eq!(v1.unit, v2.unit);
    assert_eq!(v1.reasons, v2.reasons);
}

#[test]
fn a_trailing_window_rebuilds_the_following_partitions_not_the_preceding_ones() {
    // mart is a 3-day rolling window over raw: mart[d] reads raw[d-2 ..= d].
    let c = cfg(&[("mart", Some(Grain::Day), &[("raw", 3)])]);
    let plan = plan_incremental(&chain(), &c, &restate("raw", &["a"], &["2026-06-20"]), Grain::Day);
    assert_eq!(
        partitions_of(&plan, "mart"),
        Some(keys(&["2026-06-20", "2026-06-21", "2026-06-22"])),
        "raw 06-20 is inside the windows ending 06-20, 06-21 and 06-22 — and no earlier one"
    );
}

#[test]
fn a_daily_upstream_rebuilds_the_monthly_partition_that_contains_it() {
    let c = cfg(&[("raw", Some(Grain::Day), &[]), ("mart", Some(Grain::Month), &[])]);
    let plan = plan_incremental(&chain(), &c, &restate("raw", &["a"], &["2026-06-20", "2026-07-01"]), Grain::Day);
    assert_eq!(partitions_of(&plan, "mart"), Some(keys(&["2026-06", "2026-07"])));
}

#[test]
fn an_undeclared_side_inherits_the_declared_grain() {
    let c = cfg(&[("raw", Some(Grain::Month), &[])]);
    let plan = plan_incremental(&chain(), &c, &restate("raw", &["a"], &["2026-6"]), Grain::Day);
    assert_eq!(partitions_of(&plan, "raw"), Some(keys(&["2026-06"])), "seed keys are canonicalised in a declared grain");
    assert_eq!(partitions_of(&plan, "mart"), Some(keys(&["2026-06"])));
}

#[test]
fn a_key_the_grain_cannot_place_escalates_the_downstream_to_full() {
    let c = cfg(&[("mart", Some(Grain::Day), &[("raw", 2)])]);
    let plan = plan_incremental(&chain(), &c, &restate("raw", &["a"], &["2026-02-30"]), Grain::Day);
    assert_eq!(plan.unit["mart"], BackfillUnit::FullModel, "an impossible date widens; it never vanishes");
}

#[test]
fn an_oversized_refinement_escalates_to_full() {
    let c = cfg(&[("raw", Some(Grain::Integer { step: 1_000_000 }), &[]), ("mart", Some(Grain::Integer { step: 1 }), &[])]);
    let plan = plan_incremental(&chain(), &c, &restate("raw", &["a"], &["0"]), Grain::Day);
    assert_eq!(plan.unit["mart"], BackfillUnit::FullModel);
}

#[test]
fn column_pruning_still_applies_through_windows_and_grains() {
    // The scaffold pre-seeded every downstream as directly changed, which switched
    // this off for every incremental plan. It must survive the mapper.
    let project = Project::from_models([
        incr("raw", &[], vec![col("a", &[]), col("b", &[])]),
        incr("reads_a", &["raw"], vec![col("a", &[("raw", "a")])]),
        incr("reads_b", &["raw"], vec![col("b", &[("raw", "b")])]),
    ]);
    let c = cfg(&[("reads_a", Some(Grain::Month), &[("raw", 2)]), ("reads_b", Some(Grain::Month), &[("raw", 2)]), ("raw", Some(Grain::Day), &[])]);
    let plan = plan_incremental(&project, &c, &restate("raw", &["a"], &["2026-06-20"]), Grain::Day);
    assert_eq!(partitions_of(&plan, "reads_a"), Some(keys(&["2026-06", "2026-07"])));
    assert!(!plan.models.contains(&"reads_b".to_string()), "reads_b reads only raw.b, which did not change");
    assert!(
        matches!(plan.reasons["reads_a"], freshet_core::BackfillReason::Downstream { .. }),
        "attribution survives too: reads_a is downstream of raw, not directly changed"
    );
}

// --- properties -------------------------------------------------------------------------

/// A small deterministic PRNG (no dependency, reproducible failures).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Every hour a set of keys covers, as one interval set.
fn hours_of(d: &CalendarDomain, ks: &BTreeSet<Partition>) -> IntervalSet {
    let mut set = IntervalSet::new();
    for k in ks {
        match d.span(k).unwrap() {
            Span::Hours(lo, hi) => set.insert(lo, hi),
            Span::Ints(..) => unreachable!("calendar grains only"),
        }
    }
    set
}

fn overlaps(set: &IntervalSet, lo: i64, hi: i64) -> bool {
    set.ranges().iter().any(|&(a, b)| a < hi && lo < b)
}

/// The oracle, computed the other way round from the mapper: enumerate every
/// candidate downstream partition and ask whether the hours its trailing window
/// reads overlap the hours that changed upstream. Nothing here maps a key forward.
fn stale_by_brute_force(down: &CalendarDomain, window: u32, changed: &IntervalSet) -> BTreeSet<Partition> {
    let (lo, hi) = (changed.ranges().first().unwrap().0, changed.ranges().last().unwrap().1);
    // Any stale partition starts before the changed hours end and ends, after its
    // window, no earlier than they begin: a generous candidate range around both.
    let pad = 24 * 31 * 6;
    let first = down.keys_covering(Span::Hours(lo - pad, lo - pad + 1), 1).unwrap()[0].clone();
    let mut out = BTreeSet::new();
    let mut n = down.ordinal(&first).unwrap();
    loop {
        let d = down.partition_at(n);
        let Span::Hours(start, end) = down.span(&d).unwrap() else { unreachable!() };
        if start > hi + pad {
            break;
        }
        let Span::Hours(read_lo, _) = down.span(&down.partition_at(n - (i64::from(window) - 1))).unwrap() else {
            unreachable!()
        };
        if overlaps(changed, read_lo, end) {
            out.insert(d);
        }
        n += 1;
    }
    out
}

#[test]
fn a_chain_of_mixed_grains_and_windows_rebuilds_exactly_what_the_oracle_says() {
    let grains = [Grain::Hour, Grain::Day, Grain::Week, Grain::Month];
    let mut rng = Rng(0x5eed_f7e5);
    for case in 0..250 {
        let len = 2 + rng.below(3) as usize;
        let names: Vec<String> = (0..len).map(|i| format!("m{i}")).collect();
        // Hours only at the head, so refinements stay far below the cap.
        let model_grains: Vec<Grain> = (0..len)
            .map(|i| if i == 0 { grains[rng.below(4) as usize] } else { grains[1 + rng.below(3) as usize] })
            .collect();
        let windows: Vec<u32> = (0..len).map(|_| 1 + rng.below(4) as u32).collect();
        let project = Project::from_models(names.iter().enumerate().map(|(i, n)| {
            let deps: Vec<&str> = if i == 0 { vec![] } else { vec![names[i - 1].as_str()] };
            let up_col: Vec<(&str, &str)> = if i == 0 { vec![] } else { vec![(names[i - 1].as_str(), "a")] };
            incr(n, &deps, vec![col("a", &up_col)])
        }));
        let c = IncrementalConfig {
            models: names
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    let edges = if i == 0 { BTreeMap::new() } else { BTreeMap::from([(names[i - 1].clone(), EdgePolicy { window: windows[i] })]) };
                    (n.clone(), ModelPartitionConfig { grain: Some(model_grains[i]), edges, replace: None })
                })
                .collect(),
        };
        // A few keys of m0's grain, from the key holding hour 494,000 (May 2026) on.
        let head = CalendarDomain::new(model_grains[0]);
        let first = head.keys_covering(Span::Hours(494_000, 494_001), 1).unwrap().remove(0);
        let base = head.ordinal(&first).unwrap();
        let seed_keys: BTreeSet<Partition> = (0..1 + rng.below(3)).map(|_| head.partition_at(base + rng.below(40) as i64)).collect();
        let seed = BTreeMap::from([(
            names[0].clone(),
            ChangeSpec { columns: BTreeSet::from(["a".to_string()]), partitions: PartitionChange::Partitions(seed_keys.clone()) },
        )]);

        let plan = plan_incremental(&project, &c, &seed, Grain::Day);

        let mut truth = seed_keys;
        for i in 1..len {
            let (up, down) = (CalendarDomain::new(model_grains[i - 1]), CalendarDomain::new(model_grains[i]));
            let expected = stale_by_brute_force(&down, windows[i], &hours_of(&up, &truth));
            let got = partitions_of(&plan, &names[i]).unwrap_or_else(|| panic!("case {case}: {} widened to full", names[i]));
            assert_eq!(
                got, expected,
                "case {case}: {} ({:?}, window {}) under {:?}",
                names[i], model_grains[i], windows[i], model_grains[i - 1]
            );
            truth = expected;
        }
    }
}

#[test]
fn windows_only_ever_add_to_what_v1_rebuilds() {
    // v1 result ⊆ v2 result for any windows on a shared grain.
    let mut rng = Rng(7);
    for _ in 0..200 {
        let project = Project::from_models([
            incr("a", &[], vec![col("x", &[])]),
            incr("b", &["a"], vec![col("x", &[("a", "x")])]),
            incr("c", &["b"], vec![col("x", &[("b", "x")])]),
        ]);
        let c = cfg(&[
            ("b", Some(Grain::Day), &[("a", 1 + rng.below(5) as u32)]),
            ("c", Some(Grain::Day), &[("b", 1 + rng.below(5) as u32)]),
            ("a", Some(Grain::Day), &[]),
        ]);
        let d = CalendarDomain::day();
        let start = d.ordinal(&"2026-06-01".into()).unwrap();
        let ks: Vec<String> = (0..1 + rng.below(4)).map(|_| d.partition_at(start + rng.below(60) as i64)).collect();
        let seed = restate("a", &["x"], &ks.iter().map(String::as_str).collect::<Vec<_>>());
        let v1 = plan_backfill_scoped(&project, &seed);
        let v2 = plan_incremental(&project, &c, &seed, Grain::Day);
        for m in ["a", "b", "c"] {
            let (Some(small), Some(big)) = (partitions_of(&v1, m), partitions_of(&v2, m)) else { panic!("{m} widened") };
            assert!(small.is_subset(&big), "{m}: v1 {small:?} ⊄ v2 {big:?}");
        }
    }
}

// --- the ledger -------------------------------------------------------------------------

#[test]
fn an_interval_set_coalesces_what_touches_and_splits_what_it_removes() {
    let mut s = IntervalSet::new();
    s.insert(5, 7);
    s.insert(1, 3);
    s.insert(3, 5);
    assert_eq!(s.ranges(), [(1, 7)], "touching ranges are one run");
    s.remove(3, 4);
    assert_eq!(s.ranges(), [(1, 3), (4, 7)], "the tombstone splits");
    assert_eq!(s.count(), 5);
    assert!(s.contains(4) && !s.contains(3) && !s.contains(7));
    s.remove(0, 100);
    assert!(s.is_empty());
}

#[test]
fn a_ledger_stores_ranges_not_keys() {
    let mut wm = Watermark::new(Grain::Day);
    wm.mark_pending(&keys(&["2026-06-20", "2026-06-21", "2026-06-22", "2026-07-01"]));
    assert_eq!(
        wm.pending,
        [("2026-06-20".to_string(), "2026-06-23".to_string()), ("2026-07-01".to_string(), "2026-07-02".to_string())],
        "half-open: three days as one range, and the lone day as its own"
    );
    wm.mark_built(&keys(&["2026-06-21"]));
    assert_eq!(wm.owed(100), Some(PartitionChange::Partitions(keys(&["2026-06-20", "2026-06-22", "2026-07-01"]))));
    assert_eq!(wm.built, [("2026-06-21".to_string(), "2026-06-22".to_string())]);
}

#[test]
fn a_restatement_that_dies_is_recovered_by_the_next_plan_after_a_restart() {
    let project = chain();
    let c = IncrementalConfig::default();
    let restated = plan_incremental(&project, &c, &restate("raw", &["a"], &["2026-06-20"]), Grain::Day);

    // Plan the restatement: record it as owed, then "crash" — the state survives only
    // as the JSON a FileStateBackend would have written.
    let mut snap = StateSnapshot::default();
    record_pending(&mut snap, &c, Grain::Day, &restated);
    let on_disk = serde_json::to_string(&snap).unwrap();

    // Restart. Nothing in the project changed, so a plan without the ledger is empty.
    let reloaded: StateSnapshot = serde_json::from_str(&on_disk).unwrap();
    let mut seed = BTreeMap::new();
    recover_gaps(&project, &reloaded, DEFAULT_PARTITION_CAP, &mut seed);
    let replan = plan_incremental(&project, &c, &seed, Grain::Day);
    assert_eq!(partitions_of(&replan, "raw"), Some(keys(&["2026-06-20"])));
    assert_eq!(partitions_of(&replan, "mart"), Some(keys(&["2026-06-20"])), "and its downstream with it");
    assert_eq!(seed["raw"].columns, BTreeSet::from(["a".to_string()]), "recovered with every column, so pruning cannot skip readers");

    // The rerun succeeds: settled, and the next plan owes nothing.
    let mut snap = reloaded;
    record_built(&mut snap, &c, Grain::Day, &replan);
    let mut seed = BTreeMap::new();
    recover_gaps(&project, &snap, DEFAULT_PARTITION_CAP, &mut seed);
    assert!(seed.is_empty(), "nothing owed after a successful run: {seed:?}");
    assert_eq!(read_watermark(&snap, "raw").unwrap().built, [("2026-06-20".to_string(), "2026-06-21".to_string())]);
}

#[test]
fn an_unreadable_ledger_owes_a_full_rebuild_and_heals_after_one() {
    let project = chain();
    let mut snap = StateSnapshot::default();
    snap.watermarks.insert("raw".into(), "{not json".into());
    snap.watermarks.insert("mart".into(), r#"{"high":"2026-06-20","gaps":[]}"#.into()); // a pre-ledger shape
    let mut seed = BTreeMap::new();
    recover_gaps(&project, &snap, DEFAULT_PARTITION_CAP, &mut seed);
    assert_eq!(seed["raw"].partitions, PartitionChange::Full, "garbage is never read as 'nothing owed'");
    assert_eq!(seed["mart"].partitions, PartitionChange::Full);

    let plan = plan_incremental(&project, &IncrementalConfig::default(), &seed, Grain::Day);
    record_built(&mut snap, &IncrementalConfig::default(), Grain::Day, &plan);
    let mut seed = BTreeMap::new();
    recover_gaps(&project, &snap, DEFAULT_PARTITION_CAP, &mut seed);
    assert!(seed.is_empty(), "one full rebuild writes a clean ledger");
    assert_eq!(read_watermark(&snap, "raw").map(|w| w.v), Some(WATERMARK_VERSION));
}

#[test]
fn a_key_the_ledger_cannot_place_owes_the_whole_model() {
    let mut wm = Watermark::new(Grain::Day);
    wm.mark_pending(&keys(&["2026-06-20", "tuesday"]));
    assert_eq!(wm.owed(100), Some(PartitionChange::Full));
}

#[test]
fn too_much_owed_is_owed_as_a_full_rebuild() {
    let mut wm = Watermark::new(Grain::Hour);
    let d = CalendarDomain::new(Grain::Hour);
    let ks: BTreeSet<Partition> = (0..50).map(|n| d.partition_at(n)).collect();
    wm.mark_pending(&ks);
    assert_eq!(wm.owed(10), Some(PartitionChange::Full));
    assert!(matches!(wm.owed(100), Some(PartitionChange::Partitions(p)) if p.len() == 50));
}

#[test]
fn a_ledger_written_by_an_unknown_version_is_not_trusted() {
    let mut snap = StateSnapshot::default();
    let mut wm = Watermark::new(Grain::Day);
    wm.v = WATERMARK_VERSION + 1;
    write_watermark(&mut snap, "raw", &wm);
    assert_eq!(read_watermark(&snap, "raw"), None);
}
