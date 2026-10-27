//! The ordered partition domain: what a partition key *is*.
//!
//! A plan carries partition keys as strings (`2026-06-20`), which is enough to say
//! *which* partitions rebuild but not enough to reason about them: to widen a key by
//! a trailing window you need its neighbours, and to carry a changed *day* into a
//! *monthly* model you need to know which month contains it. This module gives each
//! [`Grain`] a total order ([`PartitionDomain::ordinal`]) and — for mapping between
//! grains — the stretch of time or integer space a key covers ([`Span`]).
//!
//! Every function here answers `None` when it cannot place a key, and every caller
//! turns `None` into a full rebuild. That is the whole safety story: an unparseable
//! key, an impossible date, a grain pair with nothing in common, or a mapping that
//! would enumerate an absurd number of keys all widen — never narrow, never guess.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A partition key. A plain string, matching freshet-core's
/// [`BackfillUnit::Partitions`](freshet_core::BackfillUnit::Partitions).
pub type Partition = String;

/// The grain a model is partitioned at, and so the spelling of its keys.
///
/// | grain | canonical key | also accepted |
/// |---|---|---|
/// | `Hour` | `2026-06-20T13` | `2026-06-20 13`, with `:00` or `:00:00` |
/// | `Day` | `2026-06-20` | — |
/// | `Week` | `2026-W25` (ISO 8601 week) | — |
/// | `Month` | `2026-06` | — |
/// | `Integer { step }` | `3000000` (the bucket's start) | any value inside the bucket |
///
/// All calendar grains are UTC and proleptic Gregorian; there are no time zones in a
/// partition key, so there are none here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Grain {
    Hour,
    Day,
    Week,
    Month,
    /// Integer-ranged partitions in fixed steps (e.g. id buckets of 1_000_000).
    Integer { step: i64 },
}

/// The stretch a partition key covers, half-open: `[lo, hi)`.
///
/// Two universes, deliberately incomparable: every calendar grain is measured in
/// hours since 1970-01-01T00, every integer grain in its own values. Mapping a day
/// onto an id bucket has no meaning, and [`PartitionDomain::keys_covering`] says so
/// with `None` rather than inventing one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Span {
    Hours(i64, i64),
    Ints(i64, i64),
}

/// A total order over one grain's keys.
///
/// `ordinal` returning `None` is the universal "I can't reason about this key" signal
/// and forces the caller to escalate to a full rebuild — never to silently drop the
/// key. Implement it for a custom domain; the span methods default to `None`, which
/// makes every cross-grain mapping out of that domain widen to a full rebuild.
pub trait PartitionDomain {
    /// Position of `p` in the total order, or `None` if `p` is not a valid key.
    /// The inverse direction is [`partition_at`](Self::partition_at).
    fn ordinal(&self, p: &Partition) -> Option<i64>;
    /// The key at position `n`: the inverse of [`ordinal`](Self::ordinal).
    ///
    /// Takes `&self` because it is an inverse *on this domain* (the day domain and an
    /// `Integer { step }` domain map the same `n` to different keys), not a
    /// constructor — hence `partition_at` rather than a `from_*` name.
    fn partition_at(&self, n: i64) -> Partition;

    /// Canonical form of `p` (round-trip through the ordinal), or `None` if invalid.
    fn normalize(&self, p: &Partition) -> Option<Partition> {
        self.ordinal(p).map(|n| self.partition_at(n))
    }

    /// The next key after `p`.
    fn next(&self, p: &Partition) -> Option<Partition> {
        self.ordinal(p).map(|n| self.partition_at(n + 1))
    }

    /// The trailing-N **read window** of `p`: `{ p-(N-1) ..= p }`, the partitions a
    /// model with an N-partition lookback *reads* to build `p`. A `window` of 1 is
    /// `{ p }`. `None` if `p` is invalid.
    ///
    /// Note the direction. This is what building `p` reads — *not* what a change to
    /// `p` affects, which runs the other way (`p ..= p+(N-1)`, see
    /// [`affected_by`](Self::affected_by)). Confusing the two under-rebuilds: an
    /// earlier revision of this crate widened a restated day *backwards* and left the
    /// following days' rolling sums stale.
    fn lookback_set(&self, p: &Partition, window: u32) -> Option<BTreeSet<Partition>> {
        let n = self.ordinal(p)?;
        let lo = n - (i64::from(window.max(1)) - 1);
        Some((lo..=n).map(|k| self.partition_at(k)).collect())
    }

    /// The partitions whose trailing-N read window contains `p`: `{ p ..= p+(N-1) }`.
    /// A change to `p` makes exactly these stale in a model that looks back N
    /// partitions. `None` if `p` is invalid.
    fn affected_by(&self, p: &Partition, window: u32) -> Option<BTreeSet<Partition>> {
        let n = self.ordinal(p)?;
        let hi = n + (i64::from(window.max(1)) - 1);
        Some((n..=hi).map(|k| self.partition_at(k)).collect())
    }

    /// The stretch `p` covers, for mapping between grains. `None` if `p` is invalid
    /// or the domain cannot say.
    fn span(&self, _p: &Partition) -> Option<Span> {
        None
    }

    /// Every key of this domain whose span intersects `span`, or `None` if the span
    /// is in another universe or would take more than `cap` keys to cover.
    fn keys_covering(&self, _span: Span, _cap: usize) -> Option<Vec<Partition>> {
        None
    }
}

/// The concrete calendar/integer domain for a [`Grain`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CalendarDomain {
    pub grain: Grain,
}

impl CalendarDomain {
    pub fn new(grain: Grain) -> Self {
        Self { grain }
    }

    /// A `YYYY-MM-DD` day domain — the common case.
    pub fn day() -> Self {
        Self::new(Grain::Day)
    }

    /// The ordinal of the key containing hour `h` (hours since the epoch).
    fn ordinal_of_hour(&self, h: i64) -> Option<i64> {
        let day = h.div_euclid(24);
        match self.grain {
            Grain::Hour => Some(h),
            Grain::Day => Some(day),
            Grain::Week => Some(week_ordinal_of_monday(monday_of(day))),
            Grain::Month => {
                let (y, m, _) = civil_from_days(day);
                Some(y * 12 + (m - 1))
            }
            Grain::Integer { .. } => None,
        }
    }
}

impl PartitionDomain for CalendarDomain {
    fn ordinal(&self, p: &Partition) -> Option<i64> {
        let p = p.trim();
        match self.grain {
            Grain::Hour => parse_hour(p),
            Grain::Day => parse_ymd(p).map(|(y, m, d)| days_from_civil(y, m, d)),
            Grain::Week => parse_iso_week(p),
            Grain::Month => parse_ym(p).map(|(y, m)| y * 12 + (m - 1)),
            Grain::Integer { step } if step > 0 => p.parse::<i64>().ok().map(|v| v.div_euclid(step)),
            // A non-positive step names no buckets at all.
            Grain::Integer { .. } => None,
        }
    }

    fn partition_at(&self, n: i64) -> Partition {
        match self.grain {
            Grain::Hour => {
                let (y, m, d) = civil_from_days(n.div_euclid(24));
                format!("{y:04}-{m:02}-{d:02}T{:02}", n.rem_euclid(24))
            }
            Grain::Day => {
                let (y, m, d) = civil_from_days(n);
                format!("{y:04}-{m:02}-{d:02}")
            }
            Grain::Week => {
                let (y, w) = iso_week_of_monday(monday_of_week_ordinal(n));
                format!("{y:04}-W{w:02}")
            }
            Grain::Month => format!("{:04}-{:02}", n.div_euclid(12), n.rem_euclid(12) + 1),
            Grain::Integer { step } if step > 0 => n.saturating_mul(step).to_string(),
            Grain::Integer { .. } => n.to_string(),
        }
    }

    fn span(&self, p: &Partition) -> Option<Span> {
        let n = self.ordinal(p)?;
        Some(match self.grain {
            Grain::Hour => Span::Hours(n, n + 1),
            Grain::Day => Span::Hours(n * 24, (n + 1) * 24),
            Grain::Week => {
                let monday = monday_of_week_ordinal(n);
                Span::Hours(monday * 24, (monday + 7) * 24)
            }
            Grain::Month => {
                let (y, m) = (n.div_euclid(12), n.rem_euclid(12) + 1);
                let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
                Span::Hours(days_from_civil(y, m, 1) * 24, days_from_civil(ny, nm, 1) * 24)
            }
            Grain::Integer { step } => {
                let lo = n.checked_mul(step)?;
                Span::Ints(lo, lo.checked_add(step)?)
            }
        })
    }

    fn keys_covering(&self, span: Span, cap: usize) -> Option<Vec<Partition>> {
        let (first, last) = match (self.grain, span) {
            (Grain::Integer { step }, Span::Ints(lo, hi)) if step > 0 && lo < hi => {
                (lo.div_euclid(step), (hi - 1).div_euclid(step))
            }
            (Grain::Integer { .. }, _) | (_, Span::Ints(..)) => return None,
            (_, Span::Hours(lo, hi)) if lo < hi => (self.ordinal_of_hour(lo)?, self.ordinal_of_hour(hi - 1)?),
            (_, Span::Hours(..)) => return None,
        };
        let count = usize::try_from(last.checked_sub(first)?.checked_add(1)?).ok()?;
        if count > cap {
            return None;
        }
        Some((first..=last).map(|n| self.partition_at(n)).collect())
    }
}

/// Map `key`, a partition of the `up` domain, to every partition of the `down`
/// domain it overlaps: a day into its month, a month into its days, an hour into its
/// day. `None` — widen to a full rebuild — when the key is invalid, the domains share
/// no universe, or covering it would take more than `cap` keys (a month refined into
/// hours is 744; a coarse integer bucket refined into fine ones can be millions).
pub fn map_across_grains(
    up: &dyn PartitionDomain,
    down: &dyn PartitionDomain,
    key: &Partition,
    cap: usize,
) -> Option<Vec<Partition>> {
    down.keys_covering(up.span(key)?, cap)
}

// ---------------------------------------------------------------------------
// Parsing and calendar arithmetic
// ---------------------------------------------------------------------------

/// The years a calendar key may name. A key is user input, and the ordinal
/// arithmetic below (`y * 12`, `days_from_civil`) is unchecked, so a key naming an
/// absurd year is refused before it can overflow — a panic in a debug build, and in
/// a release build a wrapped ordinal that aliases some other partition.
const YEARS: std::ops::RangeInclusive<i64> = 0..=9999;

pub(crate) fn parse_ymd(p: &str) -> Option<(i64, i64, i64)> {
    let mut it = p.splitn(3, '-');
    let y = it.next()?.parse::<i64>().ok()?;
    let m = it.next()?.parse::<i64>().ok()?;
    let d = it.next()?.parse::<i64>().ok()?;
    if !YEARS.contains(&y) || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    // Reject *impossible* dates (e.g. 2026-02-30): days_from_civil would otherwise
    // silently roll them forward to a different valid day, aliasing one partition to
    // another. An unparseable key must escalate to Full, never alias.
    if civil_from_days(days_from_civil(y, m, d)) == (y, m, d) {
        Some((y, m, d))
    } else {
        None
    }
}

/// `YYYY-MM-DDTHH`, `YYYY-MM-DD HH`, optionally followed by `:00` or `:00:00`. A key
/// with non-zero minutes names part of an hour, not an hour, and is refused.
fn parse_hour(p: &str) -> Option<i64> {
    // `get` rather than `split_at`: a key is user input, and slicing through the
    // middle of a multi-byte character must be a refusal, not a panic.
    let (date, rest) = (p.get(..10)?, p.get(10..)?);
    let (y, m, d) = parse_ymd(date)?;
    let rest = rest.strip_prefix('T').or_else(|| rest.strip_prefix(' '))?;
    let (hh, tail) = (rest.get(..2)?, rest.get(2..)?);
    if !hh.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if !matches!(tail, "" | ":00" | ":00:00") {
        return None;
    }
    let h: i64 = hh.parse().ok()?;
    if h > 23 {
        return None;
    }
    Some(days_from_civil(y, m, d) * 24 + h)
}

/// `YYYY-MM`, the month optionally unpadded (`2026-6`) — as lenient as a day key,
/// since normalisation spells every key canonically anyway.
fn parse_ym(p: &str) -> Option<(i64, i64)> {
    let (y, m) = p.split_once('-')?;
    if m.is_empty() || m.len() > 2 {
        return None;
    }
    let (y, m) = (y.parse::<i64>().ok()?, m.parse::<i64>().ok()?);
    (YEARS.contains(&y) && (1..=12).contains(&m)).then_some((y, m))
}

/// `YYYY-Www`, an ISO 8601 week — refused if the year has no such week.
fn parse_iso_week(p: &str) -> Option<i64> {
    let (y, w) = p.split_once("-W")?;
    if w.len() != 2 {
        return None;
    }
    let (y, w) = (y.parse::<i64>().ok()?, w.parse::<i64>().ok()?);
    if !YEARS.contains(&y) {
        return None;
    }
    let week_one = iso_week_one_monday(y);
    let weeks_in_year = (iso_week_one_monday(y + 1) - week_one) / 7;
    if !(1..=weeks_in_year).contains(&w) {
        return None;
    }
    Some(week_ordinal_of_monday(week_one + 7 * (w - 1)))
}

/// ISO weekday, Monday = 1 … Sunday = 7. Day 0 (1970-01-01) was a Thursday.
fn iso_weekday(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
}

fn monday_of(days: i64) -> i64 {
    days - (iso_weekday(days) - 1)
}

/// ISO week 1 is the week holding the year's first Thursday — equivalently, 4 January.
fn iso_week_one_monday(year: i64) -> i64 {
    monday_of(days_from_civil(year, 1, 4))
}

/// Mondays fall on days ≡ -3 (mod 7) — 1969-12-29 was one — so this is exact.
fn week_ordinal_of_monday(monday: i64) -> i64 {
    (monday + 3).div_euclid(7)
}

fn monday_of_week_ordinal(n: i64) -> i64 {
    n * 7 - 3
}

/// The ISO (year, week) a Monday starts: the week belongs to the year of its Thursday.
fn iso_week_of_monday(monday: i64) -> (i64, i64) {
    let (year, _, _) = civil_from_days(monday + 3);
    (year, (monday - iso_week_one_monday(year)) / 7 + 1)
}

// Howard Hinnant's days_from_civil / civil_from_days — exact, branch-light,
// dependency-free proleptic-Gregorian date arithmetic. Epoch is 1970-01-01 = 0.
pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

pub(crate) fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}
