//! `freshet-state-server`: hosted state for Freshet, run by the team that uses it.
//!
//! `freshet --state https://…` keeps the planner's state on a server instead of in a
//! file, so that every CI job plans against one state and commits by compare-and-swap
//! instead of last-writer-wins. This is a server for that protocol, with nothing else
//! to it: a directory on disk, a bearer token, the three calls the CLI makes, and two
//! reads for the console (`apps/freshet-console`), which `--console` serves alongside.
//!
//! | Call | Answer |
//! |---|---|
//! | `GET  /v1/{name}/current` | `200 {"version": int\|null, "snapshot": StateSnapshot}` |
//! | `POST /v1/{name}/commit` `{"expected_version", "snapshot", "summary"}` | `200 {"version"}` · `409 {"error", "expected", "found"}` |
//! | `GET  /v1/{name}/history?limit=N&before=V` | `200 {"commits": [...], "limit", "next_before"}`, newest first |
//! | `GET  /v1/` | `200 {"states": [{"name", "version", "committed_at", "models"}], "access"}` |
//! | `GET  /v1/{name}/versions/{v}` | `200 {"version", "committed_at", "summary", "snapshot"}` · `404` once not kept |
//!
//! `{name}` is a state, one per project, created by its first commit; `--state
//! https://host/v1/analytics` names one. The snapshot is the exact document a state
//! file holds, stored as received, so u64 fingerprints and ledger strings are exact.

pub mod console;
pub mod http;
pub mod store;

use std::time::{SystemTime, UNIX_EPOCH};

/// `t` as RFC 3339 in UTC, to the second: `2026-10-27T12:00:00Z`.
pub fn rfc3339(t: SystemTime) -> String {
    let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::rfc3339;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn timestamps_are_rfc3339_utc() {
        for (secs, want) in [
            (0, "1970-01-01T00:00:00Z"),
            (951_782_400, "2000-02-29T00:00:00Z"),
            (1_709_164_800, "2024-02-29T00:00:00Z"),
            (1_761_566_400, "2025-10-27T12:00:00Z"),
            (4_102_444_799, "2099-12-31T23:59:59Z"),
        ] {
            assert_eq!(rfc3339(UNIX_EPOCH + Duration::from_secs(secs)), want);
        }
    }
}
