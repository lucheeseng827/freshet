//! freshet-fingerprint — AST-canonical fingerprinting for the v2 planner.
//!
//! The planner core's [`freshet_state::fingerprint_project`] hashes each model's own
//! definition: **whitespace-normalized SQL text** + column-name shape + sorted
//! dependency names. That is correct and cheap, but a purely *textual* normalization
//! re-fingerprints a model on cosmetic edits (re-indentation, a comment, lower- vs
//! upper-case keywords, a redundant paren), triggering a needless rebuild.
//!
//! This crate closes the planner core's TODO 6: it canonicalizes the SQL by **parsing it and
//! re-`Display`ing the AST** (via the same `sqlparser` freshet-sql pins), so two
//! cosmetically-different-but-semantically-identical definitions hash the same, while
//! any real change still flips the hash.
//!
//! ## Drop-in, state-compatible
//!
//! [`AstFingerprinter::fingerprint_project`] has the **same signature and return type**
//! as freshet-state's free function (`BTreeMap<String, Fingerprint>` where
//! `Fingerprint == u64`), so swapping the call site is the whole integration. The
//! on-disk [`StateSnapshot`](freshet_state::StateSnapshot) JSON shape is unchanged.
//!
//! ## Safety: only erase *provably inert* differences
//!
//! Under-detection (two genuinely different models sharing a `u64`) is a **silent data
//! bug** — the planner would skip a needed rebuild. So canonicalization erases ONLY
//! what is provably inert — whitespace, comments, keyword case, trailing semicolons,
//! and (from [`FingerprintScheme::AstV2`]) redundant parentheses — and **preserves**
//! identifier case/quoting, projection / `GROUP BY` / `ORDER BY` order, commutative
//! operand order, literals, grouping, and the `FROM`/`JOIN` graph. No operand
//! reordering, no identifier folding. SQL that fails to parse — or isn't a single
//! statement — falls back to the [`FingerprintScheme::Text`] path, which is
//! byte-for-byte freshet-state's normalization.
//!
//! ## AstV2: the tree, not its spelling
//!
//! [`FingerprintScheme::AstV1`] hashes the re-`Display`ed AST. That erases layout but
//! not parentheses: `sqlparser` keeps a redundant `(a)` as an `Expr::Nested` node and
//! prints it back, so wrapping a column in parens rebuilt the model. AstV2 hashes the
//! AST's *structure* (its `Debug` form, which `sqlparser` 0.52 gives no positions to)
//! with every `Nested` node replaced by its contents. That is exactly right, not merely
//! convenient: once parsed, grouping is encoded by the tree's shape — `(a+b)*c` and
//! `a+(b*c)` are different trees — so a parenthesis node carries no information of its
//! own. Stripping it from the *printed* text would be wrong (`a + b * c` would then be
//! the spelling of both), which is why AstV2 hashes the tree.
//!
//! ## Upgrading costs nothing: reconcile, don't rebuild
//!
//! A new scheme changes every fingerprint, which read naively is "every model
//! changed". [`reconcile`] instead checks a stored fingerprint that does not match
//! against what each *older* scheme would produce for the model today: a match means
//! the definition is unchanged and only the spelling of its hash moved, so the model
//! is re-keyed, not rebuilt. A genuine edit matches under no scheme. The only cost of
//! trying several schemes is a cross-scheme hash collision, which the scheme tag in
//! each preimage makes a 2⁻⁶⁴ event.
//!
//! A [`FingerprintScheme`] tag is folded into the hash preimage *first*, so a `Text`
//! hash and an `AstV1` hash of the same bytes can never collide — which is also what
//! makes [`reconcile`]'s comparison across schemes safe to do at all.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use freshet_core::{Model, Project};
use freshet_state::{Fingerprint, StateSnapshot}; // Fingerprint == u64; reused, never redefined.

/// Which algorithm produced a given model's fingerprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FingerprintScheme {
    /// freshet-state fallback: whitespace-normalized SQL text. Used when SQL fails to
    /// parse (or isn't a single statement). Byte-for-byte compatible with
    /// `freshet_state::fingerprint_project`'s normalization.
    Text = 0,
    /// AST-canonical: hash of the re-`Display`ed `sqlparser` AST. Erases layout,
    /// comments and keyword case; keeps redundant parentheses.
    AstV1 = 1,
    /// AST-structural: hash of the parsed tree with redundant parentheses removed.
    /// The default. See the crate docs for why this is the tree and not its text.
    AstV2 = 2,
}

/// A model's fingerprint plus the scheme that produced it. Only `fingerprint` ever
/// reaches the state file; `scheme` drives the one-time reconcile and observability
/// ("how many models are still on the text fallback?").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModelFingerprint {
    pub fingerprint: Fingerprint,
    pub scheme: FingerprintScheme,
}

/// Result of fingerprinting a whole project. Never escapes into the state format —
/// [`into_state_map`](Self::into_state_map) yields exactly the
/// `BTreeMap<String, Fingerprint>` the state layer expects.
#[derive(Clone, Debug, Default)]
pub struct AstFingerprintReport {
    pub per_model: BTreeMap<String, ModelFingerprint>,
}

impl AstFingerprintReport {
    /// The drop-in value for `freshet_state::fingerprint_project`'s return: scheme
    /// tags dropped, only the `u64`s remain.
    pub fn into_state_map(self) -> BTreeMap<String, Fingerprint> {
        self.per_model.into_iter().map(|(k, v)| (k, v.fingerprint)).collect()
    }

    /// Models that fell back to the text scheme (parser rejected their SQL) — for a
    /// CLI `--explain` / daemon metric. Empty in the happy path.
    pub fn text_fallbacks(&self) -> impl Iterator<Item = &String> {
        self.per_model
            .iter()
            .filter(|(_, v)| v.scheme == FingerprintScheme::Text)
            .map(|(k, _)| k)
    }
}

/// Stateless AST-canonical fingerprinter.
#[derive(Clone, Copy, Debug)]
pub struct AstFingerprinter {
    scheme: FingerprintScheme,
}

impl Default for AstFingerprinter {
    fn default() -> Self {
        Self::new()
    }
}

impl AstFingerprinter {
    /// The current scheme, [`FingerprintScheme::AstV2`].
    pub fn new() -> Self {
        Self::with_scheme(FingerprintScheme::AstV2)
    }

    /// A fingerprinter for one specific scheme — what [`reconcile`] uses to ask "what
    /// would the old scheme have said?". `Text` never parses; the AST schemes fall
    /// back to `Text` for SQL that does not parse, exactly as they always have.
    pub fn with_scheme(scheme: FingerprintScheme) -> Self {
        Self { scheme }
    }

    pub fn scheme(&self) -> FingerprintScheme {
        self.scheme
    }

    /// Drop-in replacement for `freshet_state::fingerprint_project`: same signature,
    /// same return type.
    pub fn fingerprint_project(&self, project: &Project) -> BTreeMap<String, Fingerprint> {
        self.report(project).into_state_map()
    }

    /// Like [`fingerprint_project`](Self::fingerprint_project) but keeps per-model
    /// scheme tags.
    pub fn report(&self, project: &Project) -> AstFingerprintReport {
        let mut per_model = BTreeMap::new();
        for name in project.names() {
            let m = project.get(name).expect("names() yields known models");
            per_model.insert(name.clone(), self.fingerprint_model(m));
        }
        AstFingerprintReport { per_model }
    }

    /// Fingerprint one model. Preserves freshet-state's own-definition-only semantics —
    /// folds the canonical SQL form + column-name shape + sorted dep names. ONLY the
    /// SQL sub-step changes (AST-canonical, with a text fallback). The scheme byte is
    /// hashed FIRST so a `Text` and an `AstV1` hash of the same bytes never collide.
    pub fn fingerprint_model(&self, model: &Model) -> ModelFingerprint {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let scheme = match self.scheme {
            FingerprintScheme::AstV2 if hash_structure(&model.sql, &mut h) => FingerprintScheme::AstV2,
            // Byte-for-byte the preimage this crate's default wrote before AstV2
            // existed — which is what lets `reconcile` recognise those state files.
            FingerprintScheme::AstV1 => match try_canonical_sql(&model.sql) {
                Some(c) => {
                    (FingerprintScheme::AstV1 as u8).hash(&mut h);
                    c.hash(&mut h);
                    FingerprintScheme::AstV1
                }
                None => text_preimage(&model.sql, &mut h),
            },
            // AstV2 over SQL that did not parse (`hash_structure` wrote nothing), and
            // Text itself.
            FingerprintScheme::AstV2 | FingerprintScheme::Text => text_preimage(&model.sql, &mut h),
        };
        // Column shape participates: add/remove/rename a column is a change.
        for c in &model.columns {
            c.name.hash(&mut h);
        }
        // The set of upstream names (sorted) — re-pointing a dependency is a change.
        let mut deps: Vec<&String> = model.depends_on.iter().collect();
        deps.sort();
        for up in deps {
            up.hash(&mut h);
        }
        ModelFingerprint { fingerprint: h.finish(), scheme }
    }
}

/// Committed fingerprints compared with the project today — see [`reconcile`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// Every model's fingerprint under the fingerprinter's scheme: what a commit writes.
    pub current: BTreeMap<String, Fingerprint>,
    /// Models whose definition changed, and new ones: what a plan seeds.
    pub changed: BTreeSet<String>,
    /// Models whose stored fingerprint was written by another scheme and still
    /// matches the model under that scheme. Unchanged — re-keyed by the next commit,
    /// not rebuilt.
    pub rekeyed: BTreeSet<String>,
}

/// Compare `prior` with the project, forgiving a change of fingerprint *scheme*.
///
/// A stored fingerprint that matches today's under the current scheme is unchanged,
/// as always. One that does not is checked against the other schemes a state file
/// can hold — this crate's `AstV1`, `AstV2` and `Text`, and freshet-state's own untagged
/// text fingerprint — each computed for the model as it is **today**. A match under
/// any of them means the definition has not moved since the fingerprint was written;
/// only its spelling did. A real edit matches under none.
///
/// This is what turns a scheme upgrade from "every model rebuilds once" into "every
/// model is re-keyed once". Only mismatching models pay for the older schemes, so a
/// steady-state plan costs exactly what it did.
pub fn reconcile(prior: &StateSnapshot, project: &Project, fp: &AstFingerprinter) -> Reconciled {
    let current = fp.fingerprint_project(project);
    let mut changed = BTreeSet::new();
    let mut rekeyed = BTreeSet::new();
    // freshet-state's scheme hashes whole projects; computed once, and only if needed.
    let mut module57: Option<BTreeMap<String, Fingerprint>> = None;
    let others: Vec<FingerprintScheme> = [FingerprintScheme::AstV2, FingerprintScheme::AstV1, FingerprintScheme::Text]
        .into_iter()
        .filter(|s| *s != fp.scheme())
        .collect();

    for (name, now) in &current {
        let Some(stored) = prior.fingerprints.get(name) else {
            changed.insert(name.clone());
            continue;
        };
        if stored == now {
            continue;
        }
        let model = project.get(name).expect("current is fingerprinted from this project");
        let same_under_another_scheme = others
            .iter()
            .any(|s| AstFingerprinter::with_scheme(*s).fingerprint_model(model).fingerprint == *stored)
            || module57.get_or_insert_with(|| freshet_state::fingerprint_project(project)).get(name) == Some(stored);
        if same_under_another_scheme {
            rekeyed.insert(name.clone());
        } else {
            changed.insert(name.clone());
        }
    }
    Reconciled { current, changed, rekeyed }
}

/// Hash the Text scheme's preimage: tag, then freshet-state's whitespace normalization.
fn text_preimage(sql: &str, h: &mut std::collections::hash_map::DefaultHasher) -> FingerprintScheme {
    (FingerprintScheme::Text as u8).hash(h);
    normalize_sql_fallback(sql).hash(h);
    FingerprintScheme::Text
}

/// AstV2: parse, strip redundant parentheses, and stream the tree's structure into
/// `h`. Returns `false` — having written nothing — when the SQL does not parse into
/// exactly one statement, so the caller can fall back.
#[cfg(feature = "ast")]
fn hash_structure(sql: &str, h: &mut std::collections::hash_map::DefaultHasher) -> bool {
    use std::fmt::Write as _;
    use std::hash::Hasher as _;
    let Some(tree) = structural_tree(sql) else { return false };

    /// Feeds formatted output straight into the hasher: a large model's Debug form is
    /// several times its SQL, and there is no reason to allocate it just to hash it.
    struct Feed<'a>(&'a mut std::collections::hash_map::DefaultHasher);
    impl std::fmt::Write for Feed<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            self.0.write(s.as_bytes());
            Ok(())
        }
    }
    (FingerprintScheme::AstV2 as u8).hash(h);
    let _ = write!(Feed(h), "{tree:?}");
    // Terminate the stream the way `str::hash` terminates a string, so what follows
    // (column names) can never be read as part of the tree. UTF-8 never holds 0xff.
    h.write_u8(0xff);
    true
}

#[cfg(not(feature = "ast"))]
fn hash_structure(_sql: &str, _h: &mut std::collections::hash_map::DefaultHasher) -> bool {
    false
}

/// The single statement `sql` parses to, with every redundant parenthesis removed —
/// the tree AstV2 hashes. `None` when it is not exactly one statement.
#[cfg(feature = "ast")]
pub fn structural_tree(sql: &str) -> Option<sqlparser::ast::Statement> {
    use sqlparser::ast::{visit_expressions_mut, Expr, Value};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    let mut stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None;
    }
    let _ = visit_expressions_mut(&mut stmts, |e| {
        // A loop, for `((a))`. Replacing the node with its contents loses nothing: the
        // tree's shape already says what the parentheses grouped.
        while let Expr::Nested(inner) = e {
            let unwrapped = std::mem::replace(inner.as_mut(), Expr::Value(Value::Null));
            *e = unwrapped;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    stmts.pop()
}

/// Canonicalize SQL by parsing and re-`Display`ing the AST. Returns `None` (forcing
/// the Text fallback) when the SQL fails to parse or is not exactly one statement.
#[cfg(feature = "ast")]
pub fn try_canonical_sql(sql: &str) -> Option<String> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None; // multi-statement / empty: conservative fallback.
    }
    Some(stmts[0].to_string())
}

/// Without the `ast` feature there is no SQL engine: every model uses the Text scheme,
/// i.e. exactly freshet-state's behaviour.
#[cfg(not(feature = "ast"))]
pub fn try_canonical_sql(_sql: &str) -> Option<String> {
    None
}

/// Byte-for-byte freshet-state's `normalize_sql`: collapse runs of whitespace, trim.
pub fn normalize_sql_fallback(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use freshet_core::{Column, ColumnRef, Materialization, Model};

    fn model(name: &str, sql: &str) -> Model {
        Model {
            name: name.into(),
            sql: sql.into(),
            materialization: Materialization::Table,
            columns: vec![Column::new("a", vec![ColumnRef::new("t", "a")])],
            depends_on: vec!["t".to_string()],
        }
    }

    fn fp(sql: &str) -> ModelFingerprint {
        AstFingerprinter::new().fingerprint_model(&model("m", sql))
    }

    #[test]
    fn text_fallback_matches_module57_normalization() {
        assert_eq!(normalize_sql_fallback("select   1   as x\n"), "select 1 as x");
    }

    #[cfg(feature = "ast")]
    #[test]
    fn cosmetic_rewrites_collide() {
        // whitespace, newline, keyword case, and a trailing comment are all inert.
        let a = fp("select a from t");
        let b = fp("SELECT   a\nFROM t");
        let c = fp("select a from t -- a trailing comment\n");
        assert_eq!(a.scheme, FingerprintScheme::AstV2, "the default scheme");
        assert_eq!(a.fingerprint, b.fingerprint, "keyword-case/whitespace are inert");
        assert_eq!(a.fingerprint, c.fingerprint, "comments are inert");
    }

    #[cfg(feature = "ast")]
    #[test]
    fn semantic_edits_differ() {
        assert_ne!(fp("select a from t").fingerprint, fp("select b from t").fingerprint);
        assert_ne!(fp("select a from t").fingerprint, fp("select a from u").fingerprint);
    }

    fn v2(sql: &str) -> u64 {
        AstFingerprinter::with_scheme(FingerprintScheme::AstV2).fingerprint_model(&model("m", sql)).fingerprint
    }

    fn v1(sql: &str) -> u64 {
        AstFingerprinter::with_scheme(FingerprintScheme::AstV1).fingerprint_model(&model("m", sql)).fingerprint
    }

    /// The M2 exit criterion, cosmetic half: every edit here is inert, so every
    /// spelling must hash like the first.
    #[cfg(feature = "ast")]
    #[test]
    fn cosmetic_edits_never_change_an_astv2_fingerprint() {
        let groups: &[&[&str]] = &[
            &[
                "select a, b from t where a > 1",
                "SELECT a, b FROM t WHERE a > 1",
                "select a,\n       b\n  from t\n where a > 1;",
                "select a, b -- the columns\nfrom t /* the table */ where a > 1",
            ],
            &[
                "select a + b as s from t",
                "select (a + b) as s from t",
                "select ((a + b)) as s from t",
                "select ((a) + (b)) as s from t",
            ],
            &[
                "select a from t where (x = 1) and (y = 2)",
                "select a from t where ((x = 1) and (y = 2))",
                "select a from t where x = 1 and y = 2",
            ],
        ];
        for group in groups {
            let first = v2(group[0]);
            for sql in &group[1..] {
                assert_eq!(v2(sql), first, "cosmetic: {sql:?} vs {:?}", group[0]);
            }
        }
    }

    /// The semantic half: every pair differs in meaning, so every pair must differ.
    #[cfg(feature = "ast")]
    #[test]
    fn semantic_edits_always_change_an_astv2_fingerprint() {
        let pairs: &[(&str, &str, &str)] = &[
            ("select (a + b) * c as v from t", "select a + (b * c) as v from t", "grouping"),
            ("select a - (b - c) as v from t", "select a - b - c as v from t", "associativity"),
            ("select a, b from t", "select b, a from t", "projection order"),
            ("select a + b as v from t", "select b + a as v from t", "operand order (kept: not provably inert)"),
            ("select a from t where x = 1", "select a from t where x = 2", "literal"),
            ("select a from t", "select A from t", "identifier case (kept: not inert in every dialect)"),
            ("select a from t", "select \"a\" from t", "identifier quoting"),
            ("select a from t order by a", "select a from t order by a desc", "sort direction"),
            ("select a from t", "select distinct a from t", "distinct"),
            ("select a from t union select a from u", "select a from t union all select a from u", "set quantifier"),
            ("select a from t where not (x and y)", "select a from t where not x and y", "negation scope"),
        ];
        for (a, b, why) in pairs {
            assert_ne!(v2(a), v2(b), "{why}: {a:?} vs {b:?}");
        }
    }

    #[cfg(feature = "ast")]
    #[test]
    fn astv1_keeps_redundant_parens_which_is_what_astv2_fixes() {
        assert_ne!(v1("select (a) from t"), v1("select a from t"), "AstV1 hashes the printed text, parens and all");
        assert_eq!(v2("select (a) from t"), v2("select a from t"));
    }

    #[cfg(feature = "ast")]
    #[test]
    fn the_scheme_tag_keeps_schemes_apart_on_identical_sql() {
        let sql = "select a from t";
        let text = AstFingerprinter::with_scheme(FingerprintScheme::Text).fingerprint_model(&model("m", sql)).fingerprint;
        assert!(v1(sql) != v2(sql) && v1(sql) != text && v2(sql) != text);
    }

    #[cfg(feature = "ast")]
    #[test]
    fn the_default_fingerprinter_is_astv2_and_falls_back_to_text() {
        assert_eq!(AstFingerprinter::new().scheme(), FingerprintScheme::AstV2);
        assert_eq!(fp("select a from t").scheme, FingerprintScheme::AstV2);
        assert_eq!(fp(")) nope ((").scheme, FingerprintScheme::Text);
        assert_eq!(
            fp(")) nope ((").fingerprint,
            AstFingerprinter::with_scheme(FingerprintScheme::Text).fingerprint_model(&model("m", ")) nope ((")).fingerprint,
            "an unparseable model hashes the same under every scheme — its text is all there is"
        );
    }

    /// Pinned: the tree AstV2 hashes for a small statement. `sqlparser` is pinned
    /// exactly (`=0.52.0`) because its `Debug` form *is* this scheme; if a bump changes
    /// this string, that bump is a scheme bump (AstV3 + reconcile), not a routine update.
    #[cfg(feature = "ast")]
    #[test]
    fn the_astv2_tree_of_a_small_statement_is_pinned() {
        let tree = format!("{:?}", structural_tree("select (a) from t").unwrap());
        assert!(tree.starts_with("Query(Query { with: None, body: Select(Select { distinct: None, top: None,"), "{tree}");
        assert!(tree.contains("projection: [UnnamedExpr(Identifier(Ident { value: \"a\", quote_style: None }))]"), "{tree}");
        assert!(!tree.contains("Nested"), "parentheses are gone from the tree: {tree}");
    }

    fn state_with(fps: BTreeMap<String, u64>) -> StateSnapshot {
        StateSnapshot { fingerprints: fps, ..Default::default() }
    }

    #[test]
    fn a_state_written_by_freshet_state_is_re_keyed_not_rebuilt() {
        let project = Project::from_models([model("a", "select a from t"), model("b", "select b from t")]);
        let prior = state_with(freshet_state::fingerprint_project(&project));
        let r = reconcile(&prior, &project, &AstFingerprinter::new());
        assert!(r.changed.is_empty(), "nothing was edited: {:?}", r.changed);
        assert_eq!(r.rekeyed, BTreeSet::from(["a".to_string(), "b".to_string()]));
        assert_eq!(r.current, AstFingerprinter::new().fingerprint_project(&project), "the commit writes the new scheme");
    }

    #[test]
    fn a_state_written_by_the_previous_default_is_re_keyed_not_rebuilt() {
        let project = Project::from_models([model("a", "select a from t"), model("broken", "%%% not sql")]);
        let prior = state_with(AstFingerprinter::with_scheme(FingerprintScheme::AstV1).fingerprint_project(&project));
        let r = reconcile(&prior, &project, &AstFingerprinter::new());
        assert!(r.changed.is_empty(), "{:?}", r.changed);
    }

    #[test]
    fn a_real_edit_is_changed_under_every_scheme() {
        let before = Project::from_models([model("a", "select a from t"), model("b", "select b from t")]);
        let after = Project::from_models([model("a", "select a from t where a > 0"), model("b", "select b from t")]);
        for written_by in [
            freshet_state::fingerprint_project(&before),
            AstFingerprinter::with_scheme(FingerprintScheme::AstV1).fingerprint_project(&before),
            AstFingerprinter::new().fingerprint_project(&before),
        ] {
            let r = reconcile(&state_with(written_by), &after, &AstFingerprinter::new());
            assert_eq!(r.changed, BTreeSet::from(["a".to_string()]));
            assert!(!r.rekeyed.contains("a"), "an edited model is never forgiven as a re-key");
        }
    }

    #[test]
    fn a_new_model_is_changed_and_a_matching_one_is_neither() {
        let project = Project::from_models([model("a", "select a from t"), model("new", "select n from t")]);
        let mut fps = AstFingerprinter::new().fingerprint_project(&project);
        fps.remove("new");
        let r = reconcile(&state_with(fps), &project, &AstFingerprinter::new());
        assert_eq!(r.changed, BTreeSet::from(["new".to_string()]));
        assert!(r.rekeyed.is_empty());
    }

    #[test]
    fn unparseable_sql_falls_back_to_text_without_panicking() {
        let f = fp(")) not valid sql ((");
        assert_eq!(f.scheme, FingerprintScheme::Text);
    }

    #[test]
    fn report_lists_text_fallbacks() {
        let project = Project::from_models([
            model("ok", "select a from t"),
            model("broken", "%%% not sql %%%"),
        ]);
        let report = AstFingerprinter::new().report(&project);
        let fb: Vec<&String> = report.text_fallbacks().collect();
        // Without the `ast` feature there is no parser, so every model is a fallback —
        // which is the lean build working as documented, not a failure to report.
        if cfg!(feature = "ast") {
            assert_eq!(fb, vec![&"broken".to_string()]);
        } else {
            assert_eq!(fb, vec![&"broken".to_string(), &"ok".to_string()]);
        }
        // into_state_map yields exactly the BTreeMap<String,u64> the state layer wants.
        assert_eq!(report.into_state_map().len(), 2);
    }
}
