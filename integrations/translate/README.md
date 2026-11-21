# freshet-translate: move a project's SQL to another warehouse

A project moving to a new warehouse needs its models in the new side's dialect.
Freshet translates nothing; this package runs [sqlglot](https://github.com/tobymao/sqlglot)
over every model, writes the result as a project for the new side, and then has
Freshet's own planner say which models the translation **changed in substance**, and
which models read those.

Those are the models whose new side runs SQL the old side never ran. Rebuild them
there, and check them first. Everything else was only respelled.

## Use it

```sh
pip install ./integrations/translate   # from a checkout; plus the freshet CLI, on PATH or in $FRESHET
freshet-translate --project ./models --from postgres --to bigquery --out ./models.bigquery --diff
```

```text
postgres -> bigquery (sqlglot 30.25.0): 3 model(s) into models.bigquery: 1 rewritten, 1 downstream, 1 unchanged
  rewritten   daily                    rebuild it on bigquery and check it
  downstream  recent                   check it: it reads daily
  unchanged   events
passed through as written, so bigquery must have them with the same meaning:
  daily                    my_udf

--- daily.sql (postgres)
+++ daily.sql (bigquery)
@@ -4,7 +4,7 @@
   my_udf(note) AS tag
 FROM events
 WHERE
-  note ILIKE '%refund%'
+  LOWER(note) LIKE LOWER('%refund%')
 GROUP BY
   dt,
   tag
```

`./models.bigquery` is an ordinary Freshet project, so `freshet plan --project
./models.bigquery --sql bigquery` plans the new side. The JSON side files
(`sources.json`, `materializations.json`, `incremental.json`) are copied as they are.

| Status | Meaning |
|---|---|
| `rewritten` | The translator rewrote it (or a person did: see overrides). The new side runs different SQL. |
| `downstream` | It reads a rewritten model through a column that changed, per Freshet's column lineage. |
| `unchanged` | The translation is the source in substance. |
| `failed` | sqlglot could not translate it. The run fails, and nothing is written. |

Functions sqlglot does not know are passed through as written and listed: the new
warehouse has to have each one, with the same meaning. A construct sqlglot cannot
write for the target (`generate_series` in a projection, for Snowflake) is a
failure, not a guess.

`--json` prints the same report as a `freshet-translate/1` document:

```json
{"contract": "freshet-translate/1", "from": "postgres", "to": "bigquery",
 "translator": "sqlglot 30.25.0", "out": "models.bigquery", "ok": true,
 "models": [{"name": "daily", "status": "rewritten", "override": false, "because_of": null,
             "untranslated_functions": ["my_udf"], "error": null}, ...]}
```

Exit status: `0` when every model was translated and the project written, `1` when a
model failed or Freshet could not plan the translation (nothing is written), `2` for
a usage error.

## Overrides

Some models need a person. Write the new side's SQL for them as `<dir>/<model>.sql`
and pass `--overrides <dir>`: it is used as written, in place of a translation, and
classified like one (`override: true` in the report). An override that names no model
of the project is refused, since a misspelled one would silently not apply.

## Run it on every commit

While both warehouses run, the source project stays the one people edit. Translate it
on each commit, as a build step, and plan each side from its own project:

```sh
freshet-translate --project ./models --from postgres --to bigquery --out ./models.bigquery --overrides ./bigquery
freshet plan --project ./models          --state .planner/postgres.json --sql postgres --json
freshet plan --project ./models.bigquery --state .planner/bigquery.json --sql bigquery --json
```

`--out` is generated output, replaced whole on every run. It must be a new (or empty)
directory, or one this tool wrote, which it records in a `.freshet-translate` file
there: a directory without that file is refused rather than deleted, even one holding
nothing but `.sql` files. The earlier translation is kept until the new one is in
place, so a run that fails leaves it as it was.

## How it asks Freshet

Freshet's fingerprint already separates spelling from substance. It ignores layout,
comments, keyword case and redundant parentheses, and keeps everything that can
change a result. So the source is committed to a scratch state and the translation
is planned against it. What Freshet plans is what changed, with its reason.

Two things are compared in a common spelling rather than as written:

- **The source.** A translator respells every model, so the state is committed from
  the source translated into *its own* dialect. Both sides then come from the same
  generator and share every cosmetic choice.
- **Function names.** Dialects case the same call differently (`count` and `COUNT`),
  and the fingerprint keeps function-name case because a BigQuery UDF's name is
  case-sensitive. Both sides are compared with function names in upper case. What is
  written keeps sqlglot's spelling for the dialect.

Then the translation as written is planned cold with `--sql <to>`. A project Freshet
cannot plan for the new side fails here rather than on the warehouse.

## What "unchanged" does not mean

An unchanged model runs the same SQL text on both sides. It can still give a
different answer: integer division, NULL ordering and string comparison are each a
warehouse's own. sqlglot rewrites what it knows to differ (an ascending `ORDER BY`
gains `NULLS LAST` on the way to BigQuery), and that shows as `rewritten`. What it
does not know, nothing here can show. A parity check of the moved data, row count
and checksum per partition on both sides, is what proves the results equal. This
report says where to look first.

## Limits

- Freshet's project directory format only (`*.sql` and the JSON side files). A dbt
  project's Jinja is not SQL, and is not translated.
- The five dialects Freshet renders: `postgres`, `snowflake`, `bigquery`,
  `databricks`, `redshift`.
- What sqlglot rewrites moves between its versions. The report names the version, and
  `sqlglot>=27` is the oldest the tests pass on.

## Test it

```sh
pip install sqlglot pytest
FRESHET=/path/to/freshet python -m pytest     # the classification tests skip without it
```
