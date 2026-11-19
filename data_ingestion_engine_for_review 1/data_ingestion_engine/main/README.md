# `aimatic.py` — what it is, in plain terms

## What

`aimatic.py` is the **one script AWS Glue actually runs**. It doesn't do any
data processing itself — it's a router. It figures out *which* pipeline you
want (structured or unstructured) and hands off to the real code for that
pipeline.

## Where

- It runs inside an **AWS Glue Python-shell job** (not your laptop, not a
  server you manage — Glue's own managed runtime).
- The actual pipeline code (`core/`, `architecture/`, `structured_readers/`,
  etc.) doesn't live inside Glue permanently — it's zipped up
  (`data_ingestion_engine_v2.zip`), sits in S3, and `aimatic.py`'s first job
  is to download and unzip it before anything else can even be imported.

## When

Whenever someone (or something) starts a Glue job run:
```bash
aws glue start-job-run --job-name <your-job> --arguments '{...}'
```
That can be manual, on a schedule, or triggered by another AWS event —
`aimatic.py` doesn't care how it got started, only what parameters came with
that specific run.

## Why it's built this way

Before this, structured and unstructured ingestion would have needed two
separate Glue scripts to maintain. Instead, there's **one script, one
`ScriptLocation`, forever** — you never touch `aimatic.py`'s own Glue
configuration again. Which pipeline runs is decided entirely by the
*parameters* you pass on a given run, not by which script you pointed Glue
at.

## How it works (in order)

1. **Unpack the code.** Downloads the zip from S3, extracts it, adds it to
   Python's import path. Without this step, nothing else below can even be
   imported.
2. **Read the parameters** that were passed to this specific job run
   (`--reader`, `--architecture`, `--writer`, `--ecs_endpoint`, etc.).
3. **Load `config.yaml`** from S3 — this holds the *data* side of things
   (bucket names, table names, credentials) as opposed to the *parameters*,
   which decide *what to run*.
4. **Decide the mode** — unstructured or structured:
   - `--ecs_endpoint` present → **unstructured** (parses PDFs/docs via the
     Docling service)
   - `--reader` / `--architecture` present → **structured** (reads from a
     database/API/S3 file, processes it, writes it out)
   - `--pipeline_mode` can also be set explicitly instead of relying on
     which parameters are present
5. **Hand off to the real pipeline:**
   - Unstructured → `pipelines/unstructured_pipeline.py`
   - Structured → `core/pipeline_builder.py`, which assembles
     reader → architecture (Medallion or Lambda) → writer
6. **Report the result** — logs success/failure; alerts fire automatically
   if configured in `config.yaml`'s `alerting:` section.

## The one thing to know before reusing a job

If a Glue job already has `--pipeline_mode`/`--ecs_endpoint` saved on it
(from a previous unstructured run, say) and you try to reuse that *same*
job for a structured run by just adding `--reader`/`--architecture`, the9
script will now **stop you with a clear error** instead of silently running
the wrong pipeline — because the old saved parameters are still there
underneath your new ones. Safest option: give structured and unstructured
runs their own separate Glue jobs.

## Cheat sheet — minimum parameters per mode

| Mode | Required parameters |
|---|---|
| Unstructured | `--config_path`, `--ecs_endpoint`, `--writer_bucket` |
| Structured | `--config_path`, `--reader`, `--architecture`, `--writer`, `--writer_bucket` |

Full parameter list and defaults are in the docstring at the top of
`aimatic.py` itself.
