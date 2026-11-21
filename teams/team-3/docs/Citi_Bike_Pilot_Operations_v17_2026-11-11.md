# Citi Bike pilot operations (prototype v17), November 11, 2026

Code: `12_Prototype_v17/`. Job notebook to import: `Citi_Bike_Pilot_Window_Job_v17_IMPORT_INTO_DATABRICKS.py`. 45 local tests and the outcome checks pass. Nothing here sends alerts or writes to S3 (Simple Storage Service).

## What changed from v16 (P2, P3)

- **Fresh database per window.** A run restores a checkpoint only from the same window (a restart inside a window). Nothing is carried between windows: the outcome tail runs 60 minutes past the last scoring slot, so every prediction ends as known or unknown before the window closes. The window report records `pending_outcomes_at_end`, which should be 0.
- **Size and speed recorded.** Every poll records checkpoint bytes and publish seconds. If one publish takes longer than 60 seconds, the window stops, writes its report, releases the lock and fails the job (exit code 5).
- **Late start handled.** Slots that passed before a run started are listed as "not attempted before start", separate from "skipped late".
- **Entry point.** `pilot_job_entry.py` picks am (start 06:00 to 11:45) or pm (start 15:00 to 21:45) from New York time, uses the New York date, checks the frozen model files, manifest and ACS (American Community Survey) table fingerprints before polling, uses the state folder "Citi Bike - Pilot State", and exits non-zero with a clear message: 2 outside windows, 3 lock held, 4 frozen files changed, 5 publish too slow, 1 any other failure.
- **Retention (off until you approve).** Job parameter `retention_keep_last`. Blank keeps every checkpoint.

## Decision 1: checkpoint retention rule (needs your approval)

Proposed rule: **keep the newest 3 checkpoints of each window.** After each new checkpoint is published and its fingerprint verified, older checkpoints of the same window are deleted. The newest (the one `LATEST.json` points to) is never deleted, so the final checkpoint of every window is always kept. Other windows' files are never touched. Raw response archives are kept in full.

To enable: set the job parameter `retention_keep_last` to `3`.

## Storage estimates (P2d)

Measured in the live test: a checkpoint starts near 0.33 MB and grows about 0.22 MB per poll; raw archives are about 0.2 MB per poll. A morning window has 61 polls, an evening window 73.

| | Keep all checkpoints | Keep newest 3 per window |
|---|---:|---:|
| One morning window | about 0.43 GB | about 40 MB |
| One evening window | about 0.60 GB | about 48 MB |
| Per day (both windows, with raw archives) | about 1.1 GB | about 115 MB |
| Pilot Nov 11 evening to Nov 13 (5 windows) | about 2.7 GB | about 0.3 GB |
| Through Nov 22 (about 23 windows) | about 12 GB | about 1.3 GB |

## Decision 2: crash recovery

### Manual procedure (always available)

Use this when a run failed with exit code 3 ("Lock held") or a run was killed or timed out.

1. In Databricks, open **Jobs & Pipelines**, then the job. Confirm that **no run is active** (none shows "Running" or "Pending").
2. Open the failed run's output. Its summary shows `lock` → `lock` → `owner` (the dead owner ID) and `heartbeat_age_seconds`.
3. Wait until the heartbeat age is over 1,200 seconds (20 minutes). A recent heartbeat means a runner may still be alive.
4. Click **Run now with different parameters**. Set `takeover_owner` to the dead owner ID from step 2. Leave everything else unchanged.
5. The new run moves the old lock into `locks_cleared/`, restores the latest checkpoint of the same window, resolves the dead run's unfinished poll, and continues. Slots missed during the outage are listed as "not attempted before start".
6. After it finishes, clear `takeover_owner` again (it is only for that one run).

If a run fails outside a window (exit code 2), nothing was written; no action is needed.

### Proposed automatic rule (not enabled; needs your approval)

At start, if the lock is held, the run takes over automatically only when all three are true: the heartbeat is older than 20 minutes; the job is set to at most one concurrent run (so the lock holder cannot be another active run of this job); and the takeover is written to a takeover log in the state folder with the old owner, heartbeat age and time. Otherwise it exits with code 3 as today. Risk: a run started by hand outside the job (for example, from a notebook) would not be protected by the one-concurrent-run setting. Not built yet; I will build it only if you approve.

## Creating the job by hand (approved settings)

1. Import `Citi_Bike_Pilot_Window_Job_v17_IMPORT_INTO_DATABRICKS.py` into your user folder (Workspace → Users → your folder → ⋮ → Import → File).
2. Open **Jobs & Pipelines** → **Create** → **Job**. Name: `Citi Bike Team 3 Pilot Window Runner`.
3. Task: type **Notebook**, source **Workspace**, path = the imported notebook. Compute: **Serverless** if offered.
4. Parameters: `retention_keep_last` (blank unless approved) and `takeover_owner` (blank).
5. Job settings: **Maximum concurrent runs = 1**; **Timeout = 6 hours 30 minutes**; **Retries = 0**; **Notifications: on failure, email you**.
6. Schedule: **leave it paused or off** until the dress rehearsal passes. When approved: two triggers, 06:28 and 15:28, time zone America/New_York (cron `0 28 6,15 * * ?`).

## Job feasibility checklist (P7, for you to check)

- **Serverless jobs**: in the task's compute dropdown, is "Serverless" offered? If not, which compute types are?
- **Maximum run length**: does your plan show a limit on job run duration (Settings, or the job's Timeout field rejecting 6.5 hours)? An evening window needs about 6 hours from a 15:28 start.
- **Usage limits**: Settings → Billing or Usage (or the account's plan page): is there a daily compute or "DBU" (Databricks Unit) limit, and how much does an idle 6-hour Python run use?
- **Workspace storage**: any quota on workspace files (the pilot state folder needs about 0.3 GB with retention for three days).

## Dress rehearsal tonight (P8)

With the job created and the schedule off, click **Run now** as soon as possible. It joins the evening window already in progress: slots before its start are listed as "not attempted before start". It scores until 20:25 New York and collects until 21:30, then exits. I will report polls, gaps, late slots, unknown share, checkpoint sizes, publish times, and the files the S3 publisher would write. The S3 publisher (P6) is not built yet; nothing goes to S3 tonight.
