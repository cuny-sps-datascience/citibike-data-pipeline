# Final Project Consolidation

> Historical record: this October 11 consolidation was superseded as the active working location on October 20, 2026. Team 6 now works from `cuny-sps-datascience/citibike-data-pipeline`, branch `team-6`, directory `teams/team-6`. See [Team 6 shared-repository migration](shared_pipeline_migration.md).

On October 11, 2026, all local DATA 622 final-project work was consolidated under `/Users/randyhowk/Sites/CUNY/CUNY622FinalProject` at the user's request. This is the default directory for future work.

## Files and history

- The destination's existing Git history was retained. Its worktree was clean before migration.
- Of 86 relevant files in the earlier `CUNY/622/FinalProject` worktree, 75 already matched the destination and 11 were added or updated. These include newer bike-mix code/tests, documentation, the neighborhood proposal/PDF, and the Free Edition plan.
- The previous worktree, including its old environment and caches, was moved to `.local/migration-2026-10-11/previous-project/`. Pre-migration versions of differing destination files are preserved under `.local/migration-2026-10-11/destination-originals/`.
- The top-level `CUNY622FinalProjectPublic` repository was moved to `public/`. Its content hashes and Git HEAD were verified before/after the move. It remains an independent repository and is ignored by the parent repository.
- `CUNY/citibike_april_2025_sample.csv` was moved to `data/samples/`, with its content hash verified. It is a small example, not a modeling dataset.
- Original and processed bulk datasets remain at the RAID paths in `config/local.yaml`. The migration does not change their stored contents or ledger entries.

The [migration manifest](project_migration_manifest.json) records incoming content hashes, changed files, original repository HEADs, the public content inventory and the CSV hash. It describes the migration inputs; later documentation edits and future public-repository changes can legitimately differ from those hashes.

## Default working directory

The workspace `AGENTS.md`, project `AGENTS.md`, README and active runbook point to the canonical root. Active Markdown working-directory references were updated. Historical PDF specifications and recovery copies retain their original text; use the current README and runbook for paths.

The environment was rebuilt with `uv sync --locked` at the canonical root, rather than reusing the moved virtual environment. `citibike doctor` resolves the live RAID inputs, and package imports resolve to this checkout. The older filesystem locations no longer contain active worktrees.

## Verification

Verification covers migration hashes, both Git histories, default import/config paths, CLI help and the existing test suite. The separate modeling-input audit reads the live published-file ledger and recent trip columns without changing the trip archive. Its report is linked from the implementation status and export plan.

The suite passed all 180 tests in 27.30 seconds from the canonical checkout. It used a temporary file-based runner to avoid the existing Miniforge `readline` startup issue; the pytest invocation was guarded with `if __name__ == '__main__'` so multiprocessing workers did not rerun the suite. `git diff --check` and migration/content/link checks also passed.
