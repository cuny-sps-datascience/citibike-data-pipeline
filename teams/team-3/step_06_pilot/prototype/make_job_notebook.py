"""Build the importable job notebook with the v18 package embedded and fingerprinted."""
# Reading copy with comments added. The code is unchanged from the original module (this file is not embedded in the deployed notebook).
# Do not run this copy to rebuild the job notebook: the frozen notebook was built from the original.
#
# Purpose: pack the 16 package files into one compressed archive, fingerprint
# it, and paste it as text into a Databricks notebook template. The job then
# unpacks exactly these bytes and checks the fingerprint before running.

## Packages
import base64, hashlib, io, json, tarfile  # Text encoding, fingerprints, in-memory file, manifest, archive

## List the files that make up the v18 package
files = ['pilot_job_entry.py', 'pilot_loop.py', 'pilot_state.py', 'window_runner.py', 'window_runner_cli.py', 'batch_scorer.py',  # Window runner code
         'candidate_scorer_acs.py', 'scoring.py', 'normalization.py', 'outcomes.py', 'private_artifacts.py', 's3_publisher.py', 'model_parameters.json',  # Scoring, outcomes, publishing
         'cohort_reconstructed.csv', 'dashboard_view.py', 'station_groups_poverty_and_no_vehicle_thirds.csv']  # Cohort, dashboard view, equity groups

## Create a reproducible compressed archive in memory
buf = io.BytesIO()
with tarfile.open(fileobj=buf, mode='w:gz') as t:
    for f in files:
        # Clear time stamps and owner names so the same files always give the same archive bytes.
        info = t.gettarinfo(f, arcname=f); info.mtime = 0; info.uid = info.gid = 0; info.uname = info.gname = ''
        with open(f, 'rb') as h: t.addfile(info, h)

## Fingerprint and encode the archive
data = buf.getvalue(); sha = hashlib.sha256(data).hexdigest(); b64 = base64.b64encode(data).decode()  # Archive bytes, fingerprint, text form
body = '\n'.join("    '%s'" % b64[i:i + 100] for i in range(0, len(b64), 100))  # Split into 100-character lines for the notebook
manifest = json.dumps({f: hashlib.sha256(open(f, 'rb').read()).hexdigest() for f in files}, indent=1)  # Per-file fingerprints

## Fill the template and write the notebook
nb = open('job_notebook_template.txt').read().replace('@@SHA@@', sha).replace('@@B64@@', body).replace('@@FILES@@', manifest)
out = '../Citi_Bike_Pilot_Window_Job_v18_IMPORT_INTO_DATABRICKS.py'  # Output goes one folder up
open(out, 'w').write(nb); print(sha, hashlib.sha256(nb.encode()).hexdigest())  # Display archive and notebook fingerprints
