# Databricks notebook source
# MAGIC %md
# MAGIC # Citi Bike Team 3 setup: give a new run folder the fixed name later steps read
# MAGIC Steps 02 to 06 save each run in a new folder whose name ends in a random run ID. Later steps read three folders by the IDs of the original runs. After a fresh run, use this notebook to copy the new folder to the fixed name. Nothing is moved, renamed or overwritten.
# MAGIC
# MAGIC | Choice | New folder made by | Copied to | Read by |
# MAGIC |---|---|---|---|
# MAGIC | `experimental_models` | step 02, cell 11 | `citibike_experimental_models_c4e388f77d614002a3512d0478d40037` | step 02 cells 14 and 33; step 03 cell 6 (optional check) |
# MAGIC | `acs_table` | step 02, cell 28 | `Citi Bike - ACS and July Trip Development - e1eb6e26` | step 02 cell 31; steps 03, 04, 04b, 05; the pilot |
# MAGIC | `approved_models` | step 03, cell 9 | `Citi Bike - ACS Controls and Neighborhood Audit - ea8df268` | steps 04, 04b, 05; the pilot |
# MAGIC
# MAGIC Run it with the cell's Run button after the step that made the folder. Leave `source_folder` blank to use the newest matching folder, or type its exact name.

# COMMAND ----------

## Packages and the three fixed names
import hashlib, shutil
from pathlib import Path

LINKS = {
    'experimental_models': ('citibike_experimental_models_', 'citibike_experimental_models_c4e388f77d614002a3512d0478d40037'),
    'acs_table': ('Citi Bike - ACS and July Trip Development - ', 'Citi Bike - ACS and July Trip Development - e1eb6e26'),
    'approved_models': ('Citi Bike - ACS Controls and Neighborhood Audit - ', 'Citi Bike - ACS Controls and Neighborhood Audit - ea8df268'),
}
dbutils.widgets.dropdown('link', 'acs_table', list(LINKS))  # Which folder to copy
dbutils.widgets.text('source_folder', '')  # Optional: exact name of the new folder

# COMMAND ----------

## Find the new folder and copy it to the fixed name
user = spark.sql('select current_user() as u').first()['u']
home = Path('/Workspace/Users') / user  # The same home folder every step uses
prefix, fixed = LINKS[dbutils.widgets.get('link')]
target = home / fixed
# Confirm the fixed name is free; an existing folder is never replaced
if target.exists():
    raise RuntimeError('Already present, nothing copied: ' + fixed)
name = dbutils.widgets.get('source_folder').strip()
if name:
    source = home / name
else:
    # Newest folder with the right prefix, other than the fixed name
    found = [p for p in home.iterdir() if p.is_dir() and p.name.startswith(prefix) and p.name != fixed]
    if not found:
        raise RuntimeError('No folder starting with: ' + prefix)
    source = max(found, key=lambda p: p.stat().st_mtime)
if not source.is_dir():
    raise RuntimeError('Not a folder: ' + str(source))
shutil.copytree(source, target)


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


# Confirm every copied file is identical to its source
pairs = [(f, target / f.relative_to(source)) for f in source.rglob('*') if f.is_file()]
assert all(sha(a) == sha(b) for a, b in pairs), 'a copied file differs from its source'
# Display what was copied
print('COPIED', source.name, '->', fixed, 'files', len(pairs))
