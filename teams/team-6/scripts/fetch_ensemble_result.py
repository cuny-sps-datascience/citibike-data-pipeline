"""Wait for and preserve one cloud ensemble result on the processed RAID."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import time

from databricks.sdk import WorkspaceClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("station", "availability"))
    arguments = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    os.environ["PATH"] = str(project / ".local/bin") + os.pathsep + os.environ["PATH"]
    state_path = project / (".local/execution/databricks_" + arguments.kind + "_ensemble.json")
    state = json.loads(state_path.read_text())
    client = WorkspaceClient(profile="DATA622")
    last = None
    for _ in range(180):
        run = client.jobs.get_run(state["run_id"]).as_dict()
        status = run.get("state", {})
        if status != last:
            print(json.dumps(status), flush=True)
            last = status
        if status.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(10)
    else:
        raise TimeoutError("Ensemble run did not finish within 30 minutes")
    task = run["tasks"][0]
    output = client.jobs.get_run_output(task["run_id"]).as_dict()
    if status.get("result_state") != "SUCCESS":
        raise RuntimeError(json.dumps({"state": status, "output": output}))
    report = json.loads(output["notebook_output"]["result"])
    cloud_root = report["output_root"]
    base = Path("/Volumes/BigRaid/RandyFiles/Cuny/622/FinalProject_processed/modeling_exports/three_hour")
    destination = base / (arguments.kind + "_ensemble_results") / Path(cloud_root).name
    destination.mkdir(parents=True, exist_ok=True)
    names = ["report.json", "selection.json", "models.joblib", "predictions.parquet"]
    if arguments.kind == "availability":
        names.insert(2, "calibration.json")
    artifacts = []
    for name in names:
        data = client.files.download(cloud_root + "/" + name).contents.read()
        path = destination / name
        if path.exists() and path.read_bytes() != data:
            raise ValueError("Existing result differs")
        path.write_bytes(data)
        artifacts.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    active = "/Volumes/workspace/citibike_3h/models/borough_active.json"
    pointer = hashlib.sha256(client.files.download(active).contents.read()).hexdigest()
    expected_pointer = report.get("active_pointer_sha256", report.get("active_departure_pointer_sha256"))
    if pointer != expected_pointer:
        raise ValueError("Active departure model pointer changed")
    receipt = state | {
        "report": report, "result_root": str(destination), "cloud_output_root": cloud_root,
        "status": status, "task_run_id": task["run_id"], "artifacts": artifacts,
        "active_pointer_sha256": pointer, "run_url": run.get("run_page_url", state["run_url"]),
    }
    receipt_path = project / (".local/execution/" + arguments.kind + "_ensemble_result.json")
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    (project / (".local/execution/" + arguments.kind + "_ensemble_result_root.txt")).write_text(str(destination) + "\n")
    print(json.dumps({"run_id": state["run_id"], "status": status, "result_root": str(destination),
                      "schema_version": report["schema_version"], "active_model_changed": False}, indent=2))
