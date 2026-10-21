"""Upload and submit an immutable ensemble experiment without changing active jobs or tables."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, workspace

from citibike_pipeline.forecast_sources import digest
from deploy_three_hour_pilot import upload_immutable


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("station", "availability"))
    parser.add_argument("--package", type=Path, required=True)
    arguments = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    os.environ["PATH"] = str(project / ".local/bin") + os.pathsep + os.environ["PATH"]
    manifest = json.loads((arguments.package / "manifest.json").read_text())
    expected = arguments.kind + "-ensemble-package-v1"
    if manifest["schema_version"] != expected or manifest["status"] != "built":
        raise ValueError("Wrong or incomplete ensemble package")
    client = WorkspaceClient(profile="DATA622")
    destination = f"/Volumes/workspace/citibike_3h/inputs/{arguments.kind}_ensembles/{arguments.package.name}"
    for artifact in manifest["artifacts"]:
        path = arguments.package / artifact["path"]
        if digest(path) != artifact["sha256"]:
            raise ValueError("Package checksum mismatch")
        upload_immutable(client, destination + "/" + artifact["path"], path.read_bytes())
    upload_immutable(client, destination + "/manifest.json", (arguments.package / "manifest.json").read_bytes())
    number = "24" if arguments.kind == "station" else "25"
    filename = number + "_" + arguments.kind + "_ensemble"
    folder = "/Users/" + client.current_user.me().user_name + "/DATA622_ThreeHour"
    notebook = folder + "/" + filename
    content = (project / "notebooks" / (filename + ".py")).read_bytes()
    client.workspace.upload(notebook, content, format=workspace.ImportFormat.SOURCE,
                            language=workspace.Language.PYTHON, overwrite=True)
    response = client.jobs.submit(
        run_name="DATA622 " + arguments.kind + " ensemble development",
        environments=[jobs.JobEnvironment(environment_key="ensemble", spec=compute.Environment(
            environment_version="5", dependencies=["scikit-learn==1.7.2"]))],
        tasks=[jobs.SubmitTask(task_key=arguments.kind + "_ensemble", notebook_task=jobs.NotebookTask(
            notebook, base_parameters={"package_root": destination}), environment_key="ensemble", timeout_seconds=1800)],
        timeout_seconds=1800,
    )
    state = {
        "host": client.config.host, "run_id": response.run_id, "kind": arguments.kind,
        "notebook": notebook, "package_root": destination, "local_package": str(arguments.package),
        "manifest_sha256": digest(arguments.package / "manifest.json"),
        "notebook_sha256": digest(project / "notebooks" / (filename + ".py")),
        "scheduled": False, "stable_tables_changed": False, "active_model_changed": False,
        "run_url": client.config.host + "/jobs/runs/" + str(response.run_id),
    }
    path = project / (".local/execution/databricks_" + arguments.kind + "_ensemble.json")
    path.write_text(json.dumps(state, indent=2) + "\n")
    print(json.dumps(state, indent=2))
