"""Run a versioned ensemble experiment locally without changing active models."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from uuid import uuid4


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("station", "availability"))
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    runtime_name = arguments.kind + "_ensemble_runtime"
    runtime = load(arguments.package / (runtime_name + ".py"), runtime_name)
    output = arguments.output or arguments.package.parent.parent / (arguments.kind + "_ensemble_results") / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    )
    report = runtime.run(arguments.package, output)
    print(json.dumps({"output_root": str(output), "schema_version": report["schema_version"]}, indent=2))
