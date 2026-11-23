"""MLflow bundle and version-pinned scoring for Team 6."""

import json
from importlib.metadata import version
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from mlflow.models import infer_signature

MODEL_NAME = "workspace.citibike_3h.team6_next_hour_departures"


def forecast_request(as_of, mode="both", station_id="", forecasts=None):
    return pd.DataFrame([{
        "issued_at_utc": pd.Timestamp(as_of).isoformat(),
        "mode": mode,
        "station_id": station_id,
        "weather_json": "[]" if forecasts is None else forecasts.to_json(
            orient="records", date_format="iso", date_unit="us", double_precision=15
        ),
    }])


class StationForecast(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        self.bundle = Path(context.artifacts["bundle"])

    def predict(self, context, model_input, params=None):
        from next_hour import score_next_hour

        batches = []
        for request in model_input.itertuples(index=False):
            forecasts = pd.DataFrame(json.loads(request.weather_json))
            scored = score_next_hour(self.bundle, request.issued_at_utc, request.mode,
                                     request.station_id, forecasts)
            batches.append(scored.assign(station_id=scored.station_id.astype(str)))
        return pd.concat(batches, ignore_index=True)


def publish_version(model_dir, model_name):
    from next_hour import MODEL_FEATURES, score_next_hour

    manifest = json.loads((model_dir / "manifest.json").read_text())
    extension = ".joblib" if manifest.get("estimator_family") == "poisson_glm" else ".txt"
    manifest.setdefault("bundle_version", model_dir.name)
    scripts = Path(__file__).resolve().parent
    example = forecast_request(pd.Timestamp.now(tz="UTC"), "calendar")
    example_output = score_next_hour(model_dir, example.issued_at_utc.iloc[0], "calendar")
    signature = infer_signature(example, example_output.assign(station_id=example_output.station_id.astype(str)))
    with TemporaryDirectory(prefix="team6-registry-") as staging:
        bundle = Path(staging) / "bundle"
        bundle.mkdir()
        for name in ["demand_curves.parquet", "stations.parquet", *[f"{m}{extension}" for m in MODEL_FEATURES]]:
            copyfile(model_dir / name, bundle / name)
        (bundle / "manifest.json").write_text(json.dumps(manifest))
        logged = mlflow.pyfunc.log_model(
            name="station_forecast", python_model=StationForecast(), artifacts={"bundle": str(bundle)},
            input_example=example, signature=signature, registered_model_name=model_name,
            code_paths=[str(scripts / name) for name in (
                "forecast_registry.py", "mvp_data.py", "next_hour.py", "poisson_challenger.py",
            )],
            pip_requirements=[f"{name}=={version(name)}" for name in (
                "mlflow", "pandas", "numpy", "pyarrow", "scipy", "lightgbm", "scikit-learn",
            )],
        )
    return str(logged.registered_model_version)


def existing_alias(client, model_name, alias):
    try:
        return client.get_model_version_by_alias(model_name, alias)
    except MlflowException as exc:
        if exc.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise
        return None


def score_registered(model_name, alias, request):
    mlflow.set_registry_uri("databricks-uc")
    client = MlflowClient()
    version_number = str(client.get_model_version_by_alias(model_name, alias).version)
    model = mlflow.pyfunc.load_model(f"models:/{model_name}/{version_number}")
    return model.predict(request).assign(model_name=model_name, model_alias=alias, model_version=version_number)
