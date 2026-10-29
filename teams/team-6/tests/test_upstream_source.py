from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from citibike_pipeline import upstream_source as source


def test_information_freshness_is_strict_and_timezone_safe():
    observed = datetime(2026, 10, 12, 0, tzinfo=timezone.utc)
    assert source.information_is_fresh(observed, datetime(2026, 10, 14, 0, tzinfo=timezone.utc))
    assert not source.information_is_fresh(observed, datetime(2026, 10, 14, 1, tzinfo=timezone.utc))


def test_status_profile_records_keys_schema_and_invalid_rows(tmp_path: Path):
    path = tmp_path / "status.parquet"
    pq.write_table(
        pa.table(
            {
                "station_id": ["a", "a", "a"],
                "fetched_at": pa.array(
                    [datetime(2026, 8, 21, 0), datetime(2026, 8, 21, 0), datetime(2026, 8, 21, 0, 5)],
                    type=pa.timestamp("ns"),
                ),
                "_has_parse_error": [False, False, True],
                "_required_value_missing": [False, False, False],
                "_has_negative_count": [False, False, False],
            }
        ),
        path,
    )
    profile = source.parquet_profile(path, "status")
    assert profile["rows"] == 3
    assert profile["stations"] == 1
    assert profile["duplicate_keys"] == 1
    assert profile["invalid_rows"] == 1
    assert {field["name"] for field in profile["schema"]} >= {"station_id", "fetched_at"}
