import duckdb
import pyarrow
import yaml

import citibike_pipeline


def test_stack_imports():
    assert citibike_pipeline.__version__ == "0.1.0"
    major, minor = (int(x) for x in duckdb.__version__.split(".")[:2])
    assert (major, minor) >= (1, 3)
    assert pyarrow.__version__
    assert yaml.safe_load("a: 1") == {"a": 1}
