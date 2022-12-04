from dask.datasets import timeseries
import pytest

@pytest.fixture()
def sample_ddf():
    return timeseries(
        dtypes={
            "floats": float,
            "ints": int,
            "strings": str,
        }
    )