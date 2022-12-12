import pytest
from dask.datasets import timeseries


@pytest.fixture()
def sample_ddf():
    return timeseries(
        dtypes={
            "floats": float,
            "ints": int,
            "strings": str,
        }
    )

