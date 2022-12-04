from dask.datasets import timeseries
from pandas.testing import assert_frame_equal
import dask_deltalake as ddl
from dask.dataframe.utils import assert_eq
import pathlib
import pytest
import os

from deltalake.table import DeltaTableProtocolError


def test_write_ddf_with_index(sample_ddf, tmp_path: pathlib.Path):
    path = str(tmp_path)
    with pytest.raises(DeltaTableProtocolError):
        ddl.to_delta(sample_ddf, path)


def test_roundtrip_basic(sample_ddf, tmp_path: pathlib.Path):
    # Check we can create the subdirectory
    tmp_path = tmp_path / "path" / "to" / "table"
    ddf = sample_ddf.reset_index()
    ddl.to_delta(ddf, str(tmp_path))
    assert ("0"*20+".json") in os.listdir(tmp_path/"_delta_log")

    ddf_out = ddl.read_delta(str(tmp_path))

    assert_eq(ddf, ddf_out)
