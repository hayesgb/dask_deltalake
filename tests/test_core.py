import glob
import os
import tempfile
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from dask.dataframe.utils import assert_eq
from mock import MagicMock, patch

import dask_deltalake as ddl


@pytest.fixture(scope="session")
def simple_table(tmp_path_factory):
    output_dir = tmp_path_factory.mktemp("test")
    deltaf = zipfile.ZipFile("tests/data/simple.zip")
    deltaf.extractall(output_dir)
    fpath = [f for f in Path(output_dir).iterdir() if f.is_dir()]
    if len(fpath) == 1:
        return fpath[0].as_posix()
    else:
        raise RuntimeError


@pytest.fixture(scope="session")
def simple_table2(tmp_path_factory):
    output_dir = tmp_path_factory.mktemp("test2")
    deltaf = zipfile.ZipFile("tests/data/simple2.zip")
    deltaf.extractall(output_dir)
    fpath = [f for f in Path(output_dir).iterdir() if f.is_dir()]
    if len(fpath) == 1:
        return fpath[0].as_posix()
    else:
        raise RuntimeError
    # return str(output_dir) + "/simple_table/"


@pytest.fixture()
def partition_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/partition.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/test2/"


@pytest.fixture()
def empty_table1(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/empty1.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/empty/"


@pytest.fixture()
def empty_table2(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/empty2.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/empty2/"


@pytest.fixture(scope="session")
def checkpoint_table():
    # with tempfile.TemporaryDirectory as td:
    from pathlib import Path

    cwd = Path.cwd()
    output_dir = cwd
    deltaf = zipfile.ZipFile("tests/data/checkpoint.zip")
    deltaf.extractall(output_dir)
    output_dir = str(output_dir)
    return str(output_dir) + "/checkpoint/"


@pytest.fixture()
def vacuum_table(tmpdir):
    output_dir = tmpdir
    deltaf = zipfile.ZipFile("tests/data/vacuum.zip")
    deltaf.extractall(output_dir)
    return str(output_dir) + "/vaccum_table"


def test_read_delta(simple_table):
    df = ddl.read_delta(simple_table)

    assert df.columns.tolist() == ["id", "count", "temperature", "newColumn"]
    assert df.compute().shape == (200, 4)


def test_read_delta_with_different_versions(simple_table):
    df = ddl.read_delta(simple_table, version=0)
    assert df.compute().shape == (100, 3)

    df = ddl.read_delta(simple_table, version=1)
    assert df.compute().shape == (200, 4)


# def test_row_filter(simple_table):
#     # row filter
#     df = ddl.read_delta(
#         simple_table,
#         version=0,
#         filter=[("count", ">", 30)],
#     )
#     assert df.compute().shape == (61, 3)


def test_different_columns(simple_table):
    ddf = ddl.read_delta(simple_table, columns=["count", "temperature"])
    assert ddf.columns.tolist() == ["count", "temperature"]


def test_different_schema(simple_table):
    # testing schema evolution

    df = ddl.read_delta(simple_table, version=0)
    assert df.columns.tolist() == ["id", "count", "temperature"]

    df = ddl.read_delta(simple_table, version=1)
    assert df.columns.tolist() == ["id", "count", "temperature", "newColumn"]


# def test_partition_filter(partition_table):
#     # partition filter
#     df = ddl.read_delta(partition_table, version=0, filter=[("col1", "==", 1)])
#     assert df.compute().shape == (21, 3)

#     df = ddl.read_delta(
#         partition_table, filter=[[("col1", "==", 1)], [("col1", "==", 2)]]
#     )
#     assert df.compute().shape == (39, 4)


def test_empty(empty_table1, empty_table2):
    df = ddl.read_delta(empty_table1, version=4)
    assert df.compute().shape == (0, 2)

    df = ddl.read_delta(empty_table1, version=0)
    assert df.compute().shape == (5, 2)

    # with pytest.raises(RuntimeError):
    #     # No Parquet files found
    #     _ = ddl.read_delta(empty_table2)


def test_checkpoint(checkpoint_table):
    df = ddl.read_delta(checkpoint_table, checkpoint=0, version=4)
    assert df.compute().shape[0] == 25

    df = ddl.read_delta(checkpoint_table, checkpoint=10, version=12)
    assert df.compute().shape[0] == 65

    df = ddl.read_delta(checkpoint_table, checkpoint=20, version=22)
    assert df.compute().shape[0] == 115

    with pytest.raises(Exception):
        # Parquet file with the given checkpoint 30 does not exists:
        # File {checkpoint_path} not found"
        _ = ddl.read_delta(checkpoint_table, checkpoint=30, version=33)


def test_out_of_version_error(simple_table):
    # Cannot time travel Delta table to version 4 , Available versions for given
    # checkpoint 0 are [0,1]
    with pytest.raises(Exception):
        _ = ddl.read_delta(simple_table, version=4)


def test_load_with_datetime(simple_table2):
    log_dir = f"{simple_table2}/_delta_log"
    print(log_dir)
    log_mtime_pair = [
        ("00000000000000000000.json", 1588398451.0),  # 2020-05-02
        ("00000000000000000001.json", 1588484851.0),  # 2020-05-03
        ("00000000000000000002.json", 1588571251.0),  # 2020-05-04
        ("00000000000000000003.json", 1588657651.0),  # 2020-05-05
        ("00000000000000000004.json", 1588744051.0),  # 2020-05-06
    ]
    for file_name, dt_epoch in log_mtime_pair:
        file_path = os.path.join(log_dir, file_name)
        print(file_path)
        os.utime(file_path, (dt_epoch, dt_epoch))

    expected = ddl.read_delta(simple_table2, version=0)
    result = ddl.read_delta(simple_table2, datetime="2020-05-01T00:47:31-07:00")
    print(result)
    assert_eq(expected, result)

    expected = ddl.read_delta(simple_table2, version=1)
    result = ddl.read_delta(simple_table2, datetime="2020-05-02T22:47:31-07:00")
    assert_eq(expected, result)

    expected = ddl.read_delta(simple_table2, version=4)
    result = ddl.read_delta(simple_table2, datetime="2020-05-25T22:47:31-07:00")
    assert_eq(expected, result)


def test_read_history(checkpoint_table):
    history = ddl.read_delta_history(checkpoint_table)
    assert len(history) == 26

    last_commit_info = history.iloc[0]
    last_commit_info == pd.json_normalize(
        {
            "commitInfo": {
                "timestamp": 1630942389906,
                "operation": "WRITE",
                "operationParameters": {"mode": "Append", "partitionBy": "[]"},
                "readVersion": 24.0,
                "isBlindAppend": True,
                "operationMetrics": {
                    "numFiles": "6",
                    "numOutputBytes": "5147",
                    "numOutputRows": "5",
                },
            }
        }
    )

    # check whether the logs are sorted
    current_timestamp = history.loc[0, "timestamp"]
    for h in history["timestamp"].tolist()[1:]:
        assert current_timestamp > h, "History Not Sorted"
        current_timestamp = h

    history = ddl.read_delta_history(checkpoint_table, limit=5)
    assert len(history) == 5


def test_vacuum(vacuum_table):
    print(vacuum_table)
    print(os.listdir(vacuum_table))
    tombstones = ddl.vacuum(vacuum_table, dry_run=True)
    print(tombstones)
    assert len(tombstones) == 4

    before_pq_files_len = len(glob.glob(f"{vacuum_table}/*.parquet"))
    assert before_pq_files_len == 7
    tombstones = ddl.vacuum(vacuum_table, dry_run=False)
    after_pq_files_len = len(glob.glob(f"{vacuum_table}/*.parquet"))
    assert after_pq_files_len == 3


def test_read_delta_with_error():
    with pytest.raises(ValueError) as exc_info:
        ddl.read_delta()
    assert str(exc_info.value) == "Please Provide Delta Table path"
