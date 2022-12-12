from __future__ import annotations

import contextlib
import json
import uuid
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dask
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
from dask.base import tokenize
from dask.dataframe.io.utils import _is_local_fs
from dask.delayed import delayed
from deltalake import DeltaTable
from deltalake._internal import DeltaFileSystemHandler, write_new_deltalake
from deltalake.fs import DeltaStorageHandler
# from deltalake.schema import delta_arrow_schema_from_pandas
from deltalake.table import (MAX_SUPPORTED_WRITER_VERSION, DeltaTable,
                             DeltaTableProtocolError, PyDeltaTableError)
from deltalake.writer import AddAction  # get_partitions_from_path,
from deltalake.writer import (DeltaJSONEncoder, get_file_stats_from_metadata,
                              try_get_deltatable)
from fsspec.core import get_fs_token_paths
from fsspec.spec import AbstractFileSystem

PYARROW_MAJOR_VERSION = int(pa.__version__.split(".", maxsplit=1)[0])


__all__ = "to_delta"

NONE_LABEL = "__null_dask_index__"


def delta_arrow_schema_from_pandas(
    data: "pd.DataFrame",
) -> Tuple[pa.Table, pa.Schema]:
    """
    Infers the schema for the delta table from the Pandas DataFrame.
    Necessary because of issues such as:  https://github.com/delta-io/delta-rs/issues/686
    :param data: Data to write.
    :return: A PyArrow Table and the inferred schema for the Delta Table
    """

    table = pa.Table.from_pandas(data)
    schema = table.schema
    schema_out = []
    for field in schema:
        if isinstance(field.type, pa.TimestampType):
            f = pa.field(
                name=field.name,
                type=pa.timestamp("us"),
                nullable=field.nullable,
                metadata=field.metadata,
            )
            schema_out.append(f)
        else:
            schema_out.append(field)
    schema = pa.schema(schema_out, metadata=schema.metadata)
    data = pa.Table.from_pandas(data, schema=schema)
    return data, schema


def get_partitions_from_path(path: str) -> Tuple[str, Dict[str, Optional[str]]]:
    if path[0] == "/":
        path = path[1:]
    parts = path.split("/")
    parts.pop()  # remove filename
    out: Dict[str, Optional[str]] = {}
    parts = [p for p in parts if "=" in p]
    for part in parts:
        key, value = part.split("=", maxsplit=1)
        if value == "__HIVE_DEFAULT_PARTITION__":
            out[key] = None
        else:
            out[key] = value
    return path, out


def _write_dataset(
    df,
    table_uri,
    fs,
    schema,
    partitioning,
    mode,
    storage_options,
    file_options,
    current_version,
):
    """ """
    data = pa.Table.from_pandas(df, schema=schema)
    add_actions: List[AddAction] = []

    def visitor(written_file: Any) -> None:
        path, partition_values = get_partitions_from_path(written_file.path)
        stats = get_file_stats_from_metadata(written_file.metadata)

        # PyArrow added support for written_file.size in 9.0.0
        if PYARROW_MAJOR_VERSION >= 9:
            size = written_file.size
        else:
            size = fs.get_file_info([path])[0].size  # type: ignore

        add_actions.append(
            AddAction(
                path,
                size,
                partition_values,
                int(datetime.now().timestamp()),
                True,
                json.dumps(stats, cls=DeltaJSONEncoder),
            )
        )

    ds.write_dataset(
        data=data,
        base_dir="/",
        basename_template=f"{current_version + 1}-{uuid.uuid4()}-{{i}}.parquet",
        format="parquet",
        partitioning=partitioning,
        schema=schema,
        file_visitor=visitor,
        existing_data_behavior="overwrite_or_ignore",
        filesystem=fs,
    )
    return add_actions


def to_delta(
    df: dd.DataFrame,
    table_or_uri: Union[str, DeltaTable],
    schema: Optional[pa.Schema] = None,
    partition_by: Optional[List[str]] = None,
    fs: Optional[pa_fs.FileSystem] = None,
    mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    storage_options: Optional[Dict[str, str]] = None,
    compute_kwargs={},
    file_options=None,
    overwrite_schema: bool = False,
    name: str = "",
    description: str = "",
    configuration: dict = {},
) -> None:

    """
    Store Dask.dataframe to Parquet files

    Notes
    -----
    Each partition will be written to a separate file.
    Parameters
    ----------
    df : dask.dataframe.DataFrame
    table_or_uri : string or pathlib.Path
        Destination directory for data.  Prepend with protocol like ``s3://``
        or ``hdfs://`` for remote data.
    schema : pyarrow.Schema, dict, "infer", or None, default "infer"
        Global schema to use for the output dataset. Defaults to "infer", which
        will infer the schema from the dask dataframe metadata. This is usually
        sufficient for common schemas, but notably will fail for ``object``
        dtype columns that contain things other than strings. These columns
        will require an explicit schema be specified. The schema for a subset
        of columns can be overridden by passing in a dict of column names to
        pyarrow types (for example ``schema={"field": pa.string()}``); columns
        not present in this dict will still be automatically inferred.
        Alternatively, a full ``pyarrow.Schema`` may be passed, in which case
        no schema inference will be done. Passing in ``schema=None`` will
        disable the use of a global file schema - each written file may use a
        different schema dependent on the dtypes of the corresponding
        partition. Note that this argument is ignored by the "fastparquet"
        engine.
    partition_by : list, default None
        Construct directory-based partitioning by splitting on these fields'
        values. Each dask partition will result in one or more datafiles,
        there will be no global groupby.
    fs:
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    **kwargs :
        Extra options to be passed on to the specific backend.
    """

    # We use Arrow to write the dataset.
    # See https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pandas
    # for how pyarrow handles the index.

    if df._meta.index.name is not None:
        raise DeltaTableProtocolError(
            "Unable to write table with index.  Call `reset_index()` first!"
        )

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    meta, schema = delta_arrow_schema_from_pandas(df.head())

    if isinstance(table_or_uri, str):
        if "://" in table_or_uri:
            table_uri = table_or_uri

        else:
            # Non-existant local paths are only accepted as fully-qualified URIs
            table_uri = "file://" + str(Path(table_or_uri).absolute())
            fs = pa_fs.PyFileSystem(DeltaStorageHandler(table_uri))

        if storage_options:
            if table_uri.startswith("s3://"):
                pyarrow_storage_options = {
                    "access_key": storage_options["AWS_ACCESS_KEY_ID"],
                    "secret_key": storage_options["AWS_SECRET_ACCESS_KEY"],
                    "region": storage_options["AWS_REGION"],
                }
            _, normalized_path = pa_fs.FileSystem.from_uri(table_uri)
            raw_fs = pa_fs.S3FileSystem(**pyarrow_storage_options)
            fs = pa_fs.SubTreeFileSystem(normalized_path, raw_fs)

        try:
            table = try_get_deltatable(table_uri, storage_options)
        except Exception:
            raise PyDeltaTableError("Failed to find table.  Confirm storage_options")

    elif isinstance(table_or_uri, DeltaTable):
        table = table_or_uri
        table_uri = table._table.table_uri()
    else:
        raise RuntimeError

    if table:  # already exists
        storage_options = table._storage_options or {}
        storage_options.update(storage_options or {})
        if schema != table.schema().to_pyarrow() and not (
            mode == "overwrite" and overwrite_schema
        ):
            raise ValueError(
                "Schema of data does not match table schema\n"
                f"Table schema:\n{schema}\nData Schema:\n{table.schema().to_pyarrow()}"
            )

        if mode == "error":
            raise AssertionError("DeltaTable already exists.")
        elif mode == "ignore":
            return

        current_version = table.version()

        if partition_by:
            assert partition_by == table.metadata().partition_columns

        if table.protocol().min_writer_version > MAX_SUPPORTED_WRITER_VERSION:
            raise DeltaTableProtocolError(
                "This table's min_writer_version is "
                f"{table.protocol().min_writer_version}, "
                "but this method only supports version 2."
            )
    else:  # creating a new table
        current_version = -1

    if partition_by:
        partition_schema = pa.schema([schema.field(name) for name in partition_by])
        partitioning = ds.partitioning(partition_schema, flavor="hive")
    else:
        partitioning = None

    annotations = dask.config.get("annotations", {})
    if "retries" not in annotations and not _is_local_fs(fs):
        ctx = dask.annotate(retries=5)
    else:
        ctx = contextlib.nullcontext()

    with ctx:
        dfs = df.to_delayed()
        results = [
            delayed(_write_dataset, name="write-deltalake-" + tokenize(table, df))(
                df,
                table_uri,
                fs,
                schema,
                partitioning,
                mode,
                storage_options,
                file_options,
                current_version,
            )
            for df in dfs
        ]

    results = dask.compute(*results, **compute_kwargs)
    add_actions = list(chain.from_iterable(results))

    if table is None:
        write_new_deltalake(
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by or [],
            name,
            description,
            configuration,
            storage_options,
        )
    else:
        table._table.create_write_transaction(
            add_actions,
            mode,
            partition_by or [],
            schema,
        )
