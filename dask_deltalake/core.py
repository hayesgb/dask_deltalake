import json
import os
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
from aiobotocore.session import get_session
from dask.base import tokenize
from dask.dataframe.core import new_dd_object
from dask.dataframe.io import from_delayed
from dask.dataframe.utils import make_meta
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from deltalake import DeltaTable
from deltalake.table import DeltaStorageHandler
from fsspec.core import get_fs_token_paths
from pyarrow import dataset as pa_ds


class DeltaTableWrapper(object):
    path: str
    version: int
    columns: List[str]
    datetime: str
    storage_options: Dict[str, any]

    def __init__(
        self,
        path: str,
        version: int,
        columns: List[str],
        datetime: Optional[str] = None,
        storage_options: Dict[str, str] = None,
        arrow_options: dict = {},
    ) -> None:
        self.path: str = path
        self.version: int = version
        self.columns = columns
        self.datetime = datetime
        self.storage_options = storage_options
        self.dt = DeltaTable(
            table_uri=self.path,
            version=self.version,
            storage_options=self.storage_options,
        )
        if self.datetime:
            self.dt.load_with_datetime(self.datetime)
        self.schema = self.dt.schema().to_pyarrow()
        self.arrow_options = arrow_options
        if self.path.startswith("s3://"):
            pyarrow_storage_options = {
                "access_key": storage_options["AWS_ACCESS_KEY_ID"],
                "secret_key": storage_options["AWS_SECRET_ACCESS_KEY"],
                "region": storage_options["AWS_REGION"],
            }
            _, normalized_path = pa_fs.FileSystem.from_uri(path)
            raw_fs = pa_fs.S3FileSystem(**pyarrow_storage_options)
            self.fs = pa_fs.SubTreeFileSystem(normalized_path, raw_fs)
        else:
            table_uri = "file://" + str(Path(self.path).absolute())
            self.fs = pa_fs.PyFileSystem(DeltaStorageHandler(table_uri))

    def read_delta_dataset(self, columns: list = None, filter: list = None):
        dataset = self.dt.to_pyarrow_dataset(filesystem=self.fs)

        if filter:
            filter = pq.filters_to_expression(filter)
        batches = dataset.to_batches(columns=columns, filter=filter)
        return [b for b in batches if b.num_rows > 0]

    def make_meta_from_schema(self) -> Dict[str, str]:
        meta = self.schema.empty_table().to_pandas(**self.arrow_options)
        if self.columns:
            cols = meta.columns.tolist()
            cols = [c for c in cols if c not in self.columns]
            meta = meta.drop(columns=cols)
        return make_meta(meta)

    def history(self, limit: Optional[int] = None, **kwargs) -> dd.core.DataFrame:
        history_ = self.dt.history()
        df = (
            pd.json_normalize(history_)
            .sort_values(by="timestamp", ascending=False)
            .reset_index(drop=True)
        )
        if limit:
            df = df[df.index < limit]
        cols = [
            "timestamp",
            "operation",
            "operationParameters.mode",
            "operationMetrics.numFiles",
            "operationMetrics.numOutputBytes",
            "operationMetrics.numOutputRows",
            "operationParameters.partitionBy",
            "readVersion",
            "isBlindAppend",
        ]
        return df[cols]

    def _vacuum_helper(self, filename_to_delete: str) -> None:
        full_path = urlparse(self.path)
        if full_path.scheme and full_path.netloc:  # pragma no cover
            # for different storage backend, delta-rs vacuum gives path to the file
            # it will not provide bucket name and scheme s3 or gcfs etc. so adding
            # manually
            filename_to_delete = (
                f"{full_path.scheme}://{full_path.netloc}/{filename_to_delete}"
            )
        self.fs.delete_file(filename_to_delete)

    def vacuum(self, retention_hours: int = 168, dry_run: bool = True) -> None:
        """
        Run the Vacuum command on the Delta Table: list and delete files no
        longer referenced by the Delta table and are older than the
        retention threshold.

        retention_hours: the retention threshold in hours, if none then
        the value from `configuration.deletedFileRetentionDuration` is used
         or default of 1 week otherwise.
        dry_run: when activated, list only the files, delete otherwise

        Returns
        -------
        the list of files no longer referenced by the Delta Table and are
         older than the retention threshold.
        """

        tombstones = self.dt.vacuum(retention_hours=retention_hours)

        if dry_run:
            return tombstones
        else:
            parts = [
                delayed(
                    self._vacuum_helper,
                    name="delta-vacuum-"
                    + tokenize(self.fs, f, retention_hours, dry_run),
                )(f)
                for f in tombstones
            ]
        dask.compute(parts)[0]


def _fetch_batches(chunks, arrow_options: dict):
    return pd.concat([chunk.to_pandas(**arrow_options) for chunk in chunks], axis=1)


def read_delta(
    path: Optional[str] = None,
    table_name: str = None,
    version: int = None,
    columns: List[str] = None,
    filter: List[tuple] = None,
    storage_options: Dict[str, str] = None,
    datetime: str = None,
    arrow_options: dict = {},
    **kwargs,
):
    """
    Read a Delta Table into a Dask DataFrame

    This reads a list of Parquet files in delta table directory into a
    Dask.dataframe.

    Parameters
    ----------
    path: Optional[str]
        path of Delta table directory
    version: int, default None
        DeltaTable Version, used for Time Travelling across the
        different versions of the parquet datasets
    datetime: str, default None
        Time travel Delta table to the latest version that's created at or
        before provided `datetime_string` argument.
        The `datetime_string` argument should be an RFC 3339 and ISO 8601 date
         and time string.

        Examples:
        `2018-01-26T18:30:09Z`
        `2018-12-19T16:39:57-08:00`
        `2018-01-26T18:30:09.453+00:00`
         #(copied from delta-rs docs)
    columns: None or list(str)
        Columns to load. If None, loads all.
    storage_options : dict, default None
        Key/value pairs to be passed on to the file-system backend, if any.
    kwargs: dict,optional
        Some most used parameters can be passed here are:
        1. schema
        2. filter

        schema : pyarrow.Schema
            Used to maintain schema evolution in deltatable.
            delta protocol stores the schema string in the json log files which is
            converted into pyarrow.Schema and used for schema evolution
            i.e Based on particular version, some columns can be
            shown or not shown.

        filter: Union[List[Tuple[str, str, Any]], List[List[Tuple[str, str, Any]]]], default None
            List of filters to apply, like ``[('col1', '==', 0), ...], ...]``.
            Can act as both partition as well as row based filter:
                [("x", ">" 400)] --> pyarrow.dataset.field("x") > 400

    Returns
    -------
    Dask.DataFrame

    Examples
    --------
    >>> ddf = dd.read_delta('s3://bucket/my-delta-table')  # doctest: +SKIP

    """

    if path is None:
        raise ValueError("Please Provide Delta Table path")

    label = "read-deltalake-"
    output_name = label + tokenize(
        path,
        storage_options,
        table_name,
        version,
        columns,
        datetime,
    )
    dtw = DeltaTableWrapper(
        path=path,
        version=version,
        columns=columns,
        storage_options=storage_options,
        datetime=datetime,
        arrow_options=arrow_options,
    )

    batches = dtw.read_delta_dataset(columns, filter)

    meta = dtw.make_meta_from_schema()

    if not batches:
        meta = dd.utils.make_meta(meta)
        graph = {(output_name, 0): meta}
        divisions = (None, None)
        return new_dd_object(graph, output_name, meta, divisions)

    layer = DataFrameIOLayer(
        output_name,
        meta.columns,
        batches,
        partial(_fetch_batches, arrow_options=arrow_options),
        label=label,
    )

    divisions = tuple([None] * (len(batches) + 1))
    graph = HighLevelGraph({output_name: layer}, {output_name: set()})
    return new_dd_object(graph, output_name, meta, divisions)


def read_delta_history(
    path: str, limit: Optional[int] = None, storage_options: Dict[str, str] = None
) -> dd.core.DataFrame:
    """
    Run the history command on the DeltaTable.
    The operations are returned in reverse chronological order.

    Reads the delta log json files into a DataFrame and returns the commitInfo
    as a Pandas DataFrame, or optionally, a Dask DataFrame, in reverse chronological order

    Parameters
    ----------
    path: str
        path of Delta table directory
    limit: int, default None
        the commit info limit to return, defaults to return all history

    Returns
    -------
         A DataFrame of the commit infos registered in the transaction log
    """

    dtw = DeltaTableWrapper(
        path=path, version=None, columns=None, storage_options=storage_options
    )
    return dtw.history(limit=limit)


def vacuum(
    path: str,
    retention_hours: int = 168,
    dry_run: bool = True,
    storage_options: Dict[str, str] = None,
) -> None:
    """
    Run the Vacuum command on the Delta Table: list and delete
    files no longer referenced by the Delta table and are
    older than the retention threshold.

    retention_hours: int, default 168
    the retention threshold in hours, if none then the value
    from `configuration.deletedFileRetentionDuration` is used
    or default of 1 week otherwise.
    dry_run: bool, default True
        when activated, list only the files, delete otherwise

    Returns
    -------
    None or List of tombstones
    i.e the list of files no longer referenced by the Delta Table
    and are older than the retention threshold.
    """

    dtw = DeltaTableWrapper(
        path=path, version=None, columns=None, storage_options=storage_options
    )
    return dtw.vacuum(retention_hours=retention_hours, dry_run=dry_run)
