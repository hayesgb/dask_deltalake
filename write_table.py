import dask.dataframe as dd
import dask_deltalake as ddl
from dask.datasets import timeseries
from distributed import Client


def to():
    ACCESS_KEY="AKIA3POV22Q4GBGTGGYR"
    SECRET_KEY="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"
    REGION="us-east-2"
    AWS_S3_ALLOW_UNSAFE_RENAME="true"
    STORAGE_OPTIONS={"AWS_ACCESS_KEY_ID": ACCESS_KEY, "AWS_SECRET_ACCESS_KEY": SECRET_KEY, 
    "AWS_REGION": REGION, "AWS_S3_ALLOW_UNSAFE_RENAME": AWS_S3_ALLOW_UNSAFE_RENAME}
    ddf = timeseries()
    ddf = ddf.reset_index()
    ddl.to_delta(ddf, "s3://hayesgb-scratch/test_delta/run_12.delta",
    storage_options=STORAGE_OPTIONS,
    )

if __name__ == "__main__":
    c = Client()
    to()
    c.close()