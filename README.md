## Dask Deltalake
Reads and write to deltalake from Dask leveraging delta-rs

## Dask Deltalake Reader

Reads data from Deltalake with Dask

To Try out the package:

```
pip install dask_deltalake
```

### Features:
1. Reads the parquet files based on delta logs parallely using dask engine
2. Supports all three filesystem like s3, azurefs, gcsfs
3. Supports some delta features like
   - Time Travel
   - Schema evolution
   - parquet filters
     - row filter
     - partition filter
4. Query Delta commit info - History
5. vacuum the old/ unused parquet files
6. load different versions of data using datetime.

### Usage:

```
import dask_deltalake as ddl

# read delta table
ddl.read_delta("delta_path")

# read delta table for specific version
ddl.read_delta("delta_path",version=3)

# read delta table for specific datetime
ddl.read_delta("delta_path",datetime="2018-12-19T16:39:57-08:00")


# read delta complete history
ddl.read_delta_history("delta_path")

# read delta history upto given limit
ddl.read_delta_history("delta_path",limit=5)

# read delta history to delete the files
ddl.vacuum("delta_path",dry_run=False)

# Can read from S3,azure,gcfs etc.
ddl.read_delta("s3://bucket_name/delta_path",version=3)
# please ensure the credentials are properly configured as environment variable or
# configured as in ~/.aws/credential

# can connect with AWS Glue catalog and read the complete delta table (currently only AWS catalog available)
# will take expilicit AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from environment
# variables if available otherwise fallback to ~/.aws/credential
ddl.read_delta(catalog=glue,database_name="science",table_name="physics")

```
