![Forklift](./artwork/logo_rendered.png)

# Welcome to Forklift
If you're building a 21st century data pipeline on Hadoop, and orchestrating it with Airflow, you'll quickly become overwhelmed. How do you move a table from Redshift to HDFS? How do you clean up data from S3? How do you convert from TSV to Parquet? How do you quickly rename columns? Forklift solves all of these problems. It's the first ETL ("Extract-Transform-Load") suite for Hadoop and Airflow. So sit back, clone the repo, and let Forklift do the heavy lifting.

# Setup
1. Run `pip install -e git+ssh://git@github.com/jerzygangi/forklift.git` on all nodes in the cluster
2. Launch `pyspark`
3. Type `from forklift import Forklift`
4. Have fun!

## What can Forklift do?
### Move data around
A common ETL task is to move data between stores your warehouse or lake. HDFS, Redshift, PostgreSQL, S3, CSV/TSV, and Parquet are all natively supported. Watch how easy Forklift.move() makes it to *Move Data*:

```python
# Let's move some data from Redshift into HDFS
move_from = {"url": "jdbc:redshift://db1.example.com/cars"}
move_to = {"url": "hdfs:///warehouse/cars.parquet"}

# Make a forklift, and move it
from forklift import Forklift
Forklift(sqlContext).move(move_from, move_to)
```

A list of arguments to pass to each warehouse adapter can be found by:

```python
# For CSV and TSV files
from forklift.adapters.dsv import DSVAdapter
DSVAdapter.read_options()
DSVAdapter.write_options()

# For CSV and TSV files
from forklift.adapters.parquet import ParquetAdapter
ParquetAdapter.read_options()
ParquetAdapter.write_options()

# For CSV and TSV files
from forklift.adapters.postgresql import PostgreSQLAdapter
PostgreSQLAdapter.read_options()
PostgreSQLAdapter.write_options()

# For CSV and TSV files
from forklift.adapters.redshift import RedshiftAdapter
RedshiftAdapter.read_options()
RedshiftAdapter.write_options()
```

### Read & write dataframes
The same engine, Warehouse, that powers Forklift.move() can be used to quickly *Read and Write DataFrames*:

```python
# Tell Forklift where to save the DataFrame
save_to = {"url": "hdfs:///warehouse/cars.parquet"}

# And save it
from forklift.warehouse import Warehouse
Warehouse().write(my_dataframe, save_to)
```

### Sanitize and normalize a dataframe
Another common ETL task is force a myriad schemas into a common schema, and cast datatypes along the way. In Forklift, this process is called *Normalize and Sanitize*:

```bash
# Define what columns to include, and what to name them
cat ~/my_remappings.json
{
  "remappings":{
    "timestamp": "date",
    "url": "domain",
    "clicks": "clicks"
  }
}
```

and in `pyspark`:

```python
# Create a dataframe
df1 = sqlContext.read.parquet("hdfs:///website_data.parquet")

# Create a Spark output schema
from pyspark.sql.types import *
def my_schema():
  return StructType([
    StructField("date", DateType(), True),
    StructField("domain", StringType(), True),
    StructField("clicks", LongType(), True)
  ])

# Tell Forklift how you want to cast datatypes
from forklift.cell_caster import CastProcessor
class MyCaster(CastProcessor):
	def cast_all_cells(self, value):
		if isinstance(value, float):
			return Decimal(value)
		else:
			return value
	def cast_domain(self, domain):
		return "http://" + domain

# Normalize and sanitize the dataframe
from forklift import Forklift
from forklift.stages import *
fork = Forklift(sqlContext)
df1_normalized_and_sanitized = fork.normalize_and_sanitize(df1, my_schema(), "~/my_remappings.json", MyCaster, [NS_ALL])
```

### Make an Excel file from dataframes
XLSBuilder builds an Excel file, with a tab for each Spark dataframe:

```python
xls = XLSBuilder("my_file.xls")
df1 = sqlContext.read.parquet("hdfs:///data1")
xls.addTab(df1, "Data 1")
df2 = sqlContext.read.parquet("hdfs:///data2")
xls.addTab(df2, "Data 2")
xls.build()
```

N.B. XLSBuilder collects and writes dataframes on the master node of your Spark cluster. If you attempt to use large dataframes with XLSBuilder, undefined behavior may occur. This is not considered a bug, since desktop usage of Excel is itself limited to small, `n MB` .xls files. Therefore, XLSBuilder should be used with small dataframes.
