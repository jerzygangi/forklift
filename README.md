![Forklift](./artwork/logo_rendered.png)

# Welcome to Forklift
If you're building a 21st century data pipeline on Hadoop, and orchestrating it with Airflow, you'll quickly become overwhelmed. How do you move a table from Redshift to HDFS? How do you clean up data from S3? How do you convert from TSV to Parquet? How do you quickly rename columns? Forklift solves all of these problems. It's the first ETL ("Extract-Transform-Load") suite for Hadoop and Airflow. So sit back, clone the repo, and let Forklift do the heavy lifting.

# Setup
1. Run `pip install -e git+ssh://git@github.com/jerzygangi/forklift.git` on all nodes in the cluster
2. Launch `pyspark`
3. Type `from forklift import Forklift`
4. Have fun!

## What can Forklift do?
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
