![Forklift](./artwork/logo_rendered.png)

# Welcome to Forklift
Forklift is an ETL toolbox for Spark and Airflow.

## What can Forklift do?
### Make an Excel file from dataframes
XLSBuilder builds an Excel file, with a tab for each Spark dataframe:

```python
xls = XLSBuilder("test4.xls")
df1 = sqlContext.read.parquet("hdfs:///data1")
xls.addTab(df1, "Data 1")
df2 = sqlContext.read.parquet("hdfs:///data2")
xls.addTab(df2, "Data 2")
xls.build()
```

N.B. XLSBuilder collects and writes dataframes on the master node of your Spark cluster. If you attempt to use large dataframes with XLSBuilder, undefined behavior may occur. This is not considered a bug, since desktop usage of Excel is itself limited to small, `n MB` .xls files. Therefore, XLSBuilder should be used with small dataframes.
