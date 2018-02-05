from ..utilities.options_checker import *
from ..utilities.read_sql_file import *
from . import Adapter
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class PostgreSQLAdapter(Adapter):

  @ensure_required_options_exist(["jdbc_connection_string", "sql_select_query", "username", "password"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      print("Step 1: Load the select query from a file, if necessary")
      if not isinstance(options["sql_select_query"], (str, unicode)):
        print("WARNING: The sql_select_query provided was not a string")
        raise CantReadUsingThisAdapterException
      select_query_as_string = None
      try:
        select_query_as_string = read_sql_file(options["sql_select_query"])
        print("WARNING: The sql_select_query was identified as a file")
      except StringIsNotAFileException:
        select_query_as_string = options["sql_select_query"]
        print("WARNING: The sql_select_query was identified as a SQL string")
      print("Step 2: Prepare the JDBC connection to PostgreSQL")
      postgres_jdbc_options = {}
      postgres_jdbc_options["url"] = options["jdbc_connection_string"]
      postgres_jdbc_options["user"] = options["username"]
      postgres_jdbc_options["password"] = options["password"]
      postgres_jdbc_options["driver"] = "org.postgresql.Driver"
      postgres_jdbc_options["mode"] = "error"
      # For executing a query on a JDBC connection, view syntax instructions:
      # 1) https://docs.databricks.com/spark/latest/data-sources/sql-databases.html#pushdown-an-entire-query
      # 2) http://stackoverflow.com/questions/34365692/spark-sql-load-data-with-jdbc-using-sql-statement-not-table-name
      print("Step 3: If partitioning was requested, calculate partitioning for the JDBC connection")
      if(options["partition_column"] and isinstance(options["partition_column"], (str, unicode))):
        minimum, maximum, count = sql_context.read \
          .format("jdbc") \
          .options(**postgres_jdbc_options) \
          .option("dbtable", "(SELECT min({partition_column}) AS minimum_pk, max({partition_column}) AS maximum_pk, count(*) AS count_pk FROM ({query}) rltion) AS tmp".format(partition_column=options["partition_column"], query=select_query_as_string)) \
          .load()
          .collect()[0]
        import math
        fetch_size = 10000
        computed_partitions = int(math.ceil(count*1.0/fetch_size*1.0))
        postgres_jdbc_options["fetchsize"] = fetch_size
        postgres_jdbc_options["lowerBound"] = minimum
        postgres_jdbc_options["upperBound"] = maximum
        postgres_jdbc_options["partitionColumn"] = options["partition_column"]
        postgres_jdbc_options["numPartitions"] = computed_partitions
      print("Step 4: Read the PostgreSQL query into a DataFrame")
      return sql_context.read \
        .format("jdbc") \
        .options(**postgres_jdbc_options) \
        .option("dbtable", "({0}) AS tmp".format(select_query_as_string)) \
        .load()
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not load this PostgreSQL query into a DataFrame: {0}".format(e))
      raise CantReadUsingThisAdapterException(e)

  @ensure_required_options_exist(["jdbc_connection_string", "table_name", "output_mode", "username", "password"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      print("Step 1: Write out the PostgreSQL table")
      dataframe.write.jdbc( \
        url=options["jdbc_connection_string"], \
        table=options["table_name"], \
        mode=options["output_mode"], \
        properties={'user': options["username"], 'password': options["password"], 'driver': 'org.postgresql.Driver'} \
      )
      return
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not save this PostgreSQL table to a DataFrame: {0}".format(e))
      raise CantWriteUsingThisAdapterException(e)

  @classmethod
  def read_options(klass):
    return ["jdbc_connection_string", "sql_select_query", "username", "password"]

  @classmethod
  def write_options(klass):
    return ["jdbc_connection_string", "table_name", "output_mode", "username", "password"]
