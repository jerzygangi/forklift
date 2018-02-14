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
      print("Step 3: Ensure the __forklift_tmp temporary Forklift schema exists in PostgreSQL; if not, create it")
      sql_context.read \
        .format("jdbc") \
        .options(**postgres_jdbc_options) \
        .option("dbtable", "(CREATE SCHEMA IF NOT EXISTS __forklift_tmp) AS tmp") \
        .load()        
      print("Step 4: Create a new table from the query, in the __forklift_tmp schema, in PostgreSQL")
      import random
      import string
      table_with_forklift_pk_name = "".join([random.choice(string.ascii_lowercase) for n in xrange(32)])
      sql_context.read \
        .format("jdbc") \
        .options(**postgres_jdbc_options) \
        .option("dbtable", "(CREATE TABLE __forklift_tmp.{table_with_forklift_pk_name} AS ({query})) AS tmp".format(table_with_forklift_pk_name=table_with_forklift_pk_name, query=select_query_as_string)) \
        .load()
      print("Step 5: Create a Forklift primary key on the new table from the query in PostgreSQL")
      sql_context.read \
        .format("jdbc") \
        .options(**postgres_jdbc_options) \
        .option("dbtable", "(ALTER TABLE __forklift_tmp.{table_with_forklift_pk_name} ADD COLUMN __forklift_pk SERIAL PRIMARY KEY) AS tmp".format(table_with_forklift_pk_name=table_with_forklift_pk_name)) \
        .load()
      print("Step 6: Calculate partitioning for the new table")
        minimum, maximum, count = sql_context.read \
          .format("jdbc") \
          .options(**postgres_jdbc_options) \
          .option("dbtable", "(SELECT min(__forklift_pk) AS minimum_pk, max(__forklift_pk) AS maximum_pk, count(*) AS count_pk FROM __forklift_tmp.{table_with_forklift_pk_name}) AS tmp".format(table_with_forklift_pk_name=table_with_forklift_pk_name)) \
          .load() \
          .collect()[0]
        import math
        fetch_size = 1000
        computed_partitions = int(math.ceil(count*1.0/fetch_size*1.0)) or 1
        postgres_jdbc_options["fetchsize"] = fetch_size
        postgres_jdbc_options["lowerBound"] = minimum or 0
        postgres_jdbc_options["upperBound"] = maximum or fetch_size
        postgres_jdbc_options["partitionColumn"] = "__forklift_pk"
        postgres_jdbc_options["numPartitions"] = computed_partitions
      print("Step 7: Read the new table into a DataFrame using partitioning on the Forklift primary key")
      return sql_context.read \
        .format("jdbc") \
        .options(**postgres_jdbc_options) \
        .option("dbtable", "(SELECT * FROM __forklift_tmp.{table_with_forklift_pk_name}) AS tmp".format(table_with_forklift_pk_name=table_with_forklift_pk_name)) \
        .load() \
        .drop('__forklift_pk')
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
