import .utilities.options_checker
from .adapters import Adapter
from .exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class PostgreSQLAdapter(Adapter):

  @ensure_required_options_exist(["jdbc_connection_string", "sql_select_query", "username", "password"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      # For executing a query on a JDBC connection, view syntax instructions:
      # 1) https://docs.databricks.com/spark/latest/data-sources/sql-databases.html#pushdown-an-entire-query
      # 2) http://stackoverflow.com/questions/34365692/spark-sql-load-data-with-jdbc-using-sql-statement-not-table-name
      print("Step 1: Read the PostgreSQL query into a DataFrame")
      return sql_context.read \
        .format("jdbc") \
        .option("url", options["jdbc_connection_string"]) \
        .option("dbtable", "({0}) AS tmp".format(options["sql_select_query"])) \
        .option("user", options["username"]) \
        .option("password", options["password"]) \
        .option("mode", "error") \
        .load()
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not load this PostgreSQL query into a DataFrame")
      raise CantReadUsingThisAdapterException

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
        properties={'user': options["username"], 'password': options["password"]} \
      )
      return
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not save this PostgreSQL table to a DataFrame")
      raise CantWriteUsingThisAdapterException
