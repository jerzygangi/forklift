from ..utilities.options_checker import *
from ..utilities.read_sql_file import *
from . import Adapter
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class RedshiftAdapter(Adapter):

  @ensure_required_options_exist(["jdbc_connection_string", "sql_select_query", "s3_temp_directory"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:
      print("Step 1: Load the select query from a file, if necessary")
      if not isinstance(options["sql_select_query"], str):
        raise CantReadUsingThisAdapterException
      select_query_as_string = None
      try:
        select_query_as_string = read_sql_file(options["sql_select_query"])
      except StringIsNotAFileException:
        select_query_as_string = options["sql_select_query"]
      print("Step 2: Read the Redshift query into a DataFrame")
      return sql_context.read \
        .format("com.databricks.spark.redshift") \
        .option("url", options["jdbc_connection_string"]) \
        .option("query", select_query_as_string) \
        .option("tempdir", options["s3_temp_directory"]) \
        .load()
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not load this Redshift query into a DataFrame")
      raise CantReadUsingThisAdapterException

  @ensure_required_options_exist(["jdbc_connection_string", "table_name", "s3_temp_directory", "output_mode"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:
      print("Step 1: Write out the Redshift table")
      dataframe.write \
        .format("com.databricks.spark.redshift") \
        .option("url", options["jdbc_connection_string"]) \
        .option("dbtable", options["table_name"]) \
        .option("tempdir", options["s3_temp_directory"]) \
        .mode(options["output_mode"]) \
        .save()
      return
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not save this Redshift table to a DataFrame")
      raise CantWriteUsingThisAdapterException

  @classmethod
  def read_options(klass):
    return ["jdbc_connection_string", "sql_select_query", "s3_temp_directory"]

  @classmethod
  def write_options(klass):
    return ["jdbc_connection_string", "table_name", "s3_temp_directory", "output_mode"]
