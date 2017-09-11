from ..utilities.options_checker import *
from ..utilities.read_sql_file import *
from . import Adapter
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class SparkSQLAdapter(Adapter):

  @ensure_required_options_exist(["sql_select_query"])
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
      print("Step 2: Read the SparkSQL query into a DataFrame")
      return sql_context.sql(select_query_as_string)
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not load this SparkSQL query into a DataFrame: {0}".format(e))
      raise CantReadUsingThisAdapterException(e)

  @ensure_required_options_exist(["table_name", "output_mode"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:

      if "jdbc_connection_string" in options:
        raise Exception("jdbc_connection_string is not an allowed param for SparkSQL adapter")

      print("Step 1: Write out the DataStore to SparkSQL")
      dataframe.write \
        .mode(options["output_mode"]) \
        .saveAsTable(options["table_name"])
      return
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not save this SparkSQL table: {0}".format(e))
      raise CantWriteUsingThisAdapterException(e)

  @classmethod
  def read_options(klass):
    return ["sql_select_query"]

  @classmethod
  def write_options(klass):
    return ["table_name", "output_mode"]
