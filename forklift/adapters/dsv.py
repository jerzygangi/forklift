from ..utilities.options_checker import *
from . import Adapter
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class DSVAdapter(Adapter):

  @ensure_required_options_exist(["delimiter", "has_header", "delimiter", "url", "select_query", "table_name_in_select_query"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:
      print("Step 1: The delimiter for a DSV file must be comma or tab")
      if options["delimiter"] not in [",", "\t"]:
        raise CantLoadUsingThisAdapterException 
      print("Step 2: Read in the DSV file as a temporary table")
      sql_context.read \
        .format('com.databricks.spark.csv') \
        .options(header=options["has_header"], delimiter=options["delimiter"], inferSchema="true", dateFormat="yyyy-MM-dd'T'HH:mm:ss") \
        .load(options["url"]) \
        .registerTempTable(options["table_name_in_select_query"])
      print("Step 3: Make a DataFrame by running the select query on the DSV's temporary table")
      dataframe = sql_context.sql(options["select_query"])      
      print("Step 4: Drop the temporary table of the DSV file")
      sql_context.dropTempTable(options["table_name_in_select_query"])
      print("Step 5: Return the queried DSV file as a DataFrame")
      return dataframe
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not load this DSV into a DataFrame")
      raise CantReadUsingThisAdapterException

  @ensure_required_options_exist(["output_mode", "url", "format"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:
      # N.B. At this time, writing to CSV is the only DSV supported
      print("Step 1: The format must be CSV, or else we defer to the next Adapter in the flyweight")
      if not options["format"] in ["csv", "Csv", "CSV"]):
        raise CantWriteUsingThisAdapterException
      print("Step 2: Write out the DSV file")
      dataframe.write \
        .format('com.databricks.spark.csv') \
        .mode(options["output_mode"]) \
        .save(options["url"])
      return
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not save this DataFrame to a DSV file")
      raise CantWriteUsingThisAdapterException

  @classmethod
  def read_options(klass):
    return ["delimiter", "has_header", "delimiter", "url", "select_query", "table_name_in_select_query"]

  @classmethod
  def write_options(klass):
    return ["output_mode", "url"]