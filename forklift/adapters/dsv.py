from ..utilities.options_checker import *
from ..utilities.read_sql_file import *
from . import Adapter
from ..warehouse.forklift_df import ForkliftDataFrame
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class DSVAdapter(Adapter):

  @ensure_required_options_exist(["delimiter", "has_header", "url", "select_query", "table_name_in_select_query"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:
      print("Step 1: The delimiter for a DSV file must be comma or tab")
      if options["delimiter"] not in [",", "\t"]:
        raise CantLoadUsingThisAdapterException 
      print("Step 2: Load the select query from a file, if necessary")
      if not isinstance(options["select_query"], (str, unicode)):
        print("WARNING: The select_query provided was not a string")
        raise CantReadUsingThisAdapterException
      select_query_as_string = None
      try:
        select_query_as_string = read_sql_file(options["select_query"])
        print("WARNING: The select_query was identified as a file")
      except StringIsNotAFileException:
        select_query_as_string = options["select_query"]
        print("WARNING: The select_query was identified as a SQL string")
      print("Step 3: Read in the DSV file as a temporary table")
      sql_context.read \
        .format('com.databricks.spark.csv') \
        .options(header=options["has_header"], delimiter=options["delimiter"], inferSchema="true", dateFormat="yyyy-MM-dd'T'HH:mm:ss") \
        .load(options["url"]) \
        .registerTempTable(options["table_name_in_select_query"])
      print("Step 4: Make a DataFrame by running the select query on the DSV's temporary table")
      dataframe = sql_context.sql(select_query_as_string)      
      print("Step 5: Drop the temporary table of the DSV file")
      sql_context.dropTempTable(options["table_name_in_select_query"])
      print("Step 6: Return the queried DSV file as a DataFrame")
      return dataframe
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not load this DSV into a DataFrame: {0}".format(e))
      raise CantReadUsingThisAdapterException(e)

  @ensure_required_options_exist(["output_mode", "url", "format"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]

    # Try to use this adapter
    try:
      # N.B. At this time, writing to CSV is the only DSV supported
      print("Step 1: The format must be CSV, or else we defer to the next Adapter in the flyweight")
      if options["format"] not in ["csv", "Csv", "CSV"]:
        raise CantWriteUsingThisAdapterException
      print("Step 2: Repartition the dataframe, if requested, by turning it into a Forklift DataFrame and safely coalescing it")
      if "partitions" in options.keys():
         dataframe = ForkliftDataFrame(dataframe)
         dataframe = dataframe.safely_coalesce(options["partitions"])
      print("Step 3: Write out the DSV file")
      dataframe.write \
        .format('com.databricks.spark.csv') \
        .mode(options["output_mode"]) \
        .save(options["url"])
      return
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not save this DataFrame to a DSV file: {0}".format(e))
      raise CantWriteUsingThisAdapterException(e)

  @classmethod
  def read_options(klass):
    return ["delimiter", "has_header", "url", "select_query", "table_name_in_select_query"]

  @classmethod
  def write_options(klass):
    return ["output_mode", "url", "OPTIONAL: partitions"]
