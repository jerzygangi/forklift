from ..utilities.options_checker import *
from ..utilities.read_sql_file import *
from . import Adapter
from ..warehouse.forklift_df import ForkliftDataFrame
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class ParquetAdapter(Adapter):

  @ensure_required_options_exist(["url", "select_query", "table_name_in_select_query"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      print("Step 1: Read in the Parquet directory as a temporary table")
      sql_context.read \
        .parquet(options["url"]) \
        .registerTempTable(options["table_name_in_select_query"])
      print("Step 2: Load the select query from a file, if necessary")
      if not isinstance(options["select_query"], (str, unicode)):
        print(options["select_query"].__class__)
        print("WARNING: The select_query provided was not a string")
        raise CantReadUsingThisAdapterException
      select_query_as_string = None
      try:
        select_query_as_string = read_sql_file(options["select_query"])
        print("WARNING: The select_query was identified as a file")
      except StringIsNotAFileException:
        select_query_as_string = options["select_query"]
        print("WARNING: The select_query was identified as a SQL string")
      print("Step 3: Make a DataFrame by running the select query on the Parquet's temporary table")
      dataframe = sql_context.sql(select_query_as_string)
      print("Step 3: Drop the temporary table of the Parquet directory")
      sql_context.dropTempTable(options["table_name_in_select_query"])
      print("Step 4: Return the queried Parquet directory as a DataFrame")
      return dataframe
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not load this Parquet directory into a DataFrame: {0}".format(e))
      raise CantReadUsingThisAdapterException(e)

  @ensure_required_options_exist(["output_mode", "url", "format"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      print("Step 1: The format must be Parquet, or else we defer to the next Adapter in the flyweight")
      if options["format"] not in ["parquet", "Parquet", "PARQUET"]:
        raise CantWriteUsingThisAdapterException
      print("Step 2: Repartition the dataframe, if requested, by turning it into a Forklift DataFrame and safely coalescing it")
      if "partitions" in options.keys():
         dataframe = ForkliftDataFrame(dataframe)
         dataframe = dataframe.safely_coalesce(options["partitions"])
      print("Step 3: Write out the Parquet directory")
      
      dataframe_writer = dataframe.write \
        .option("compression", "none") \
        .mode(options["output_mode"]) \  

      # partition by the supplied column. 
      if "partition_by" in options.keys():
        dataframe_writer = dataframe_writer.partitionBy(options["partition_by"])
      dataframe_writer.parquet(options["url"])
      return
    # If it bombs for any reason, skip it!
    except Exception as e:
      print("WARNING: Could not save this DataFrame to a Parquet directory: {0}".format(e))
      raise CantWriteUsingThisAdapterException(e)

  @classmethod
  def read_options(klass):
    return ["url", "select_query", "table_name_in_select_query"]

  @classmethod
  def write_options(klass):
    return ["url", "output_mode", "format", "OPTIONAL: partitions", "partition_by"]
