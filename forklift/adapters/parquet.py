import .utilities.options_checker
from .adapters import Adapter
from .exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException

class ParquetAdapter(Adapter):

  @ensure_required_options_exist(["url", "select_query", "table_name_in_select_query"])
  def read(self, sql_context, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      print("Step 1: A parquet directory must end in .parquet")
        if not options["url"].endswith(".parquet"):
          raise CantLoadUsingThisAdapterException
      print("Step 2: Read in the Parquet directory as a temporary table")
      sql_context.read \
        .parquet(options["url"]) \
        .registerTempTable(options["table_name_in_select_query"])
      print("Step 3: Make a DataFrame by running the select query on the Parquet's temporary table")
      dataframe = sql_context.sql(options["select_query"])
      print("Step 4: Drop the temporary table of the Parquet directory")
      sql_context.dropTempTable(options["table_name_in_select_query"])
      print("Step 5: Return the queried Parquet directory as a DataFrame")
      return dataframe
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not load this Parquet directory into a DataFrame")
      raise CantReadUsingThisAdapterException

  @ensure_required_options_exist(["output_mode", "url"])
  def write(self, dataframe, **kwargs):
    options = kwargs["options"]
    
    # Try to use this adapter
    try:
      print("Step 1: A parquet directory must end in .parquet")
        if not options["url"].endswith(".parquet"):
          raise CantWriteUsingThisAdapterException
      print("Step 2: Write out the Parquet directory")
      dataframe.write \
        .option("compression", "none") \
        .mode(options["output_mode"]) \
        .parquet(options["url"])
      return
    # If it bombs for any reason, skip it!
    except:
      print("WARNING: Could not save this DataFrame to a Parquet directory")
      raise CantWriteUsingThisAdapterException

  @classmethod
  def read_options(klass):
    return ["url", "select_query", "table_name_in_select_query"]

  @classmethod
  def write_options(klass):
    return ["output_mode", "url"]
