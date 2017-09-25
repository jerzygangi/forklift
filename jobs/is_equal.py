#
# is_equal one_forklift_read_options another_forklift_read_options [options]
# Compares one DataFrame with another, on your certain criteria
#
# Examine STDOUT for EQUALITY: TRUE or EQUALITY: FALSE, or $? for 0 or 1, respectively, for resultant equality
#

import json
import sys
import argparse
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from forklift import Forklift
from forklift.warehouse import Warehouse
from forklift.exceptions import NoWarehouseAdaptersCouldConnectException

# Custom exception class representing that one and/or another CLI options
# were not provided. You must call is_equal with the syntax:
# is_equal one_forklift_read_options another_forklift_read_options [options]
class ForkliftCLIArumentsWereNotProvidedException(Exception):
  pass

# Custom exception class representing the user did not request at least one
# method of equality to test
class CannotRunIsEqualWithoutAtLeastOneEqualityToCheckException(Exception):
  pass

# Custom exception class representing invalid Forklift JSON options passed in as one
# To determine valid syntax for Forklift read options, call adapter.read_options()
class ForkliftOneOptionsAreNotValidJSONException(Exception):
  pass

# Custom exception class representing invalid JSON options passed in as another
# To determine valid syntax for Forklift write options, call adapter.read_options()
class ForkliftAnotherOptionsAreNotValidJSONException(Exception):
  pass


# Step 0: Ensure the input and output table have been specified as arguments
parser = argparse.ArgumentParser()
parser.add_argument("one", type=str, help="One DataFrame to compare") # required
parser.add_argument("another", type=str, help="Another DataFrame compare") # required
parser.add_argument("--rowcount", action='store_true', help="Equality is dependent on the number of rows matching one and another")
parser.add_argument("--schema", action='store_true', help="Equality is dependent on the schema matching one and another")
parser.add_argument("--query", help="Equality is dependent on the SQL query string returning identical results in one and another; use the tablename one_or_another")
args = parser.parse_args()
if (args.one is None) or (args.another is None):
  raise ForkliftCLIArumentsWereNotProvidedException
if (args.rowcount is False) and (args.schema is False) and (args.query is None or args.query == ''):
  raise CannotRunIsEqualWithoutAtLeastOneEqualityToCheckException

# Step 1: Parse the Forklift read & write options from JSON into Python dictionaries
one_from = None
try:
  one_from = json.loads(args.one)
except ValueError as value_error:
  raise ForkliftOneOptionsAreNotValidJSONException(value_error)
another_from = None
try:
  another_from = json.loads(args.another)
except ValueError as value_error:
  raise ForkliftAnotherOptionsAreNotValidJSONException(value_error)

# Step 2: Set up our Spark and SQL contexts
conf = SparkConf().setAppName("Check equality between two DataFrames")
sparkContext = SparkContext(conf=conf)
sql_context = SQLContext(sparkContext)

# Step 3: Read in the DataFrames
try:
  one = Warehouse().read(sql_context, one_from)
except NoWarehouseAdaptersCouldConnectException:
  raise ForkliftOneOptionsAreNotValidJSONException
try:
  another = Warehouse().read(sql_context, another_from)
except NoWarehouseAdaptersCouldConnectException:
  raise ForkliftAnotherOptionsAreNotValidJSONException

# Step 4: Establish a utility method which exits this program if equality
# isn't found
def exit_with_falsehood():
  print("EQUALITY: FALSE")
  sys.exit(1)

# Step 5: If row count equality was requested, compare the row counts
if args.rowcount is True:
  if one.count() != another.count():
    exit_with_falsehood()

# Step 6: If schema equality was requested, compare the schemas
if args.schema is True:
  if one.schema() != another.schema():
    exit_with_falsehood()

# Step 7: Establish a utility method to run SQL on a DataFrame, and
# get the resultant DataFrame
def run_query_on_dataframe_and_get_resulting_dataframe(df):
  # Register it as a temporary table
  df.registerTempTable("one_or_another")
  # Run the query on the temporary table
  resultant_df = sql_context.sql(args.query)
  # Drop the temporary table
  sql_context.dropTempTable("one_or_another")
  # Return back the selected DataFrame
  return resultant_df

# Step 8: If query equality was requested, compare the query results
if args.query is not None and args.query != '':
  # Get one and another DataFrames, scoped to the query
  one_queried = run_query_on_dataframe_and_get_resulting_dataframe(one)
  another_queried = run_query_on_dataframe_and_get_resulting_dataframe(another)
  # Compare the two DataFrames
  if one_queried.subtract(another_queried).count() != 0:
    exit_with_falsehood()

# Step 9: If we've made it this far, one and another are equal
print("EQUALITY: TRUE")
sys.exit(0)
