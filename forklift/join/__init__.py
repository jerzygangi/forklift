# Join DataFrames together, using mappings that specify how
#
# N.B. join_with_mappings and join_with_mapping are two different functions

# Forklift
from .joinable import are_dataframes_joinable
from .column_renamer import ColumnRenamer

# This function takes in two DataFrames, and joins them using many mapping rules
# It does this by recursing over each mapping, joining decorate_dataframe to with_dataframe for each mapping
def join_with_mappings(decorate_dataframe, with_dataframe, mappings):
  try:
    # Step 1: Get a mapping
    mapping = mappings.pop(0)
    # Step 2: Join the decorate_dataframe and with_dataframe DataFrames using the mapping
    decorated_dataframe = join_with_mapping(decorate_dataframe, with_dataframe, mapping)
    # Step 3: Call this function again, with the newly decorated_dataframe and the remaining mappings
    return join_with_mappings(decorated_dataframe, with_dataframe, mappings)
  except IndexError:
    # There are no more mappings left to join with, so we're done
    return decorate_dataframe

# This function narrows down mappings to the ones that actually exist in the dataframe
def mappings_that_exist_on_dataframe(dataframe, mappings, mapping_key_for_dataframe):
  requested_columns_names = [column_mapping[mapping_key_for_dataframe] for column_mapping in mappings]
  requested_columns_that_actually_exist = list(set(requested_columns_names).intersection(set(dataframe.columns)))
  return [column_mapping for column_mapping in mappings if column_mapping[mapping_key_for_dataframe] in requested_columns_that_actually_exist]

# This function narrows down mappings to the ones that don't exist in the dataframe
def mappings_that_dont_exist_on_dataframe(dataframe, mappings, mapping_key_for_dataframe):
  requested_columns_names = [column_mapping[mapping_key_for_dataframe] for column_mapping in mappings]
  requested_columns_that_dont_exist = [column_name for column_name in requested_columns_names if column_name not in dataframe.columns]
  return [column_mapping for column_mapping in mappings if column_mapping[mapping_key_for_dataframe] in requested_columns_that_dont_exist]

# This function collects the values for one key in the mapping
def get_list_from_mappings(mappings, mapping_key):
  return [mapping[mapping_key] for mapping in mappings]

# This function takes in two DataFrames, and joins them using a mapping rule
def join_with_mapping(decorate_dataframe, with_dataframe, mapping):
  # Step 1: Ensure we can even join these two DataFrames
  # Check that the decorate_dataframe and the with_dataframe both have the columns that the
  # mapping wishes to join on (otherwise we can't join, so return immediately)
  if not are_dataframes_joinable(decorate_dataframe, mapping["this_join_column_name"], with_dataframe, mapping["with_join_column_name"]):
    return decorate_dataframe
  # Step 2: Get the valid mappings
  # Make a deep copy of the mappings, since we edit the hash destructively
  mappings_dereferenced = copy.deepcopy(mapping["add_from_with_to_this"])
  # Remove mappings that don't exist on with_dataframe
  compatible_mappings = mappings_that_exist_on_dataframe(with_dataframe, mappings_dereferenced, "current_with_column_name")
  # Remove mappings that would collide decorate_dataframe and with_dataframe
  compatible_mappings = mappings_that_dont_exist_on_dataframe(decorate_dataframe, compatible_mappings, "becomes_this_column_name")
  # Make sure there's at least 1 mapping left, otherwise joining would be pointless
  if len(decorate_and_with_compatible_mappings) < 1:
    return decorate_dataframe
  # Step 3: Narrow down with_dataframe to what is needed
  # Get the columns the user requested from with_dataframe
  with_dataframe = with_dataframe.select(get_list_from_mappings(compatible_mappings, "current_with_column_name") + [mapping["with_join_column_name"]])
  # Rename the columns in with_dataframe_only_columns_needed
  column_renamer = ColumnRenamer({a_mapping["current_with_column_name"]: a_mapping["becomes_this_column_name"] for a_mapping in compatible_mappings})
  with_dataframe = column_renamer.rename_columns(with_dataframe)
  # Give the join column a predefined name, so that it doesn't collide with an existing column in decorate_dataframe
  with_dataframe = with_dataframe.withColumnRenamed(mapping["with_join_column_name"], "__temp_with_join_column")
  # Step 4: Return the joined DataFrame,
  # getting rid of the uniquely-named join column along the way
  return decorate_dataframe.join(with_dataframe, decorate_df[mapping["this_join_column_name"]] == with_df_only_columns_needed_renamed_and_predefined_join_column["__temp_with_join_column"], "inner").drop("__temp_with_join_column")
