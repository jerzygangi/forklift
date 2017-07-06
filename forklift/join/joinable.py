# This function returns a boolean response if two DataFrames can be joined
#
# "Joinable" is defined as:
#   a) the column name you want to join on the left exists, and
#   b) the column name you want to join on the right exists
def are_dataframes_joinable(left_dataframe, left_column_name, right_dataframe, right_column_name):
  if (left_column_name in left_dataframe.columns) and (right_column_name in right_dataframe.columns):
    return True
  else:
    return False
