# Python
import json
# Forklift
from ..join import join_with_mappings
from ..join.joinable import are_dataframes_joinable

class Decorator(object):
  def __init__(self, mapping_file_path):
    with open(mapping_file_path) as mapping_file:
        self.mappings = json.load(mapping_file)["mappings"]

  # This function takes in a DataFrame to decorate, and multiple DataFrames to decorate it with, plus mapping rules about how to do it
  # It recursively joins decorate_dataframe with 1 with_dataframes at a time, until no with_dataframes are left
  def decorate(self, decorate_dataframe, with_dataframes, mappings=None):
    # As per https://stackoverflow.com/a/1802980/98168
    if mappings is None:
      mappings = self.mappings
    try:
      # Step 1: Get the next DataFrame to join with
      with_dataframe = with_dataframes.pop(0)
      # Step 2: Make a deep copy of the mappings, since we edit the mappings hash destructively
      mappings_dereferenced = copy.deepcopy(mappings)
      # Step 3: Join the DataFrame to decorate, with the DataFrame to decrorate it with, using the copy of mappings
      decorated_dataframe = join_with_mappings(decorate_dataframe, with_dataframe, mappings_dereferenced)
      # Step 4: Call this function again, with all mappings, but with only the remaining with DataFrames
      return self.decorate(decorated_dataframe, with_dataframes)
    except IndexError:
      # There are no more with DataFrames left to join, so we're done
      return decorate_dataframe
