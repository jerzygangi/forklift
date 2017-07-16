# Python
import os
import json

# Custom exception class for trying to rename a column that doesn't exist
class CannotRenameAColumnThatDoesntExistException(Exception):
  pass

# Custom exception class for not being able to read the provided remappings
class CannotParseRemappingsException(Exception):
  pass


class ColumnRenamer(object):
  def __init__(self, remappings):
    # remappings can be a file path of a JSON file to load, or a dictionary of remappings
    if isinstance(remappings, str):
      try:
        with open(remappings) as remappings_file:
          self.column_remappings = json.load(remappings_file)["remappings"]
      except:
        raise CannotParseRemappingsException
    elif isinstance(remappings, dict):
      self.column_remappings = remappings
    else:
      raise CannotParseRemappingsException

  def rename_columns(self, dataframe, dictionary_of_column_remapping=None):
    # As per https://stackoverflow.com/a/1802980/98168
    if dictionary_of_column_remapping is None:
      dictionary_of_column_remapping = self.column_remappings
    try:
      old_column_name, new_column_name = dictionary_of_column_remapping.popitem()
      # If the remapping includes an old column name that doesn't exist, that's probably a mistake
      # in the remappings file, so let's throw an exception
      if old_column_name not in dataframe.columns:
        if os.environ.get("DEBUG") == "true": print("The column {0} does not exist, and cannot be remapped".format(old_column_name))
        raise CannotRenameAColumnThatDoesntExistException
      else:
        return self.rename_columns(dataframe.withColumnRenamed(old_column_name, new_column_name), dictionary_of_column_remapping)
    except KeyError:
      return dataframe
