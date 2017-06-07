from json import load

# Custom exception class for trying to rename a column that doesn't exist
class CannotRenameAColumnThatDoesntExistException(Exception):
    pass

class ColumnRenamer(object):
  def __init__(self, dataframe, remappings_file_path):
    with open(remappings_file_path) as remappings_file:
        self.column_remappings = json.load(remappings_file)["remappings"]
    return self.rename_columns(dataframe)
  
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
