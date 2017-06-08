# Python
import json

class ColumnDeleter(object):
  def __init__(self, remappings_file_path):
    with open(remappings_file_path) as remappings_file:
        self.column_remappings = json.load(remappings_file)["remappings"]

  def delete_columns(self, dataframe, unwanted_column_names=None):
    if unwanted_column_names is None:
      unwanted_column_names = [column for column in dataframe.columns if column not in self.column_remappings.values()]
    try:
      column_name_to_drop = unwanted_column_names.pop()
      return self.delete_columns(dataframe.drop(column_name_to_drop), unwanted_column_names)
    except IndexError:
      return dataframe
