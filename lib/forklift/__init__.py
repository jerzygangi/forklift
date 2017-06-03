from os.path import isfile
from json import load
from pyspark.sql.dataframe import DataFrame

class Forklift(object):

  @classmethod
  def normalize_and_sanitize(klass, dataframe, with_spark_schema, remappings_file_path, cast_processor):
    print("Step 1: Check that arguments are valid")
    if not isinstance(dataframe, Dataframe):
      raise TypeError("dataframe must be an instance of Dataframe")
    if size(dataframe.columns) < 1:
      raise ValueError("dataframe must have 1 or more columns")
    if dataframe.count() < 1:
      raise ValueError("dataframe must have 1 or more rows")
    if not isinstance(with_spark_schema, StructType):
      raise TypeError("with_spark_schema must be an instance of StructType")
    if not size(with_spark_schema) < 1:
      raise ValueError("with_spark_schema must have at least one StructField column")
    if not isfile(remappings_file_path):
      raise ValueError("remappings_file_path must be a file that exists")
    if not isinstance(cast_processor, Forklift.CastProcessor):
      raise TypeError("cast_processor must be an instance of Forklift.CastProcessor")

    print("Step 2: Rename all columns, according to the mapping")
    column_renamer = Forklift.ColumnRenamer(remappings_file_path)
    dataframe_with_columns_renamed = column_renamer.rename_columns(dataframe)

    print("Step 3: Delete unwanted columns, according to the mapping")
    column_deleter = Forklift.ColumnDeleter(remappings_file_path)
    dataframe_with_columns_renamed_and_unwanted_columns_deleted = column_deleter.delete_columns(dataframe_with_columns_renamed)

    print("Step 4: Cast each cell, according to the Caster instance provided")
    caster = Forklift.Caster(cast_processor, with_spark_schema)
    dataframe_with_columns_renamed_and_unwanted_columns_deleted_and_types_cast_and_schema_applied = caster.cast(dataframe_with_columns_renamed_and_unwanted_columns_deleted)

    print("Step 5: Return the normalized and sanitized dataframe")
    return dataframe_with_columns_renamed_and_unwanted_columns_deleted_and_types_cast_and_schema_applied
