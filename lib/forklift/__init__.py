# Python
from os.path import isfile
from json import load
# Spark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
# Forklift
from .caster import CastProcessor
from .stages import *

class Forklift(object):
  @classmethod
  def normalize_and_sanitize(klass, dataframe, with_spark_schema, remappings_file_path, cast_processor, stages=NS_ALL):
    print("Step 1: Check that arguments are valid")
    if not isinstance(dataframe, DataFrame):
      raise TypeError("dataframe must be an instance of Dataframe")
    if len(dataframe.columns) < 1:
      raise ValueError("dataframe must have 1 or more columns")
    if dataframe.count() < 1:
      raise ValueError("dataframe must have 1 or more rows")
    if not isinstance(with_spark_schema, StructType):
      raise TypeError("with_spark_schema must be an instance of StructType")
    if len(with_spark_schema) < 1:
      raise ValueError("with_spark_schema must have at least one StructField column")
    if not isfile(remappings_file_path):
      raise ValueError("remappings_file_path must be a file that exists")
    if not isinstance(cast_processor, CastProcessor):
      raise TypeError("cast_processor must be an instance of CastProcessor")
    if not isinstance(stages, ForkliftStage):
      raise TypeError("stages must be an instance of ForkliftStage")

    if any([stage in stages for stage in [NS_ALL, NS_RENAME_COLS]]):
      print("Step 2: Rename all columns, according to the mapping")
      column_renamer = Forklift.ColumnRenamer(remappings_file_path)
      dataframe_with_columns_renamed = column_renamer.rename_columns(dataframe)
    else:
      print("Skipping Step 2: Rename all columns, according to the mapping")

    if any([stage in stages for stage in [NS_ALL, NS_DELETE_COLS]]):
      print("Step 3: Delete unwanted columns, according to the mapping")
      column_deleter = Forklift.ColumnDeleter(remappings_file_path)
      dataframe_with_columns_renamed_and_unwanted_columns_deleted = column_deleter.delete_columns(dataframe_with_columns_renamed)
    else:
      print("Skipping Step 3: Delete unwanted columns, according to the mapping")

    if any([stage in stages for stage in [NS_ALL, NS_CAST_CELLS]]):
      print("Step 4: Cast each cell, according to the Caster instance provided")
      caster = Forklift.Caster(cast_processor, with_spark_schema)
      dataframe_with_columns_renamed_and_unwanted_columns_deleted_and_types_cast_and_schema_applied = caster.cast(dataframe_with_columns_renamed_and_unwanted_columns_deleted)
    else:
      print("Skipping Step 4: Cast each cell, according to the Caster instance provided")

    print("Step 5: Return the normalized and sanitized dataframe")
    return dataframe_with_columns_renamed_and_unwanted_columns_deleted_and_types_cast_and_schema_applied