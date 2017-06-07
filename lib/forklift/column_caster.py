# Parent class from which to inherit your cast processors
class CastProcessor(object):
  pass

class ColumnCaster(object):
  def __init__(self, cast_processor_klass, spark_schema):
    self.cast_processor = cast_processor_klass()
    self.schema = spark_schema

  def cast_cell(self, column_name, old_cell_value):
    if hasattr(self.cast_processor, 'cast_all_cells'):
      old_cell_value = self.cast_processor.cast_all_cells(old_cell_value)
    if hasattr(self.cast_processor, 'cast_{0}'.format(column_name)):
      params=(old_cell_value,)
      return getattr(self.cast_processor, 'cast_{0}'.format(column_name))(*params)
    else:
      return old_cell_value

  def cast_values_in_row(self, row):
    uncast_row = row.asDict()
    casted_row = {column_name: self.cast_cell(column_name, cell_value) for column_name, cell_value in uncast_row.items()}
    return casted_row

  def cast(self, dataframe):
    cast_rdd = dataframe.rdd.map(lambda row: self.cast_values_in_row(row))
    return sqlContext.createDataFrame(cast_rdd, self.schema)
