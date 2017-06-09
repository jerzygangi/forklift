# Parent class from which to inherit your cast processors
class CastProcessor(object):
  pass

def cast_cell(column_name, old_cell_value, cast_processor_klass):
  cast_processor = cast_processor_klass()
  if hasattr(cast_processor, 'cast_all_cells'):
    old_cell_value = cast_processor.cast_all_cells(old_cell_value)
  if hasattr(cast_processor, 'cast_{0}'.format(column_name)):
    params=(old_cell_value,)
    return getattr(cast_processor, 'cast_{0}'.format(column_name))(*params)
  else:
    return old_cell_value

def cast_values_in_row(row, cast_processor_klass):
  uncast_row = row.asDict()
  casted_row = {column_name: cast_cell(column_name, cell_value, cast_processor_klass) for column_name, cell_value in uncast_row.items()}
  return casted_row

class CellCaster(object):
  def __init__(self, cast_processor_klass, spark_schema, sql_context):
    self.cast_processor_klass = cast_processor_klass
    self.schema = spark_schema
    self.sql_context = sql_context

  def cast(self, dataframe):
    cast_rdd = dataframe.rdd.map(lambda row: cast_values_in_row(row, self.cast_processor_klass))
    return self.sql_context.createDataFrame(cast_rdd, self.schema)
