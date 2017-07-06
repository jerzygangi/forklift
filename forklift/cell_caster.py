import copy

# Parent class from which to inherit your cast processors
class CastProcessor(object):
  pass

# Example cast processor class
class ExampleCaster(CastProcessor):
  # An example of casting all cells using carte blanche logic
  def cast_all_cells(self, value):
    if isinstance(value, float):
      return Decimal(value)
    else:
      return value
  # An example of casting values of column_name
  def cast_example_column_name(self, example_value):
    if isinstance(example_value, int):
      return example_value + 1000

class CellCaster(object):
  def __init__(self, cast_processor_klass, spark_schema, sql_context):
    self.cast_processor_klass = cast_processor_klass
    self.schema = spark_schema
    self.sql_context = sql_context

  def cast(self, dataframe):
    distributed_safe_cast_processor_klass = copy.deepcopy(self.cast_processor_klass)
    distributed_safe_this_klass = copy.deepcopy(self.__class__)
    cast_rdd = dataframe.rdd.map(lambda row: distributed_safe_this_klass.cast_values_in_row(row, distributed_safe_cast_processor_klass))
    return self.sql_context.createDataFrame(cast_rdd, self.schema)

  @classmethod
  def cast_cell(klass, column_name, old_cell_value, cast_processor_klass):
    cast_processor = cast_processor_klass()
    if hasattr(cast_processor, 'cast_all_cells'):
      old_cell_value = cast_processor.cast_all_cells(old_cell_value)
    if hasattr(cast_processor, 'cast_{0}'.format(column_name)):
      params=(old_cell_value,)
      return getattr(cast_processor, 'cast_{0}'.format(column_name))(*params)
    else:
      return old_cell_value

  @classmethod
  def cast_values_in_row(klass, row, cast_processor_klass):
    uncast_row = row.asDict()
    casted_row = {column_name: klass.cast_cell(column_name, cell_value, cast_processor_klass) for column_name, cell_value in uncast_row.items()}
    return casted_row
