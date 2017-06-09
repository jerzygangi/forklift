from .rdd_cell_caster import cast_values_in_row

# Parent class from which to inherit your cast processors
class CastProcessor(object):
  pass

class CellCaster(object):
  def __init__(self, cast_processor_klass, spark_schema, sql_context):
    self.cast_processor_klass = cast_processor_klass
    self.schema = spark_schema
    self.sql_context = sql_context

  def cast(self, dataframe):
    cast_rdd = dataframe.rdd.map(lambda row: cast_values_in_row(row, self.cast_processor_klass))
    return self.sql_context.createDataFrame(cast_rdd, self.schema)
