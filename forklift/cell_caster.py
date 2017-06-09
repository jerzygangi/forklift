from .rdd_cell_caster import RDDCellCaster

# Parent class from which to inherit your cast processors
class CastProcessor(object):
  pass

class CellCaster(object):
  def __init__(self, cast_processor_klass, spark_schema, sql_context):
    self.cast_processor_klass = cast_processor_klass
    self.schema = spark_schema
    self.sql_context = sql_context

  def cast(self, dataframe):
    rdd_caster = RDDCellCaster(dataframe.rdd, self.cast_processor_klass)
    cast_rdd = rdd_caster.cast()
    return self.sql_context.createDataFrame(cast_rdd, self.schema)
