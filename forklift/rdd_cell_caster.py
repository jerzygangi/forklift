class RDDCellCaster(object):
  def __init__(self, rdd, cast_processor_klass):
    self.rdd = rdd
    self.cast_processor_klass = cast_processor_klass

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

  def cast(self):
    return self.rdd.map(lambda row: self.__class__.cast_values_in_row(row, self.cast_processor_klass))
