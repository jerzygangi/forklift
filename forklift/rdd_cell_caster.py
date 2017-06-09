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
