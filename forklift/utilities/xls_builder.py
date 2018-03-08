#
# XLSBuilder
# Make an Excel file from dataframes
#
# N.B. XLSBuilder collects and writes dataframes on the master node of your Spark cluster. If you attempt to use large dataframes with XLSBuilder, undefined behavior may occur. This is not considered a bug, since desktop usage of Excel is itself limited to small, `n MB` .xls files. Therefore, XLSBuilder should be used with small dataframes.
#

import xlsxwriter

class XLSBuilder():
  def __init__(self, path):
    self.workbook = xlsxwriter.Workbook(path)
  def addTab(self, df, tab_name, column_format_definitions=None):
    # Step 1: Make a new tab in Excel
    this_tab = self.workbook.add_worksheet(tab_name)
    # Step 2: Write the column headers
    columns = df.columns
    for column_index, column in enumerate(columns):
      this_tab.write_string(0, column_index, column)
    # Step 3: Turn the dataframe into a local Python array in memory on the master node
    df_as_local_python_array = df.collect()
    # Step 4: Add each row from the dataframe into the Excel spreadsheet
    # We do this by iterating over each row, and then iterating over each column,
    # so as to not rely on the implicit ordering of columns in each Row object
    for row_index, row in enumerate(df_as_local_python_array):
      row_number = row_index + 1 # Accout for header row added above in Step 2
      for column_index, column in enumerate(columns):
        cell_format = column_format_definitions[column_index-1] if len(column_format_definitions) > (column_index-1) else None
        this_tab.write(row_number, column_index, row[column], cell_format)
  def write(self):
    self.workbook.close()
