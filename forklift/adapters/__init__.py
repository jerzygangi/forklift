# An "Adapter" is a way of reading data into spark.
#
# Currently, Forklift ships with four adapters: DSV for .csv
# and .tsv files, Parquet for .parquet files, PostgreSQL,
# and Redshift.
#
# Do you want a new adapter? Just subclass the Adapter class
# (below), and commit it! :)

# Abstract parent class that each concrete adapter should subclass
class Adapter(object):
  def __init__(self):
    # Each subclass should override these
    self.required_read_options = []
    self.required_write_options = []

  # Each subclass should override this
  def read(self, sql_context, **kwargs):
    pass

  # Each subclass should override this
  def write(self, dataframe, **kwargs):
    pass
