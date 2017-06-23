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

  # Each subclass should override this
  def read(self, sql_context, **kwargs):
    # Return a DataFrame in the subclass
    pass

  # Each subclass should override this
  def write(self, dataframe, **kwargs):
    # Return nothing in the subclass
    pass
  
  # Each subclass should override this
  @classmethod
  def read_options(klass):
    # Return an array of strings that are required options when
    # calling read()
    return [""]

  # Each subclass should override this
  @classmethod
  def write_options(klass):
    # Return an array of strings that are required options when
    # calling write()
    return [""]