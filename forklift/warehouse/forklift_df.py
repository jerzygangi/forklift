# Forklift's own special subclass of DataFrame, for when it's smarter
# to decorate DataFrame than provide FP around it

class ForkliftDataFrame(DataFrame):
  def __init__(self, df):
    super(self.__class__, self).__init__(df._jdf, df.sql_ctx)
  def safely_coalesce(self, partitions):
    if partitions != None and isinstance(partitions, int):
      return self.coalesce(partitions)
    else:
      return self