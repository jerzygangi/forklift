class AccuenDataFrame(DataFrame):
  def __init__(self, df):
    super(self.__class__, self).__init__(df._jdf, df.sql_ctx)
  def safely_coalesce(self, partitions):
    if partitions != None and isinstance(partitions, int):
      return self.coalesce(partitions)
    else:
      return self

class Writer(object):
  # Step 4: Establish the status of the table being written
  if os.environ.get("DEBUG") == "true": print("The number of rows being written is: {0}".format(source_table_with_partner_type_and_partner_id.count()))

  # Step 5: Convert the dataframe into an Accuen dataframe, so we can format it as per
  # the user's request
  source_table_with_partner_type_and_partner_id_accuen_df = AccuenDataFrame(source_table_with_partner_type_and_partner_id)
  source_table_with_partner_type_and_partner_id_accuen_df_formatted = source_table_with_partner_type_and_partner_id_accuen_df.safely_coalesce(number_of_output_partitions)

  # Step 6: Write the dataframe to the destination
  if os.environ.get("DEBUG") == "true": print("Saving to Redshift/S3/HDFS in format Structured/Parquet/DSV...")
