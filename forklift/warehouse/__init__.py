from ..adapters.dsv import DSVAdapter
from ..adapters.parquet import ParquetAdapter
from ..adapters.postgresql import PostgreSQLAdapter
from ..adapters.redshift import RedshiftAdapter
from ..exceptions import CantReadUsingThisAdapterException, CantWriteUsingThisAdapterException, NoWarehouseAdaptersCouldConnectException
from ..utilities.options_checker import RequiredOptionsArentAllPresentException

class Warehouse(Object):
  def __init__(self):
    # These are the curently supported warehouse adapters
    self.warehouse_adapters = [DSVAdapter, ParquetAdapter, PostgreSQLAdapter, RedshiftAdapter]

  # To read a "table" from the warehouse, loop over each warehouse
  # adapter and try to load in the requested options. The first adapter
  # that doesn't throw an exception wins.
  def read(self, sql_context, options):
    # Step 1: Try and read a "table" with options, using each warehouse
    # adapter, until one works -- then return the resulting DataFrame
    for warehouse_adapter_klass in self.warehouse_adapters:
      try:
        warehouse_adapter_instance = warehouse_adapter_klass()
        return warehouse_adapter_instance.read(sql_context, options=options)
      except (CantReadUsingThisAdapterException, RequiredOptionsArentAllPresentException):
        print("WARNING: Could not read using {0}".format(warehouse_adapter_klass))
        pass # (do next loop)
    # Step 2: If we haven't returned by this point, it means that none
    # of the adapters worked, so we let the caller know that we couldn't
    # read this "table" from the warehouse
    raise NoWarehouseAdaptersCouldConnectException

  # To write a "table" from the warehouse, loop over each warehouse
  # adapter and try to load in the requested options. The first adapter
  # that doesn't throw an exception wins.
  def write(self, dataframe, options):
    # Step 1: Try and write the dataframe with options, using each warehouse
    # adapter, until one works -- then return it
    for warehouse_adapter_klass in self.warehouse_adapters:
      try:
        warehouse_adapter_instance = warehouse_adapter_klass()
        warehouse_adapter_instance.write(dataframe, options=options)
        return
      except (CantWriteUsingThisAdapterException, RequiredOptionsArentAllPresentException):
        print("WARNING: Could not write using {0}".format(warehouse_adapter_klass))
        pass # (do next loop)
    # Step 2: If we haven't returned by this point, it means that none
    # of the adapters worked, so we let the caller know that we couldn't
    # write this dataframe to the warehouse
    raise NoWarehouseAdaptersCouldConnectException
