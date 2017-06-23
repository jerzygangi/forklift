# Custom exception class for when an adapter attempts to load in
# a DataFrame, but cannot
class CantReadUsingThisAdapterException(Exception):
  pass

# Custom exception class for when an adapter attempts to write out
# a DataFrame, but cannot
class CantWriteUsingThisAdapterException(Exception):
  pass

# Custom exception class for when all Forklift adapters have
# attempted to achieve a connection to the warehouse using the
# provided options, but none have worked
class NoWarehouseAdaptersCouldConnectException(Exception):
  pass
