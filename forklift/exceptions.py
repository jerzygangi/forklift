# Custom exception class for when an adapter attempts to load in
# a DataFrame, but cannot
class CantReadUsingThisAdapterException(Exception):
  pass

# Custom exception class for when an adapter attempts to write out
# a DataFrame, but cannot
class CantWriteUsingThisAdapterException(Exception):
  pass
