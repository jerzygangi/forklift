class StringIsNotAFileException(Exception):
  pass

def read_sql_file(path):
  try:
    with open(path, "r") as the_file:
      return the_file.read()
  except:
    raise StringIsNotAFileException
