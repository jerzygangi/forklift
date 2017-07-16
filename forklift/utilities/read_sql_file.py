class StringIsNotAFileException(Exception):
  pass

def read_sql_file(path):
  try:
    with open(path) as the_file:
        return json.load(the_file)
  except:
    raise StringIsNotAFileException
