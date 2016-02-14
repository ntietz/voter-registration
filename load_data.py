# Read in the voter records.
import csv, sqlite3

table_def = {
    'name': 'voter_records',
    'columns': [
        (1, 'county', 'INTEGER'),
        (3, 'last_name', 'TEXT'),
        (4, 'first_name', 'TEXT'),
        (7, 'date_of_birth', 'TEXT'),
        (8, 'registration_date', 'TEXT')
      ]
    }

def create_table(conn, table_def):
  columns = map(lambda c: '%s %s' % (c[1], c[2]), table_def['columns'])
  name = table_def['name']
  query = '''CREATE TABLE %s (%s)''' % (name, ', '.join(columns))
  conn.execute(query)
  conn.commit()

def load_table(conn, table_def, path):
  indices = []
  converters = []
  for (idx, _, t) in table_def['columns']:
    converter = lambda x: x
    if t == 'INTEGER':
      converter = lambda x: int(x)
    elif t == 'TEXT':
      converter = lambda x: str(x)

    indices.append(idx)
    converters.append(converter)
  ic_pairs = zip(indices, converters)
  max_idx = max(indices)

  def insert_one(row):
    if len(row)-1 < max_idx:
      return
    vals = []
    for (idx, c) in ic_pairs:
      vals.append(c(row[idx]))
    query = '''INSERT INTO %s VALUES (%s)''' % (table_def['name'], ','.join(['?']*len(vals)))
    #print query, vals
    conn.execute(query, vals)

  with open(path, 'rb') as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    header = reader.next()
    for row in reader:
      insert_one(row)
    conn.commit()

with sqlite3.connect('data/voter_records.sl3') as conn:
  create_table(conn, table_def)
  load_table(conn, table_def, 'data/voter_records.csv')

