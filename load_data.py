# Read in the voter records.
import csv, psycopg2

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
  with conn.cursor() as cursor:
    columns = map(lambda c: '%s %s' % (c[1], c[2]), table_def['columns'])
    name = table_def['name']
    query = '''DROP TABLE IF EXISTS %s; CREATE TABLE %s (%s)''' % (name, name, ', '.join(columns))
    cursor.execute(query)
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
    query = '''INSERT INTO %s VALUES (%s)''' % (table_def['name'], ','.join(['%s']*len(vals)))
    #print query, vals
    with conn.cursor() as cursor:
      cursor.execute(query, vals)

  with open(path, 'rb') as csv_file:
    reader = csv.reader(csv_file, delimiter=',')
    header = reader.next()
    for row in reader:
      insert_one(row)
    conn.commit()

with psycopg2.connect('dbname=voters user=nicholas') as conn:
  create_table(conn, table_def)
  load_table(conn, table_def, 'data/voter_records.csv')

