# Read in the voter records.
import csv
header = None
content = []
with open('data/voter_records.csv', 'rb') as voter_file:
  reader = csv.reader(voter_file, delimiter=',')
  header = reader.next()
  for row in reader:
    content.append(row)

print "Loaded in %s records." % len(content)

# Read in the county numbers file.
import json, ast
with open('data/counties.json', 'rb') as counties_file:
  number_to_county = ast.literal_eval(counties_file.read())
county_to_number = {v:k for k,v in number_to_county.iteritems()}

# Push the data into a sqlite database
import pandas, sqlite3
db_conn = sqlite3.connect('data/voter_records.sl3')
dataframe = pandas.DataFrame(data=content, columns=header)
dataframe.to_sql('voter_records', db_conn)
