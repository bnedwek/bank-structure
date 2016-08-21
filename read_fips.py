import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth

from secrets import NEO_PASSWORD

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", NEO_PASSWORD))
session = driver.session()

fips_path = 'data/fips5-2.txt'
fips_file = pd.read_table(fips_path, sep="|")

for i, row in fips_file.iterrows():
	fips = row.STATE
	name_short = row.STUSAB
	name_long = row.STATE_NAME
	gnis = row.STATENS

	session.run("CREATE (a:STATE {fips: {fips}, name_short: {name_short}, name_long: {name_long}, gnis: {gnis}})", {"fips": fips, "name_short": name_short, "name_long": name_long, "gnis": gnis})
