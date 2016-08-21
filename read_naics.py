## Read NAICS code file ##
import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth

from secrets import NEO_PASSWORD

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", NEO_PASSWORD))
session = driver.session()

naics_path = 'data/naics07.txt'
naics_file = pd.read_fwf(naics_path, colspecs=[(8,14), (16,200)], names=['code', 'desc'])

for i, row in naics_file.iterrows():
	code = row.code
	desc = row.desc

	if len(code) > 2:
		parent = code[:-1]
	else:
		parent = None

	if desc.startswith('"') and desc.endswith('"'):
		desc = desc[1:-1]

	session.run("CREATE (a:NAICS {code: {code}, description: {desc}})", {"code": code, "desc": desc})

	if parent:
		session.run("""
				MATCH (a:NAICS), (b:NAICS)
				WHERE a.code = {code} AND b.code = {parent}
				CREATE (a)-[r:PARENT]->(b)
				RETURN r
			""", {"code": code, "parent": parent})
