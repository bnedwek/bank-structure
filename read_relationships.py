from lxml import etree
from neo4j.v1 import GraphDatabase, basic_auth

from secrets import NEO_PASSWORD

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", NEO_PASSWORD))
session = driver.session()

with open("data/20160630_RELATIONSHIPS/20160630_RELATIONSHIPS.XML", 'rb') as f:
	tree_rel = etree.parse(f)

root = tree_rel.getroot()

rels = root.findall('RELATIONSHIP')

RELATIONSHIP_LEVELS = {"1": "Direct", "2": "Indirect", "3": "2G3", "4": "Debt Previously Contracted"}

for r in rels:

	params = {
		'parent': r.find('ID_RSSD_PARENT').text,
		'offspring': r.find('ID_RSSD_OFFSPRING').text,
		'relationship_level': RELATIONSHIP_LEVELS[r.find('RELN_LVL').text],
		'percent_equity': r.find('PCT_EQUITY').text,
		'dt_start': r.find('DT_START').text,
		'dt_end': r.find('DT_END').text,
		'dt_established': r.find('DT_RELN_EST').text
	}

	session.run("""
		MATCH (p:ENTITY), (o:ENTITY)
		WHERE p.entity = {parent} and o.entity = {offspring}
		CREATE (o)-[r:BANK_PARENT {relationship_level: {relationship_level},
								   percent_equity: {percent_equity},
								   dt_start: {dt_start},
								   dt_end: {dt_end},
								   dt_established: {dt_established}}]->(p)
		RETURN r
		""", params)
