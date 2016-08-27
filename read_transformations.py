from lxml import etree
from neo4j.v1 import GraphDatabase, basic_auth

from secrets import NEO_PASSWORD

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", NEO_PASSWORD))
session = driver.session()

with open("data/20160630_TRANSFORMATIONS/20160630_TRANSFORMATIONS.XML", 'rb') as f:
	tree_transform = etree.parse(f)

root = tree_transform.getroot()

transforms = root.findall('TRANSFORMATION')

TRANSFORM_CODES = {
	'1': 'Charter Discontinued',
	'5': 'Split',
	'7': 'Sale of Assets',
	'9': 'Charter Retained',
	'50': 'Failure'
}

ACCT_METHODS = {
	'0': 'Not Applicable',
	'1': 'Pooling of interests',
	'2': 'Purchase/acquisition'
}

for t in transforms:

	params = {
		'pred': t.find('ID_RSSD_PREDECESSOR').text,
		'succ': t.find('ID_RSSD_SUCCESSOR').text,
		'dt_trans': t.find('DT_TRANS').text,
		'transform_type': TRANSFORM_CODES[t.find('TRNSFM_CD').text],
		'accounting_method': ACCT_METHODS[t.find('ACCT_METHOD').text]
	}

	session.run("""
		MATCH (p:ENTITY), (s:ENTITY)
		WHERE p.entity = {pred} and s.entity = {succ}
		CREATE (p)-[t:TRANSFORM {dt_trans: {dt_trans},
								 transform_type: {transform_type},
								 accounting_method: {accounting_method}}]->(s)
		RETURN t
		""", params)