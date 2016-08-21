
import pandas as pd
from neo4j.v1 import GraphDatabase, basic_auth

from secrets import NEO_PASSWORD

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", NEO_PASSWORD))
session = driver.session()

districts = [
	{'code': 0,  'district': 'Not Applicable'},
	{'code': 1,  'district': 'Boston'},
	{'code': 2,  'district': 'New York'},
	{'code': 3,  'district': 'Philadelphia'},
	{'code': 4,  'district': 'Cleveland'},
	{'code': 5,  'district': 'Richmond'},
	{'code': 6,  'district': 'Atlanta'},
	{'code': 7,  'district': 'Chicago'},
	{'code': 8,  'district': 'St. Louis'},
	{'code': 9,  'district': 'Minneapolis'},
	{'code': 10, 'district': 'Kansas City'},
	{'code': 11, 'district': 'Dallas'},
	{'code': 12, 'district': 'San Francisco'},
]

for d in districts:
	session.run("CREATE (a:FRB_DISTRICT {code: {code}, name: {name}})", {'code': d['code'], 'name': d['district']})