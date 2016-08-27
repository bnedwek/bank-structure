from lxml import etree
from neo4j.v1 import GraphDatabase, basic_auth

from secrets import NEO_PASSWORD

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", NEO_PASSWORD))
session = driver.session()

with open("data/20160630_ATTRIBUTES_INACTIVE/20160630_ATTRIBUTES_INACTIVE.XML", 'rb') as f:
	tree_inactive = etree.parse(f)

root = tree_inactive.getroot()

entities = root.findall('ATTRIBUTES')

for e in entities:

	params = {
		'id_rssd': e.find('ID_RSSD').text,
		'id_rssd_hd_off': e.find('ID_RSSD_HD_OFF').text,
		'act_prim_cd': e.find('ACT_PRIM_CD').text,
		'bhc_ind': e.find('BHC_IND').text,
		'chtr_type_cd': e.find('CHTR_TYPE_CD').text,
		'cntry_cd': e.find('CNTRY_CD').text,
		'dist_frs': e.find('DIST_FRS').text,
		'dt_open': e.find('DT_OPEN').text,
		'dt_start': e.find('DT_START').text,
		'dt_end': e.find('DT_END').text,
		'entity_type': e.find('ENTITY_TYPE').text,
		'insur_pri_cd': e.find('INSUR_PRI_CD').text,
		'mjr_own_mnrty': e.find('MJR_OWN_MNRTY').text,
		'nm_lgl': e.find('NM_LGL').text,
		'nm_short': e.find('NM_SHORT').text,
		'org_type_cd': e.find('ORG_TYPE_CD').text,
		'state_cd': e.find('STATE_CD').text,
		'state_home_cd': e.find('STATE_HOME_CD').text,
		'state_inc_cd': e.find('STATE_INC_CD').text,
		'zip_cd': e.find('ZIP_CD').text
	}

	session.run("""
		CREATE (a:ENTITY {entity: {id_rssd},
			head_office: {id_rssd_hd_off},
			primary_activity: {act_prim_cd},
			is_bhc: {bhc_ind},
			charter_type: {chtr_type_cd},
			country: {cntry_cd},
			fed_dist: {dist_frs},
			dt_open: {dt_open},
			dt_start: {dt_start},
			dt_end: {dt_end},
			entity_type: {entity_type},
			primary_insurer: {insur_pri_cd},
			minorty_owned: {mjr_own_mnrty},
			name_long: {nm_lgl},
			name_short: {nm_short},
			organization_type: {org_type_cd},
			state: {state_cd},
			home_state: {state_home_cd},
			inc_state: {state_inc_cd},
			zip_code: {zip_cd}})
		""", params)