import mysql.connector
from datetime import datetime

def get_current_vocabularies(cnx):
    vocabularies = {}
    cursor = cnx.cursor()
    query = ("SELECT id, ref FROM vocabularies")
    cursor.execute(query)
    for (id, ref) in cursor:
        if ref not in vocabularies:
            vocabularies[ref] = id
    cursor.close()
    return vocabularies

def add_vocabulary(vocabulary, cnx):
    sql = ("""INSERT INTO vocabularies(ref, name, url, description, status, version)
              VALUES(%s, %s, %s, %s, %s, %s)""")
    values = (
        vocabulary['ref'].strip(),
        vocabulary['name'].strip(),
        vocabulary['url'].strip(),
        vocabulary['description'].strip(),
        vocabulary['status'].strip(),
        vocabulary['version'].strip(),
    )
    cursor = cnx.cursor()
    cursor.execute(sql, values)
    cnx.commit()
    return cursor.lastrowid

def update_task_status(status, uuid, cnx):
    now = datetime.now()
    sql = ("""UPDATE tasks SET status = %s, last_update_date = %s WHERE uuid = %s""")
    values = (
        status,
        now.strftime("%Y-%m-%d %H:%M:%S"),
        uuid,
    )
    cursor = cnx.cursor()
    cursor.execute(sql, values)
    cnx.commit()


def get_current_concepts(cnx):
    """Trae todos los CONCEPT_ID de la base de datos"""
    concepts = {}
    cursor = cnx.cursor()
    query = ("SELECT idCODE, CONCEPT_ID FROM concepts")
    cursor.execute(query)
    for (idCODE, CONCEPT_ID) in cursor:
        if CONCEPT_ID not in concepts:
            concepts[CONCEPT_ID] = idCODE
    cursor.close()
    return concepts



def add_concepts(concept, cnx):
    sql = ("""INSERT INTO concepts(PXORDX, OLDPXORDX, CODETYPE, CONCEPT_CLASS_ID, CONCEPT_ID, VOCABULARY_ID, 
                                        DOMAIN_ID, TRACK, STANDARD_CONCEPT, CODE, CODEWITHPERIODS, CODESCHEME, 
                                        LONG_DESC, SHORT_DESC, CODE_STATUS, CODE_CHANGE, CODE_CHANGE_YEAR, CODE_PLANNED_TYPE, 
                                        CODE_BILLING_STATUS, CODE_CMS_CLAIM_STATUS, SEX_CD, ANAT_OR_COND, POA_CODE_STATUS, 
                                        POA_CODE_CHANGE, POA_CODE_CHANGE_YEAR, VALID_START_DATE, VALID_END_DATE, INVALID_REASON, CREATE_DT)
              VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
    values = (
        concept['pxordx'].strip(),
        concept['oldpxordx'],
        concept['codetype'].strip(),
        concept['concept_class_id'],
        concept['concept_id'].strip(),
        concept['vocabulary_id'].strip(),
        concept['domain_id'].strip(),
        concept['track'],
        concept['standard_concept'],
        concept['code'],
        concept['codewithperiods'],
        concept['codescheme'],
        concept['long_desc'],
        concept['short_desc'],
        concept['code_status'],
        concept['code_change'],
        concept['code_change_year'],
        concept['code_planned_type'],
        concept['code_billing_status'],
        concept['code_cms_claim_status'],
        concept['sex_cd'],
        concept['anat_or_cond'],
        concept['poa_code_status'],
        concept['poa_code_change'],
        concept['poa_code_change_year'],
        concept['valid_start_date'],
        concept['valid_end_date'],
        concept['invalid_reason'],
        concept['create_dt'],
    )
    cursor = cnx.cursor()
    cursor.execute(sql, values)
    cnx.commit()
    return cursor.lastrowid