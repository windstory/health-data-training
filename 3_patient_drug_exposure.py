import db_connection as dc

cur = dc.get_db_connection()
sql = '''
SELECT person_id, drug_concept_id, drug_exposure_start_datetime, drug_exposure_end_datetime,
DATE_PART('day', drug_exposure_end_datetime - drug_exposure_start_datetime) + 1 AS dosing_days
FROM drug_exposure
WHERE person_id = 1891866
ORDER BY dosing_days DESC
LIMIT 10;
'''

cur.execute(sql)
for row in cur.fetchall():
    print(row)