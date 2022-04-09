import db_connection as dc

cur = dc.get_db_connection()

sql = '''
SELECT DISTINCT concept_name
FROM condition_occurrence co
JOIN concept c ON c.concept_id = co.condition_concept_id
WHERE LOWER(concept_name) SIMILAR TO '(a|b|c|d|e)%heart%';
'''

cur.execute(sql)
for row in cur.fetchall():
    print(row)