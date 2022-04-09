import db_connection as dc

cur = dc.get_db_connection()

max_value_sql = '''
WITH RECURSIVE total_visit_count AS(
    SELECT person_id, SUM(DATE_PART('day', visit_end_datetime - visit_start_datetime) + 1) AS visit_count
    FROM visit_occurrence
    GROUP BY person_id
)

SELECT MAX(visit_count) FROM total_visit_count;
'''
cur.execute(max_value_sql)
for row in cur.fetchall():
    print(row)


patient_count_sql = '''
WITH RECURSIVE total_visit_count AS(
    SELECT person_id, SUM(DATE_PART('day', visit_end_datetime - visit_start_datetime) + 1) AS visit_count
    FROM visit_occurrence
    GROUP BY person_id
)

SELECT COUNT(1)
FROM total_visit_count 
WHERE visit_count = (SELECT MAX(visit_count) FROM total_visit_count);
'''

cur.execute(patient_count_sql)
for row in cur.fetchall():
    print(row)