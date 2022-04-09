import db_connection as dc

cur = dc.get_db_connection()

# 1. condition_occurrence에서 제 2형 당뇨병을 진단받은 환자를 추출
# 2. age 함수를 통해서 환자 나이를 계산해서 조건에 맞는 테이블 추출
# 3. 진단 날짜 이후로 복용날을 계산해서 테이블 추출
# 1,2,3 세 개의 테이블을 조인해서 총 환자 수를 계산

sql = '''
SELECT COUNT(1)
FROM (SELECT person_id, visit_occurrence_id
    FROM condition_occurrence
    WHERE condition_concept_id IN (3191208,36684827,3194332,3193274,43531010,4130162,45766052,
        45757474,4099651,4129519,4063043,4230254,4193704,4304377,201826,3194082,3192767)) co
JOIN (SELECT visit_occurrence_id
    FROM drug_exposure
    WHERE drug_concept_id = 40163924 
        AND DATE_PART('day', drug_exposure_end_datetime - drug_exposure_start_datetime) + 1 >= 90) de
ON de.visit_occurrence_id = co.visit_occurrence_id
JOIN (SELECT person_id
    FROM person
    WHERE EXTRACT(year FROM age(birth_datetime::date)) + 1 >= 18) p ON p.person_id = co.person_id;
'''

cur.execute(sql)
for row in cur.fetchall():
    print(row)