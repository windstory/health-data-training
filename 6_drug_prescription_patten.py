from datetime import datetime
import db_connection as dc

cur = dc.get_db_connection()

# condition_occurrence에서 제 2형 당뇨병을 진단받은 환자를 추출
# drug_exposure과 concept 조인하여 drug_concept_id에 대한 약 이름을 구하고 split을 통해서 앞부분만 사용
# person_id, drug_name, drug_exposure_start_date이 중복되는 데이터를 먼저 제거
# 같은 날에 제공 받은 약에 대해서 () 로 묶음
# 약 변동이 있는 경우에 대해서 -> 로 변동 히스토리를 순차적으로 표시
# STRING_AGG 함수를 사용해서 drug_history 컬럼을 만들어서 사용

sql = '''
WITH drug_history_table AS (
    SELECT person_id, STRING_AGG(drug_history,'->') AS drug_history
    FROM (SELECT person_id, 
            CASE WHEN (STRING_AGG(drug_name,',') LIKE '%,%') THEN
                CONCAT('(', STRING_AGG(drug_name,','), ')')
            ELSE STRING_AGG(drug_name,',') END AS drug_history
        FROM (SELECT DISTINCT person_id, RTRIM(split_part(concept_name, ' ', 1),',') AS drug_name, drug_exposure_start_date
            FROM (SELECT visit_occurrence_id
                FROM condition_occurrence
                WHERE condition_concept_id IN (3191208,36684827,3194332,3193274,43531010,4130162,45766052,
                45757474,4099651,4129519,4063043,4230254,4193704,4304377,201826,3194082,3192767)) AS diabetes_person
            JOIN drug_exposure de ON de.visit_occurrence_id = diabetes_person.visit_occurrence_id
            JOIN concept c ON c.concept_id = de.drug_concept_id) AS drug_prescription
        GROUP BY person_id, drug_exposure_start_date) AS drug_history_table1
    GROUP BY person_id)

SELECT drug_history AS pattern, COUNT(person_id) AS person_count
FROM drug_history_table
GROUP BY pattern
ORDER BY person_count DESC;
'''

cur.execute(sql)
for row in cur.fetchall():
    print(row)
