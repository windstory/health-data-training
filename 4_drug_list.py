import db_connection as dc

cur = dc.get_db_connection()
sql = '''
with drug_list as (
select distinct drug_concept_id, concept_name, count(*) as cnt from
drug_exposure de
join concept
on drug_concept_id = concept_id
where concept_id in (
40213154,19078106,19009384,40224172,19127663,1511248,40169216,1539463,
19126352,1539411,1332419,40163924,19030765,19106768,19075601)
group by drug_concept_id,concept_name
order by count(*) desc
)
, drugs as (select drug_concept_id, concept_name from drug_list)

, prescription_count as (select drug_concept_id, cnt from drug_list)

SELECT d.concept_name
FROM drug_pair dp
JOIN drugs d ON d.drug_concept_id = dp.drug_concept_id1
JOIN prescription_count pc1 ON pc1.drug_concept_id = dp.drug_concept_id1
JOIN prescription_count pc2 ON pc2.drug_concept_id = dp.drug_concept_id2
WHERE pc1.cnt < pc2.cnt
ORDER BY pc1.cnt DESC
'''

cur.execute(sql)
for row in cur.fetchall():
    print(row)