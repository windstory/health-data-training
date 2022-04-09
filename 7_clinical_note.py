import db_connection as dc
import re

from datetime import datetime
from config import config

def get_clinical_note(db_con):
    sql = '''
    SELECT * FROM clinical_note;
    '''
    db_con.execute(sql)
    
    data_list = []
    for data in db_con.fetchall():
        data_list.append(str(data))
    return data_list

def create_person_table(db_con):
    sql = '''
    DROP TABLE IF EXISTS {schema}.person;
    CREATE TABLE {schema}.person (
        person_id bigint primary key generated always as identity,
        year_of_birth int not null,
        month_of_birth int,
        day_of_birth int,
        death_date date,
        gender_value varchar(50),
        race_value varchar(50),
        ethnicity_value varchar(50)
    );
    '''.format(schema=config['schema'])
    db_con.execute(sql)

def insert_person_table(db_con, data):
    person_key = ["Gender", "Birth Date", "Race", "Ethnicity"]
    person_info = {}

    for key in person_key:
        pattern = r"{}:\s+([\w|-]+)\\n".format(key)
        p = re.compile(pattern)
        m = p.search(data)
        if m:
            person_info[key] = m.groups()[0]
        else:
            print(data)
        
    birth_date = datetime.strptime(person_info["Birth Date"], "%Y-%m-%d")
    year = birth_date.year
    month = birth_date.month
    day = birth_date.day
    gender = person_info['Gender']
    race = person_info['Race']
    ethnicity = person_info['Ethnicity']

    sql = '''
    INSERT INTO {schema}.person(
        year_of_birth, month_of_birth, day_of_birth, gender_value, race_value, ethnicity_value)
    VALUES({year}, {month}, {day}, '{gender}', '{race}', '{ethnicity}');
    '''.format(year=year, month=month, day=day, \
        gender=gender, race=race, ethnicity=ethnicity, schema=config['schema'])
    db_con.execute(sql)

    sql = '''
    SELECT MAX(person_id) FROM {schema}.person;
    '''.format(schema=config['schema'])
    db_con.execute(sql)
    return db_con.fetchone()[0] + 1

def create_visit_occurrence_table(db_con):
    sql = '''
    DROP TABLE IF EXISTS {schema}.visit_occurrence;
    CREATE TABLE {schema}.visit_occurrence (
        visit_occurrence_id bigint primary key generated always as identity,
        person_id bigint not null,
        visit_start_date date,
        care_site_nm text,
        visit_type_value varchar(50)
    );
    '''.format(schema=config['schema'])
    db_con.execute(sql)

def insert_visit_occurrence_table(db_con, data, person_id):
    visit_occurrence_info = {"visit_start_date":"", "care_site_nm":"", "visit_type_value":""}
    pattern = r"ENCOUNTER\\n([0-9|-]*)[\s*:\s*]*([\w|\s]*)[\s*:\s*]*([\w|\s|(|)])*\\n"
    p = re.compile(pattern)
    m = p.search(data)
    if m:
        if m.groups()[0]: visit_occurrence_info["visit_start_date"] = m.groups()[0]
        if m.groups()[1]: visit_occurrence_info["care_site_nm"] = m.groups()[1]
        if m.groups()[2]: visit_occurrence_info["visit_type_value"] = m.groups()[2]

    sql = '''
    INSERT INTO {schema}.visit_occurrence(
        person_id, visit_start_date, care_site_nm, visit_type_value)
    VALUES({id}, {visit_start_date}, '{care_site_nm}', '{visit_type_value}');
    '''.format(schema=config['schema'], id=person_id, \
        visit_start_date=datetime.strptime(visit_occurrence_info["visit_start_date"], "%Y-%m-%d"), \
        care_site_nm=visit_occurrence_info["care_site_nm"], \
        visit_type_value=visit_occurrence_info["visit_type_value"])
    db_con.execute(sql)

    sql = '''
    SELECT MAX(visit_occurrence_id) FROM {schema}.visit_occurrence;
    '''.format(schema=config['schema'])
    db_con.execute(sql)
    return db_con.fetchone()[0] + 1




# 정규식을 사용해서 clinical_note 데이터에서 필요한 정보를 매칭으로 찾는 방식으로 구현
# 지금 소스코드에서는 person, visit_occurrence 테이블만 생성 및 데이터 삽입이 구현
# 현재 만들어진 방식을 통해서 나머지도 구현하려고 했으나 시간이 부족했습니다.
# insert_visit_occurrence_table() 수행 시 visit_start_date에 python의 datetime과 postgresql date 타입이 맞지 않아 데이터가 삽입이 안되는 상태입니다.


db_con = dc.get_db_connection()
create_person_table(db_con)
create_visit_occurrence_table(db_con)

data_list = get_clinical_note(db_con)
for data in data_list:
    person_id = insert_person_table(db_con, data)
    visit_occurrence_id = insert_visit_occurrence_table(db_con, data, person_id)