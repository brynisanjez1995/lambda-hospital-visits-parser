create_hospital_a = '''
Create table if not exists 
hospital_a(
    visit_id int primary key,
    name varchar,
    age int,
    gender varchar,
    visit_date date,
    diagnosis varchar, 
    created_at timestamp default now(),
    updated_at timestamp default now()
)
'''

create_hospital_b = '''
Create table if not exists 
hospital_b(
    visit_id int primary key,
    name varchar,
    age int,
    gender varchar,
    visit_date date,
    diagnosis varchar, 
    created_at timestamp default now(),
    updated_at timestamp default now()
)
'''

create_seq = 'create sequence if not exists dw_patient_id_seq'

create_patient = '''
Create table if not exists 
dw_patient(
    id int primary key default nextval('dw_patient_id_seq'), 
    name varchar, 
    age int, 
    gender varchar,
    source varchar, 
    created_at timestamp default now(),
    updated_at timestamp default now(),
    unique (name,age,gender)
)
'''

insert_hospital_a = '''
Insert into hospital_a(visit_id,name,age,gender,visit_date,diagnosis) 
values(%s, %s, %s, %s, %s, %s)
on conflict(visit_id) 
    do update set 
        name = excluded.name, 
        age = excluded.age, 
        gender = excluded.gender,
        visit_date = excluded.visit_date, 
        diagnosis = excluded.diagnosis, 
        updated_at = excluded.updated_at 
'''

insert_hospital_b = '''
Insert into hospital_b(visit_id,name,age,gender,visit_date,diagnosis) 
values(%s, %s, %s, %s, %s, %s)
on conflict(visit_id) do update set 
    name = excluded.name, 
    age = excluded.age, 
    gender = excluded.gender,
    visit_date = excluded.visit_date, 
    diagnosis = excluded.diagnosis, 
    updated_at = excluded.updated_at 
'''

insert_patients = '''
Insert into dw_patient(name,age,gender,source ) 
values (%s, %s, %s, %s)
on conflict(name,age,gender) Do nothing
'''
