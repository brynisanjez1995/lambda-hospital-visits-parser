import pandas as pd
import os 
from utils import connect_db
import query
from datetime import datetime 

# Get today's date
def get_today_date_str():
    return datetime.now().strftime('%Y-%m-%d')

# Get hospital A file
def read_hospital_a_file():
    
    file_path = f"s3://hospital-visits/{get_today_date_str()}/hospital_a_data.csv"
    # file_path = "C:\\Users\\bryni\\Downloads\\lambda-hospital-visits-parser\\in-files\\hospital_a_data.csv"
    # Needs environment variables:
    #    -aws_access_key_id
    #    - aws_secret_access_key
    #    - aws_session_token
    df = pd.read_csv(file_path)
    return df

# Get hospital B file
def read_hospital_b_file():
    file_path = f"s3://hospital-visits/{get_today_date_str()}/hospital_b_data.csv"
    #file_path = "C:\\Users\\bryni\\Downloads\\lambda-hospital-visits-parser\\in-files\\hospital_b_data.csv"
    df = pd.read_csv(file_path)
    df = df.rename(columns={'Sex': 'Gender'})
    df['Gender'] = df['Gender'].replace({'F': 'Female', 'M': 'Male'})
    return df

# Given visits data from two hospitals, extract unique patients
def extract_unique_patients(hospital_a_visits_df, hospital_b_visits_df):
    # Extract unique patients
    merged_vists_df = hospital_a_visits_df.merge(hospital_b_visits_df, on=['Name','Gender','Age'], how='outer', indicator=True)
    # Keep only unique rows
    unique_patients_df = merged_vists_df.drop_duplicates()
    # Create a new column indicating the source of each row
    unique_patients_df['source'] = unique_patients_df['_merge'].map({
        'both': 'Both',
        'left_only': 'A',
        'right_only': 'B'
    })
    return unique_patients_df[['Name', 'Age', 'Gender', 'source']]


# create query list
create_queries = [query.create_hospital_a,query.create_hospital_b,query.create_seq,query.create_patient] 

# Specify the file paths
root_path = os.getcwd()

#create table if not exists
def execute_create_query(conn,cursor,query):
    cursor.execute(query)
    conn.commit()

def create_tables():
    conn,cursor = connect_db()
    try:   
        print('Creating tables if they do not exist...')   
        for create_query in create_queries:
            execute_create_query(conn,cursor,create_query)
    finally: 
        conn.close()

#load data into db
def load_data_into_db(conn, cursor,hospital_x_df,insert_query):
    for index, row in hospital_x_df.iterrows():
        cursor.execute(insert_query, tuple(row))
    conn.commit()

def load_all_data_into_db(insert_df_and_queries):
    conn,cursor = connect_db()
    try:   
        for df,insert_query in insert_df_and_queries:
            load_data_into_db(conn, cursor,df,insert_query)
    finally:
        conn.close()

def process_hospital_visits():
    try:   
        create_tables()
        print('Created missing tables')

        # Read hospital visits data
        hospital_a_visits_df = read_hospital_a_file()
        print('Data extracted for hospital A')
        hospital_b_visits_df = read_hospital_b_file()
        print('Data extracted for hospital B')
        
        unique_patients_df = extract_unique_patients(hospital_a_visits_df,hospital_b_visits_df)
        print('Unique Patient data extracted')

        insert_df_and_queries = [
            (hospital_a_visits_df, query.insert_hospital_a),
            (hospital_b_visits_df, query.insert_hospital_b),
            (unique_patients_df, query.insert_patients)
        ]
        load_all_data_into_db(insert_df_and_queries)
        print('Data loaded into Tables')
        
    except Exception as e:
        print(f'Error processing data: {e}')
        raise

def lambda_handler(event,context):
    process_hospital_visits()

# process_hospital_visits()

    