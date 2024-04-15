# Lambda Hospital data  Parser
This is a Python-based Lambda function designed to process hospital visits data from two hospitals, extract unique patients, and load the data into a PostgreSQL database.

## Features
- Reads hospital visits (CSV files) from S3 and normalizes the files into a common format
- Extracts unique patients from the combined data of both hospitals.
- Creates missing database tables if they do not exist.
- Loads hospital visit data in each hospital's table 
- Loads the unique patient data from both hospital into one table.

## Requirements
- Python 3.x
- pandas
- psycopg2

## Usage
- Setup the following env variables
    - aws_access_key_id
    - aws_secret_access_key
    - aws_session_token
- Configure the database connection credentials in utils.py.
- It expects the inputs files in
    - s3://hospital-visits/YYYY-MM-DD/hospital_a_data.csv
    - s3://hospital-visits/YYYY-MM-DD/hospital_b_data.csv
- Sample files are included in `in-files` directory
- Deploy the script files to AWS lambda and invoke


## Database Handling
- The ON CONFLICT clause is used in SQL queries to handle conflicts during data insertion, ensuring data integrity upon re-runs
- Primary key sequence generation is handled automatically by PostgreSQL for the dw_patient table using the dw_patient_id_seq sequence.
