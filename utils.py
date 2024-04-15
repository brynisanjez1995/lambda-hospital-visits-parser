#db connection cred
import psycopg2 
config = 'dbname=postgres user=my_user password=my_pass host=localhost'
    
def connect_db():
    try:
        #print("Connecting")
        conn = psycopg2.connect(config)
        cursor = conn.cursor()
        #print("Connected")
        return conn, cursor
    except Exception as error:
        print("error while connecting: "+ str(error))
        return 0, 0
# Test the database connection
connection, cursor = connect_db()
print(connection, cursor)