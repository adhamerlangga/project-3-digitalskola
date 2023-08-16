import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def connect_to_db(usr, pwd, host, db):
    user = usr
    passwd = pwd
    hostname = host
    database = db

    conn_string = f'postgresql://{user}:{passwd}@{hostname}:5432/{database}'

    engine = create_engine(conn_string)
    conn = engine.connect()
    return conn

def load(file, conn, table_name):
    
    # Read csv
    data = pd.read_csv(file, encoding='unicode_escape')
    data.to_sql(table_name, con=conn, if_exists='append', index=False)


if __name__ == '__main__':
    csv_path = 'C:/Users/Asus/Code/Bootcamp/Digital Skola/Week-7/Project-3/Project_3_task/global_youtube_stat.csv'

    connection = connect_to_db(usr = 'postgres', 
                               pwd = 'root', 
                               host = 'localhost', 
                               db = 'adham')
    
    try:
        load(csv_path, connection, 'latihan_users_adham_erlangga_siwi')
        print('data loaded to database successfully')
    except Exception as error:
        print('error is', error)
    
