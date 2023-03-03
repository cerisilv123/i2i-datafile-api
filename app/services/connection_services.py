import os
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def create_connection(isDatafile = False):
    
    # Create DB Connection string 
    driver = pyodbc.drivers()
    print(driver)
    
    connection_string = ''
    connection_url = ''
    
    if isDatafile:
        connection_string = f'DRIVER={driver[0]};SERVER={os.getenv(f"SERVER")};DATABASE={os.getenv(f"DATAFILE_DATABASE_NAME")};UID={os.getenv(f"UID")};PWD={os.getenv(f"PWD")}'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    else: 
        connection_string = f'DRIVER={driver[0]};SERVER={os.getenv(f"SERVER")};DATABASE={os.getenv(f"DATABASE_NAME")};UID={os.getenv(f"UID")};PWD={os.getenv(f"PWD")}'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        
    engine = create_engine(connection_url)
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind = engine)
    session = Session()

    return engine, session

def create_connection_cursor(isDatafile = False):
    
    driver = pyodbc.drivers()
    
    conn = ''
    cursor = ''
    
    if isDatafile:
        conn = pyodbc.connect(f'DRIVER={driver[0]};SERVER={os.getenv(f"SERVER")};DATABASE={os.getenv(f"DATAFILE_DATABASE_NAME")};UID={os.getenv(f"UID")};PWD={os.getenv(f"PWD")}')
        cursor = conn.cursor()
    else: 
        conn = pyodbc.connect(f'DRIVER={driver[0]};SERVER={os.getenv(f"SERVER")};DATABASE={os.getenv(f"DATABASE_NAME")};UID={os.getenv(f"UID")};PWD={os.getenv(f"PWD")}')
        cursor = conn.cursor()

    return conn, cursor