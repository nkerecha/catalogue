# This is meant to be a class that ses up the connection to sql 
# Sections of the code are also sql scripts that interface with the db 

# Python Used Imports 
from _pytest.mark import param
from mysql.connector.constants import ClientFlag
import mysql.connector
import pandas as pd


# Aim of class is to create a way to easily and securely query the database without interacting with it directly 
class Connect:
    
    # Definition of connection parameters as specified for google sql instance
    def __init__(self, user, password, host, client_flags, ssl_ca, ssl_cert, ssl_key):
        self.user = user
        self.password = password 
        self.host = host
        self.client_flags = client_flags 
        self.ssl_ca = ssl_ca
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
    
    # Predefined configuration parameters (Google Cloud MySQL Instance)
    def setup(self):
        config = {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'client_flags': self.client_flags,
            'ssl_ca': self.ssl_ca,
            'ssl_cert': self.ssl_cert,
            'ssl_key': self.ssl_key
            }
        return config
    
    # Create a database
    def database(self, database_name):
        config = self.setup()
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE {}'.format(database_name))
        conn.close()
        return None 
    
    # conn_database -- Connect to a predefined database in the mysql instance 
    def conn_database(self, database_name):
        config = self.setup()
        config['database'] = database_name
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        # Remember Naftal Connection has been left live.
        # Any other methods that call this method will need to kill that connection on completion
        return conn, cursor

    # Query to create table within a predefined mysql instance and database
    # table_querry requirements
    # columns represents a list of the table columns
    def create_table(self, database_name, table_name, columns):
        conn, cursor = self.conn_database(database_name)
        cursor.execute("CREATE TABLE {} {}".format(table_name, columns))
        conn.commit()
        conn.close()  # Killed Connection to database 
        print("SUCCESFUL & DONE")
        return None
    
    # Single pieces of information 
    def single_insert(self, database_name, table_name, columns, qs , data):
        conn, cursor = self.conn_database(database_name)
        sql = "INSERT INTO {} {} VALUES {}".format(table_name, columns, qs)
        cursor.execute(sql, data)
        conn.commit()
        print(cursor.rowcount, "was inserted.")
        conn.close()
        return None 
    
    # Insert multiple pieces of data at the same time 
    def multi_insert(self, database_name, table_name, columns, qs, data):
        conn, cursor = self.conn_database(database_name)
        sql = "INSERT INTO {} {} VALUES {}".format(table_name, columns, qs)
        cursor.executemany(sql, data)
        conn.commit()
        print(cursor.rowcount, "was inserted.")
        conn.close()
        return None
    
    # Retrieve pieces of data stored in the mysql instance 
    def fetch_all(self, database_name, table_name):
        conn, cursor = self.conn_database(database_name)
        cursor.execute("SELECT * FROM {}.{}".format(database_name, str(table_name)))
        # Closed connection
        data = cursor.fetchall()
        conn.close()
        return data
    
    # fetch data that fits a particular parameter 
    def fetch_where(self, database_name, table_name, column_name, value):
        conn, cursor = self.conn_database(database_name)
        sql = "SELECT * FROM {} WHERE {} = {}".format(table_name, column_name, value)
        cursor.execute(sql)
        data = cursor.fetchall()
        conn.close()
        return data
    
    # Update data for a particular row where the row_id meets a particular criteria 
    def update_data(self, database_name, table_name, column_name, criteria_column, update_value, criteria_value):
        conn, cursor = self.conn_database(database_name)
        sql = "UPDATE {} SET {} = {} WHERE {} = {}".format(table_name, column_name, update_value, criteria_column, criteria_value)
        cursor.execute(sql)
        conn.commit()
        conn.close()
        return None 
