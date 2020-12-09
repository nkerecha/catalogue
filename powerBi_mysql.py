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

    # conn_database -- Connect to a predefined database in the mysql instance
    def conn_database(self, database_name):
        config = self.setup()
        config['database'] = database_name
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        # Remember Naftal Connection has been left live.
        # Any other methods that call this method will need to kill that connection on completion
        return conn, cursor

user = 'root'
password = 'Torivega1234#'
host = '35.239.11.147'
client_flags = [ClientFlag.SSL]
ssl_ca = 'ssl/server-ca.pem'
ssl_cert = 'ssl/client-cert.pem'
ssl_key = 'ssl/client-key.pem'

instance = Connect(user, password, host, client_flags, ssl_ca, ssl_cert, ssl_key)
data = instance.fetch_all('guelph' , 'dummy')
frame = pd.DataFrame(data)
print(frame)
