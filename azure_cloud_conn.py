# Separate python scripts to be imported into scripts.property
# IMPORTS
import pyodbc


# Class Variable declaration
class Database:

    # Declaration of  database Intialization
    def __init__(self, server, database, username, password, driver):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

    # Methods

    def connect(self):
        conn = pyodbc.connect('DRIVER=' + self.driver + ';SERVER=' + self.server + ';PORT=1433;DATABASE=' + self.database + ';UID=' + self.username + ';PWD=' + self.password)
        cursor = conn.cursor()
        return conn, cursor

    def fetch_data(self, table):
        conn, cursor = self.connect()
        cursor.execute("SELECT * FROM %s" % (table))
        row = cursor.fetchone()
        fetched = []
        while row:
            placer = []
            print(str(row[0]) + "\n" + str(row[1]) + "\n" + str(row[2]) + "\n" + str(row[3]) + "\n" + str(row[4]) + "\n" + str(row[5]) + "\n" + 
             str(row[6]) + "\n" + str(row[7]) + "\n")
            placer.append(str(row[0]))
            placer.append(str(row[1]))
            placer.append(str(row[2]))
            placer.append(str(row[3]))
            placer.append(str(row[4]))
            placer.append(str(row[5]))
            placer.append(str(row[6]))
            placer.append(str(row[7]))
            fetched.append(placer)
            row = cursor.fetchone()
        cursor.close()
        conn.close()
        return fetched
    
    def insert_data(self, table, columns, qs, data):
        conn, cursor = self.connect()
        cursor.execute("INSERT INTO {} {} VALUES {};".format(table, columns, qs), ("{}".format(data)))
        conn.commit()
        cursor.close()
        conn.close()
        return print("Attempted Insertion")
