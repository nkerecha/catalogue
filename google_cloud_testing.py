# Testing site for google cloud sending and retrieving data 
from google_cloud_conn import *

user = 'root'
password = 'Torivega1234#'
host = '35.239.11.147'
client_flags = [ClientFlag.SSL]
ssl_ca = 'ssl/server-ca.pem'
ssl_cert = 'ssl/client-cert.pem'
ssl_key = 'ssl/client-key.pem'
    
# Naftal's MYSQL Instance Credentials 
instance = Connect(user, password, host, client_flags, ssl_ca, ssl_cert, ssl_key)

# Create table in guelph azure_cloud_conn 

querry = """(Data_Name LONGTEXT, Serial_Number LONGTEXT,
    Source_Name LONGTEXT, Data_Description LONGTEXT, Data_Owner LONGTEXT,
    Private_Data LONGTEXT, Tags LONGTEXT, Recurring_Data LONGTEXT)"""

# instance.create_table('guelph', 'dummy', querry)

# Insert data Into the table 
"""
columns = "(Data_Name,Serial_Number, Source_Name,Data_Description,Data_owner,Private_Data,Tags,Recurring_Data)"
qs = "(%s,%s,%s,%s,%s,%s,%s,%s)"
dataframe = pd.read_csv('static/data.csv')
val = []
for i in range(dataframe.shape[0]):
    holder = []
    for i in dataframe.iloc[i]:
        holder.append(str(i))
    val.append(tuple(holder))
instance.multi_insert('guelph', 'dummy', columns, qs , val)
"""

data = instance.fetch_all('guelph' , 'dummy')
frame = pd.DataFrame(data)
print(frame.to_json('static/data.txt', orient='records'))
