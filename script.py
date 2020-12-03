# Resolved Python Imports
from datetime import timedelta
import os
from flask import Flask, request, redirect, render_template, url_for, session
from flask import flash
from flask_session import Session
from azure_cloud_conn import Database
from google_cloud_conn import *
import numpy as np
import pandas as pd

# Constants ---
# SENT = 'AZURE'
SENT = 'GOOGLE'

# -- Flask App Initializations --
app = Flask(__name__)

SESSION_TYPE = 'filesystem'
app.config.from_object(__name__)
app.config['SESSION_PERMANENT'] = True
app.config['SESSION_TYPE'] = 'filesystem'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=1)
app.config['SESSION_FILE_THRESHOLD'] = 100
app.config['SECRET_KEY'] = '5f352379324c22463451387a0aec5d2f'
sess = Session()
sess.init_app(app)
# -- End of flask App Initialization --

# -- Google Cloud Mysql Instance (Login Credentials)--
user = 'root'
password = 'Torivega1234#'
host = '35.239.11.147'
client_flags = [ClientFlag.SSL]
ssl_ca = 'ssl/server-ca.pem'
ssl_cert = 'ssl/client-cert.pem'
ssl_key = 'ssl/client-key.pem'
# -- End of Google Cloud ---

# -- Azure SQL Servr Database Instance (Login Credentials) --
server = 'randomdata.azure_cloud_conn.windows.net'
database = 'testing'
username = 'Naftal'
password_azure = 'Torivega1234#'
driver = '{ODBC Driver 17 for SQL Server}'
# -- End of Azure Cloud SQL Server Instance ---


@app.route('/')
def home():
    return render_template('/home.html')


@app.route('/add', methods=['GET', 'POST'])
def add_data():
    if request.method == "POST":
        data_name = request.form.get('data_name')
        serial_number = request.form.get('serial_number')
        source_name = request.form.get('source_name')
        description = request.form.get('description')
        owner = request.form.get('data_owner')
        private_data = request.form.get('private_data')
        tags = request.form.get('tags')
        recurring_data = request.form.get('recurring_data')
        if (data_name == "" or serial_number == "" or source_name == "" or description == "" or owner == "" or private_data == "" or tags == ""
        or recurring_data == ""):
            print("The data has errors")
            flash("Submission Failed")
        else:
            if (SENT == "AZURE"):
                conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server +
                                      ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password_azure)
                cursor = conn.cursor()
                print("\n")
                print("Database connection : {}".format("LIVE"))
                cursor.execute("INSERT INTO dbo.Dummy (Data_Name,Serial_Number, Source_Name,Data_Description,Data_owner,Private_Data,Tags,Recurring_Data) VALUES (?,?,?,?,?,?,?,?);",
                               (str(data_name), str(serial_number), str(source_name), str(description), str(owner), str(private_data), str(tags), str(recurring_data)))
                conn.commit()
                print("This is an Azure SQL Server Operation")
                print("Database Connection : {}".format('LIVE'))
                print("Submission Test : {}".format("SUCESSFUL"))
                print("Data Added As: ")
                print("{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}".format(data_name, serial_number, source_name, description, owner, private_data, tags, recurring_data))
                cursor.close()
                conn.close()
                print("Database connection : {}\n".format("CLOSED"))
                flash("Your submission is successful")
                redirect(url_for('home'))
            elif (SENT == 'GOOGLE'):
                print("This is a Google Cloud MySqL Operation")
                data = Connect(user, password, host, client_flags, ssl_ca, ssl_cert, ssl_key)
                database_name = 'guelph'
                table_name = 'dummy'
                columns = "(Data_Name,Serial_Number, Source_Name,Data_Description,Data_owner,Private_Data,Tags,Recurring_Data)"
                qs = "(%s,%s,%s,%s,%s,%s,%s,%s)"
                val = (data_name, serial_number, source_name, description, owner, private_data, tags, recurring_data)

                data.single_insert(database_name, table_name, columns, qs , val)
                print("Database Connection : {}".format('LIVE'))
                print("Submission Test : {}".format("SUCESSFUL"))
                print("Data Added As: ")
                print("{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}".format(data_name, serial_number, source_name, description, owner, private_data, tags, recurring_data))
                print("Database connection : {}\n".format("CLOSED"))
                flash("Your submission is successful")
                redirect(url_for('home'))
    return render_template('/forms.html')


@app.route('/facilities')
def facilities():
    return render_template('/parks.html')


@app.route('/council')
def council():
    return render_template('/council.html')


@app.route('/licences')
def licences():

    return render_template('/licences.html')


@app.route('/display')
def display():
    if (SENT == 'AZURE'):
        # fetching azure_cloud_conn data and creating a json file from it
        display = Database(server, database, username, password, driver)
        fetched_data = display.fetch_data("dbo.Dummy")
        dataframe = pd.DataFrame(fetched_data)
        print(dataframe)
    elif (SENT == 'GOOGLE'):
        display = Connect(user, password, host, client_flags, ssl_ca, ssl_cert, ssl_key)
        fetched_data = display.fetch_all('guelph' , 'dummy')
        dataframe = pd.DataFrame(fetched_data)
        dataframe_html = list(dataframe.itertuples(index=False, name=None))
        headings = ("Data Name", "Serial Number", "Source Name", "Description", "Data Owner", "Private Data", "Tags", "Recurrent")
    return render_template('/display.html', headings=headings, data=dataframe_html)


if __name__ == '__main__':

    app.run(debug=True)
