import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name, mysql_port

'''
Make sure you have 'merged_data.csv' in the data folder first,
Otherwise please see DataProcessing.py. Please make sure you have
mysql and your local host is running, my sql workbench is also recommanded 
for visualising database. Using command line to run 
'poetry run python ./qf5214_team11/createDatabase.py',
this script will be create the data schema and perform
data ingestion from datafram to mysql database.
'''

df = pd.read_csv('./Data/merged_data.csv')

def create_database(mysql_user: str, 
                    mysql_password: str,
                    mysql_hostname: str, 
                    mysql_database_name: str, 
                    mysql_table_name: str,
                    mysql_port: str):
        try:
            cnx = mysql.connector.connect(host=mysql_hostname, 
                                          user=mysql_user, 
                                          password=mysql_password,
                                          auth_plugin= 'mysql_native_password')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("User name or password incorrect")
            else:
                print(err)
            # Close connection
            cnx.close()
            print('Connection closed') 
         
        # Instantiate cursor object
        cursor = cnx.cursor()

        # Create database (mysql_database_name) if not exists
        cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(mysql_database_name))

        # Use given database
        cursor.execute("USE {}".format(mysql_database_name))

        merge_statement = ", Timestamp TIMESTAMP NOT NULL, Open FLOAT(6,2), Close FLOAT(6,2),  High FLOAT(6,2)"\
            ", Low FLOAT(6,2) , Volume INT, Volume_Dollar FLOAT(6,2), Last_Price FLOAT(6,3)"\
            ", Ticker CHAR, stock_queried CHAR NOT NULL, score FLOAT(6,2)"\
        
        main_statement = "CREATE TABLE IF NOT EXISTS " + mysql_table_name + "(ID MEDIUMINT KEY AUTO_INCREMENT" + merge_statement  + ");"
        cursor.execute(main_statement)
        
        # Create a connection string
        connection_string = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_hostname}:{mysql_port}/{mysql_database_name}'
        # Establish a connection to the MySQL database
        engine = create_engine(connection_string)

        try:
            df.to_sql(name=mysql_table_name, con=engine, if_exists='replace', index=False)
        except Exception as e:
            print("Error:", e)

if __name__ == '__main__':
     create_database(mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name, mysql_port)