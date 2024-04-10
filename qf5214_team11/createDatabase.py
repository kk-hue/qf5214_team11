import mysql.connector
from mysql.connector import errorcode
# from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name

def create_database(mysql_user: str, 
                    mysql_password: str,
                    mysql_hostname: str, 
                    mysql_database_name: str, 
                    mysql_table_name: str):
    
        try:
            cnx = mysql.connector.connect(host= mysql_hostname, user = mysql_user, password=mysql_password)
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

        stock_statement = ", 1_datetime FLOAT(6,2) NOT NULL, 2_open FLOAT(6,2) NOT NULL, 3_close FLOAT(6,2) NOT NULL,  4_high FLOAT(6,2) NOT NULL"\
            ", 5_low FLOAT(6,2) NOT NULL, 6_volume INT NOT NULL, 7_volume_price FLOAT(6,2) NOT NULL, 8_last_price FLOAT(6,2) NOT NULL"\
            ", 9_news_sentiment_score FLOAT(6,2) NOT NULL"

        main_statement = "CREATE TABLE IF NOT EXISTS " + mysql_table_name + "(ID MEDIUMINT KEY AUTO_INCREMENT" + stock_statement + ");"
        cursor.execute(main_statement)

