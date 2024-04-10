"""Main module."""
from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name
from createDatabase import create_database
if __name__ == '__main__':
    create_database(mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name)