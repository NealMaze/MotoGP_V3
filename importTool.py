import os
import csv
import mysql.connector
from importHelpers import *
from getpass import getpass

user = 'linroot'
print('user: linroot')
# password = getpass('enter MotoGP database password: ')
password = input('enter MotoGP database password: ')

# MySQL Connection Config
config = {
    'user': user,
    'password': password,
    'host': 'lin-14001-8310-mysql-primary.servers.linodedb.net',
}

try:
    # Create a connection using a context manager
    with mysql.connector.connect(**config) as conn:
        cursor = conn.cursor()

        database_name = 'your_database_name'

        setup_database(cursor, database_name)

        # Prompt user for year of data to import
        year = input("Enter the year to import data for: ")
        csv_dir = 'G:\Shared drives\MotoGP-Project\csvFiles\sessions'

        # List CSV files based on user input for year
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv') and str(year) in f]

        # Loop over the identified CSV files and import data to the respective tables
        for file in csv_files:
            league = identify_lge(file)
            process_csv_file(os.path.join(csv_dir, file), cursor, league, conn, months)

except mysql.connector.Error as err:
    print(f"Error: {err}")
