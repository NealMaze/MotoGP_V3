# imports
import os
import mysql.connector
from importHelpers import *
from dataCleaningHelpers import *
import winsound
# from getpass import getpass

frequency = 1000
duration = 300

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

        database_name = 'MotoGP'

        setup_database(cursor, database_name)

        # Prompt user for year of data to import
        year = input("Enter the year to import data from.\n> ")
        rnd = input("Enter the round to import data from.\n(to import all files, please enter 'all'\n> ")

        csv_dir = 'G:\Shared drives\MotoGP-Project\csvFiles\sessions'

        # List CSV files based on user input for year
        if rnd == "all": csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv') and str(year) in f]
        else: csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv') and str(year) in f and f"Round_{rnd}" in f]

        # Loop over the identified CSV files and import data to the respective tables
        for file in csv_files:
            winsound.Beep(frequency, duration)

            if not is_file_processed(cursor, file):
                print(f"{file}")
                league = identify_lge(file)
                process_csv_file(os.path.join(csv_dir, file), cursor, league, conn)

            else: print (f"\n   ###   ###   ###   \nfile already processed\n{file}\n   ###   ###   ###   \n")

except mysql.connector.Error as err:
    print(f"Error: {err}\n")

play_mario()
