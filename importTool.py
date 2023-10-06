import os
import mysql.connector
from importHelpers import *
import winsound
from getpass import getpass

frequency = 1500  # Set Frequency To 2500 Hertz
duration = 400  # Set Duration To 1000 ms == 1 second

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
        # year = input("Enter the year to import data from.\n> ")
        # rnd = input("Enter the round to import data from.\n(to import all files, please enter 'all'\n> ")
        year = "2021"
        rnd = "2"

        csv_dir = 'G:\Shared drives\MotoGP-Project\csvFiles\sessions'

        # List CSV files based on user input for year
        if rnd == "all": csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv') and str(year) in f]
        else: csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv') and str(year) in f and f"Round_{rnd}" in f]

        # Loop over the identified CSV files and import data to the respective tables
        for file in csv_files:
            print(f"{file}")
            winsound.Beep(frequency, duration)
            league = identify_lge(file)
            process_csv_file(os.path.join(csv_dir, file), cursor, league, conn)

except mysql.connector.Error as err:
    print(f"Error: {err}\n")

winsound.Beep(frequency, duration)
winsound.Beep(frequency, duration)
winsound.Beep(frequency, duration)
winsound.Beep(frequency, duration)
winsound.Beep(frequency, duration)
