def setup_database(cursor, database_name):
    """
    Set up the database and its tables.

    Args:
    - cursor (cursor object): MySQL cursor object to execute commands.
    - database_name (str): The name of the database to create and use.
    """
    # Create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE {database_name}")  # Switch to the database

    # Define tables and their respective columns
    tables = {
        'MotoGP_Laps': '''(
            file_reference VARCHAR(255),
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            track VARCHAR(255),
            rider_ID INT,
            run INT,
            f_tire VARCHAR(255),
            r_tire VARCHAR(255),
            laps_on_f INT,
            laps_on_r INT,
            lap INT,
            lap_time TIME,
            pit VARCHAR(255),
            sec_one TIME,
            sec_two TIME,
            sec_thr TIME,
            sec_four TIME,
            avg_speed FLOAT,
            PRIMARY KEY (event_date, lge, ses_type)
        )''',
        'Moto2_Laps': '''(
            file_reference VARCHAR(255),
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            track VARCHAR(255),
            rider_ID INT,
            run INT,
            f_tire VARCHAR(255),
            r_tire VARCHAR(255),
            laps_on_f INT,
            laps_on_r INT,
            lap INT,
            lap_time TIME,
            pit VARCHAR(255),
            sec_one TIME,
            sec_two TIME,
            sec_thr TIME,
            sec_four TIME,
            avg_speed FLOAT,
            PRIMARY KEY (event_date, lge, ses_type)
        )''',
        'Moto3_Laps': '''(
            file_reference VARCHAR(255),
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            track VARCHAR(255),
            rider_ID INT,
            run INT,
            f_tire VARCHAR(255),
            r_tire VARCHAR(255),
            laps_on_f INT,
            laps_on_r INT,
            lap INT,
            lap_time TIME,
            pit VARCHAR(255),
            sec_one TIME,
            sec_two TIME,
            sec_thr TIME,
            sec_four TIME,
            avg_speed FLOAT,
            PRIMARY KEY (event_date, lge, ses_type)
        )''',
        'Sessions': '''(
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            weather VARCHAR(255),
            trk VARCHAR(255),
            humidity INT,
            track_temp FLOAT,
            air_temp FLOAT,
            PRIMARY KEY (event_date, lge, ses_type)
        )''',
        'Riders': '''(
            rdr_id INT PRIMARY KEY,
            f_name VARCHAR(255),
            l_name VARCHAR(255),
            nationality VARCHAR(255),
            dob DATE,
            height FLOAT,
            weight FLOAT
        )'''
        # ... Add definitions for other tables as needed
    }

    # Create tables if they don't exist
    for table, columns in tables.items():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} {columns}")

def identify_lge(filename):
    # Extract data type from the filename to identify the table
    lge = filename.split('-')[1].replace('.csv', '')
    return lge

def insert_data(cursor, table_name, data_dict):
    columns = ', '.join(data_dict.keys())
    placeholder = ', '.join(['%s'] * len(data_dict))
    query = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholder})"
    cursor.execute(query, list(data_dict.values()))

def process_csv_file(filename, cursor, league, conn):
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        row_counter = 1  # Initialize a counter for each row in the current file.

        for row in reader:
            # Convert month name to its number
            month_number = convert_month_to_number(row['month'])

            # Create the file reference identifier
            file_ref = f"{filename}_{row_counter}"

            # Extract data for the Laps table of the respective league
            laps_data = {
                'file_reference': file_ref,
                'event_date': f"{row['yr']}-{month_number}-{row['day']}",
                'lge': row['lge'],
                'ses_type': row['session'],
                'track': row['trk'],
                'rider_ID': row['rdr_num'],
                'run': row.get('run', None),
                'f_tire': row.get('f_tire', None),
                'r_tire': row.get('r_tire', None),
                'laps_on_f': row.get('laps_on_f', None),
                'laps_on_r': row.get('laps_on_r', None),
                'lap': row.get('lap', None),
                'lap_time': row.get('lap_time', None),
                'pit': row.get('pit', None),
                'sec_one': row.get('sec_one', None),
                'sec_two': row.get('sec_two', None),
                'sec_thr': row.get('sec_thr', None),
                'sec_four': row.get('sec_four', None),
                'avg_speed': row.get('avg_speed', None)
            }

            # Based on the league, determine the table name
            if league == "Moto3":
                table_name = "Moto3_Laps"
            elif league == "Moto2":
                table_name = "Moto2_Laps"
            elif league == "MotoGP":
                table_name = "MotoGP_Laps"

            # Insert the data into the correct table
            insert_data(cursor, table_name, laps_data)

            # Extract data for the Sessions table
            session_data = {
                'event_date': f"{row['yr']}-{month_number}-{row['day']}",
                'lge': row['lge'],
                'ses_type': row['session'],
                'weather': row.get('weather', None),
                'trk': row['trk'],
                'humidity': None,
                'track_temp': None,
                'air_temp': None
            }
            insert_data(cursor, 'Sessions', session_data)

            # Extract data for the Riders table
            rider_data = {
                'rdr_id': row['rdr_num'],
                'f_name': row['f_name'],
                'l_name': row['l_name'],
                'nationality': row.get('nat', None),
                'dob': None,
                'height': None,
                'weight': None
            }
            insert_data(cursor, 'Riders', rider_data)

        conn.commit()

def convert_month_to_number(month_name):
    """Convert month name to its corresponding number."""
    months = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }
    return months.get(month_name, '00')  # default to '00' if month_name is not found
