import csv

def setup_database(cursor, database_name):
    """
    Set up the database and its tables.

    Args:
    - cursor (cursor object): MySQL cursor object to execute commands.
    - database_name (str): The name of the database to create and use.
    """
    # print("setup_database")

    # Create the database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE {database_name}")  # Switch to the database

    # Define tables and their respective columns
    tables = {
        'Processed_Files': '''(
            file_name VARCHAR(255),
            process_status VARCHAR(255),
            PRIMARY KEY (file_name)
        )''',
        'MotoGP_Laps': '''(
            file_reference VARCHAR(255),
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            rider_name VARCHAR(255),
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
            PRIMARY KEY (file_reference)
        )''',
        'Moto2_Laps': '''(
            file_reference VARCHAR(255),
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            rider_name VARCHAR(255),
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
            PRIMARY KEY (file_reference)
        )''',
        'Moto3_Laps': '''(
            file_reference VARCHAR(255),
            event_date DATE,
            lge VARCHAR(255),
            ses_type VARCHAR(255),
            rider_name VARCHAR(255),
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
            PRIMARY KEY (file_reference)
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
        print(table)
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} {columns}")

def identify_lge(filename):
    # print("identify_lge")
    # Extract data type from the filename to identify the table
    lge = filename.split('-')[1].replace('.csv', '')
    return lge

def insert_data(cursor, table_name, data_dict):
    # print("insert_data")
    columns = ', '.join(data_dict.keys())
    placeholder = ', '.join(['%s'] * len(data_dict))
    query = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholder})"
    cursor.execute(query, list(data_dict.values()))

def extract_lap_data(row, fileRef):
    """Extract data for the Laps table."""
    # print("extract_lap_data")
    month_number = convert_month_to_number(row['month'])
    return {
        'file_reference': fileRef,
        'event_date': f"{row['yr']}-{month_number}-{row['day']}",
        'lge': row['lge'],
        'ses_type': row['session'],
        'rider_name': f"{row['f_name']} {row['l_name']}",
        'run': row.get('run', None),
        'f_tire': row.get('f_tire', None),
        'r_tire': row.get('r_tire', None),
        'laps_on_f': row.get('laps_on_f', None),
        'laps_on_r': row.get('laps_on_r', None),
        'lap': row.get('lap_num', None),
        'lap_time': row.get('lap_time', None),
        'pit': row.get('pit', None),
        'sec_one': row.get('sec_one', None),
        'sec_two': row.get('sec_two', None),
        'sec_thr': row.get('sec_thr', None),
        'sec_four': row.get('sec_four', None),
        'avg_speed': row.get('avg_speed', None)
    }

def extract_session_data(row):
    """Extract data for the Sessions table."""
    # print("extract_session_data")
    month_number = convert_month_to_number(row['month'])
    return {
        'event_date': f"{row['yr']}-{month_number}-{row['day']}",
        'lge': row['lge'],
        'ses_type': row['session'],
        'weather': row.get('weather', None),
        'trk': row['trk'],
        'humidity': None,
        'track_temp': None,
        'air_temp': None
    }

def extract_rider_data(row):
    """Extract data for the Riders table."""
    # print("extract_rider_data")
    return {
        'rdr_id': row['rdr_num'],
        'f_name': row['f_name'],
        'l_name': row['l_name'],
        'nationality': row.get('nat', None),
        'dob': None,
        'height': None,
        'weight': None
    }

def convert_month_to_number(month_name):
    """Convert month name to its corresponding number."""
    # print("convert_month_to_number")
    months = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }
    return months.get(month_name, '00')  # default to '00' if month_name is not found

def record_csv_file(cursor, filename):
    """Record the start of processing of a CSV file."""
    # print("record_csv_file")
    cursor.execute(
        "INSERT INTO Processed_Files (file_name, process_status) VALUES (%s, %s)",
        (filename, 'processing')
    )

def complete_csv_file(cursor, filename):
    """Mark a CSV file as successfully processed."""
    # print("complete_csv_file")
    cursor.execute(
        "UPDATE Processed_Files SET process_status = %s WHERE file_name = %s",
        ('complete', filename)
    )

def incomplete_csv_file(cursor, filename):
    """Mark a CSV file as having an error during processing."""
    # print("incomplete_csv_file")
    cursor.execute(
        "UPDATE Processed_Files SET process_status = %s WHERE file_name = %s",
        ('incomplete', filename)
    )

def process_csv_file(filename, cursor, league, conn):
    # print("process_csv_file")
    try:
        # Record the start of processing
        record_csv_file(cursor, filename)

        with open(filename, 'r') as csvfile:

            reader = csv.DictReader(csvfile)
            table_name = f"{league}_Laps"  # Construct table name based on the league

            for row_counter, row in enumerate(reader, start=1):  # Use enumerate instead of manual counter
                fileRef = str(filename) + str(row_counter)
                laps_data = extract_lap_data(row, fileRef)
                insert_data(cursor, table_name, laps_data)

                session_data = extract_session_data(row)
                insert_data(cursor, 'Sessions', session_data)

                rider_data = extract_rider_data(row)
                insert_data(cursor, 'Riders', rider_data)

            conn.commit()

        # Mark the CSV as successfully processed
        complete_csv_file(cursor, filename)

    except Exception as e:
        # If there's an error during processing, mark the CSV as incomplete
        print(f"Error processing {filename}: {e}")
        incomplete_csv_file(cursor, filename)
