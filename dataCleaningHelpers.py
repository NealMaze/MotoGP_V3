# def handle_missing_values(row):
#     """Handle missing values for a given row."""
#     non_null_columns = ["month", "day",""]
#
#
#     return row, is_valid

def correct_data_types(row):
    """Ensure that data types of the row are consistent with the schema."""
    # Your logic here
    return row

def standardize_date_formats(row):
    """Standardize date formats."""
    # Your logic here
    return row

def clean_row(row):
    """Clean a given row."""
    row = handle_missing_values(row)
    row = correct_data_types(row)
    row = standardize_date_formats(row)
    # ... Any other cleaning steps
    return row

def process_section_time(time_str):
    """
    Processes a section time string.

    If the string ends with an asterisk, it returns the number without the asterisk and False.
    Otherwise, it returns the number and True.
    """
    is_valid = not time_str.endswith('*')

    if is_valid:
        valid_lap = 1
    else:
        valid_lap = 0

    cleaned_time = time_str.rstrip('*')  # Remove asterisk from the end, if present
    print("process_section_time")

    return cleaned_time, valid_lap

def convert_lap_time(time_str):
    print(f"Converting lap time: {time_str}")
    time, valid_lap = process_section_time(time_str)

    if "'" in time:
        minutes, seconds = time.split("'")
        converted_time = f"00:{minutes.zfill(2)}:{seconds}"
        print(f"Converted {time_str} to {converted_time}")
        return converted_time, valid_lap
    else:
        # Return the string as it is if it doesn't have the ' character.
        return time, valid_lap
