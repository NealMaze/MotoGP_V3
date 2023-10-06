# imports
import time
import winsound

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
    # print("process_section_time")

    return cleaned_time, valid_lap

def convert_lap_time(time_str):
    # print(f"Converting lap time: {time_str}")
    time, valid_lap = process_section_time(time_str)

    if "'" in time:
        minutes, seconds = time.split("'")
        converted_time = f"00:{minutes.zfill(2)}:{seconds}"
        # print(f"Converted {time_str} to {converted_time}")
        return converted_time, valid_lap
    else:
        # Return the string as it is if it doesn't have the ' character.
        return time, valid_lap

def play_mario():
    E5 = 659
    E6 = 1319
    G5 = 783
    A5 = 880
    A4 = 440
    D5 = 587
    B4 = 493
    C5 = 523
    B5 = 988
    G4 = 392
    F5 = 698  # Added definition for F5

    # Increase the duration values to slow down the theme
    factor = 2.0  # Adjust this factor to make it slower or faster

    winsound.Beep(E5, int(100*factor))
    winsound.Beep(E5, int(100*factor))
    winsound.Beep(E5, int(100*factor))
    winsound.Beep(C5, int(100*factor))
    winsound.Beep(E5, int(100*factor))
    winsound.Beep(G5, int(200*factor))
    winsound.Beep(G4, int(200*factor))
    winsound.Beep(C5, int(200*factor))
    winsound.Beep(G4, int(200*factor))
    winsound.Beep(E5, int(300*factor))
    winsound.Beep(A4, int(300*factor))
    winsound.Beep(B4, int(100*factor))
    winsound.Beep(A5, int(200*factor))
    winsound.Beep(A4, int(100*factor))
    winsound.Beep(D5, int(200*factor))
    winsound.Beep(D5, int(200*factor))
    winsound.Beep(E5, int(100*factor))
    winsound.Beep(G4, int(300*factor))
    winsound.Beep(A4, int(100*factor))
    winsound.Beep(A5, int(200*factor))
    winsound.Beep(A4, int(100*factor))
    winsound.Beep(E5, int(200*factor))
    winsound.Beep(G5, int(100*factor))
    winsound.Beep(A5, int(100*factor))
    winsound.Beep(F5, int(100*factor))
    winsound.Beep(G5, int(100*factor))
    winsound.Beep(E5, int(100*factor))
    winsound.Beep(C5, int(100*factor))
    winsound.Beep(D5, int(100*factor))
    winsound.Beep(B4, int(200*factor))

def play_beethoven_5th():
    G4 = 392
    Eb4 = 311
    F4 = 349

    short_pause = 200
    long_pause = 400
    extra_long_pause = 600

    # Initial iconic four notes
    for _ in range(2):
        winsound.Beep(G4, short_pause)
        winsound.Beep(Eb4, short_pause)
        winsound.Beep(F4, long_pause)
        time.sleep(0.2)

    # Variation of the theme
    for _ in range(2):
        winsound.Beep(G4, short_pause)
        winsound.Beep(Eb4, short_pause)
        winsound.Beep(F4, short_pause)
        winsound.Beep(G4, long_pause)
        time.sleep(0.2)

    # Return to the iconic theme
    for _ in range(2):
        winsound.Beep(G4, short_pause)
        winsound.Beep(Eb4, short_pause)
        winsound.Beep(F4, long_pause)
        time.sleep(0.2)

    # Another variation
    for _ in range(2):
        winsound.Beep(G4, short_pause)
        winsound.Beep(Eb4, short_pause)
        winsound.Beep(G4, short_pause)
        winsound.Beep(F4, extra_long_pause)

def play_ode_to_joy():
    E5 = 659
    D5 = 587
    C5 = 523
    B4 = 493
    A4 = 440
    G4 = 392
    F4 = 349

    melody = [
        E5, E5, F4, G4, G4, F4, E5, D5, C5, C5, D5, E5, E5, D5, D5,
        E5, E5, F4, G4, G4, F4, E5, D5, C5, C5, D5, E5, D5, C5, C5,
        D5, D5, E5, C5, D5, E5, F4, E5, C5, D5, E5, F4, E5, D5, C5,
        D5, E5, F4, G4, F4, E5, D5, C5, E5, D5, C5, D5, E5, F4, E5, C5
    ]

    durations = [
        400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 800, 400, 800,
        400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 800, 400, 800,
        400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 800, 400, 800,
        400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 400, 800, 400, 800, 800
    ]

    for note, duration in zip(melody, durations):
        winsound.Beep(note, duration)
        time.sleep(50/1000)
