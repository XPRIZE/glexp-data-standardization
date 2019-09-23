# Extracts tablet serial numbers from log files collected from tablets, and stores them in a standardized format.
#
# Example usage:
#     python3 extract_tablet_serial_numbers.py ../tablet-usage-data/2018-05-25
#
# The extracted data will be stored in a file named `tablets-uploading-data-KITKIT_<DATE>.csv`.

import sys
import datetime
import os
import warnings
import glob
import ntpath
import csv

import serial_number_util


def verify_date(date_text):
    print(os.path.basename(__file__), "verify_date")
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format. Should be YYYY-mm-dd")


def extract_from_week(directory_containing_weekly_data):
    print(os.path.basename(__file__), "extract_from_week")

    # Extract the date (the last 10 characters) from the directory path
    date = directory_containing_weekly_data[len(directory_containing_weekly_data) - 10:len(directory_containing_weekly_data)]
    print(os.path.basename(__file__), "date: \"{}\"".format(date))

    # Verify that the directory name is on the format "YYYY-mm-dd"
    verify_date(date)

    # Iterate each subdirectory
    date_directory_iterator = os.scandir(directory_containing_weekly_data)
    print(os.path.basename(__file__), "date_directory_iterator: {}".format(date_directory_iterator))
    with date_directory_iterator as village_id_dir_entries:
        csv_rows = []

        for village_id_dir_entry in village_id_dir_entries:
            print(os.path.basename(__file__), "village_id_dir_entry: {}".format(village_id_dir_entry))

            # Skip if the current DirEntry is not a directory
            if not village_id_dir_entry.is_dir():
                warnings.warn("not village_id_dir_entry.is_dir(): {}".format(village_id_dir_entry))
                continue

            # Skip if the current Village ID is not valid
            village_ids = [57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85]
            village_id = int(village_id_dir_entry.name)
            print(os.path.basename(__file__), "village_id: {}".format(village_id))
            if village_id not in village_ids:
                warnings.warn("village_id not in village_ids: {}".format(village_id))
                continue

            # Add each extracted tablet serial number to an array
            tablet_serials = []

            # Iterate all subdirectories and files contained within the village ID directory
            print(os.path.basename(__file__), "Iterating all subdirectories and files for village_id " + str(village_id) + ": \"{}/**/*\"".format(village_id_dir_entry.path))
            for file_path in glob.iglob(village_id_dir_entry.path + "/**/*", recursive=True):
                print(os.path.basename(__file__), "file_path: {}".format(file_path))

                # Expect the following file structure:
                #  - "2017-12-22/57/remote/5A27001661_log_1.txt"
                #  - "2018-05-25/57/REMOTE/com_enuma_booktest.6115000540.lastlog.txt"
                #  - "2018-05-25/57/REMOTE/com_enuma_xprize.5A23001564.lastlog.txt"
                #  - "2018-05-25/57/REMOTE/com_enuma_xprize.6116002162.A.log.zip"
                #  - "2018-05-25/57/REMOTE/com_enuma_xprize.6116002162.B1.log.zip"
                #  - "2018-05-25/57/REMOTE/library_todoschool_enuma_com_todoschoollibrary.6111001905.lastlog.txt"
                #  - "2018-05-25/57/REMOTE/todoschoollauncher_enuma_com_todoschoollauncher.5A23001564.lastlog.txt"
                #  - "2018-05-25/59/REMOTE/6114000050_user0.zip"
                #  - "2018-06-22/84/remote/5A28000934_.aux.zip"
                #  - "2018-11-09/83/REMOTE/crashlog.com_enuma_todoschoollockscreen.txt
                #  - "2018-11-09/83/REMOTE/crashlog.library_todoschool_enuma_com_todoschoollibrary.txt

                # Skip if the current item is a directory
                if os.path.isdir(file_path):
                    # warnings.warn("os.path.isdir(file_path): {}".format(file_path))
                    continue

                # Get the filename, e.g. "com_enuma_booktest.6115000540.lastlog.txt"
                basename = ntpath.basename(file_path)
                print(os.path.basename(__file__), "basename: \"{}\"".format(basename))

                # Extract the tablet serial number from the filename
                tablet_serial = ""
                if ("_log_" in basename) and basename.endswith(".txt"):
                    # E.g. "5A27001661_log_1.txt"
                    tablet_serial = basename[0:10]
                elif basename.startswith("com_enuma_booktest."):
                    # E.g. "com_enuma_booktest.6115000540.lastlog.txt"
                    tablet_serial = basename[19:29]
                elif basename.startswith("com_enuma_xprize."):
                    # E.g. "com_enuma_xprize.5A23001564.lastlog.txt" or "com_enuma_xprize.6116002162.A.log.zip"
                    tablet_serial = basename[17:27]
                elif basename.startswith("library_todoschool_enuma_com_todoschoollibrary."):
                    # E.g. "library_todoschool_enuma_com_todoschoollibrary.6111001905.lastlog.txt"
                    tablet_serial = basename[47:57]
                elif basename.startswith("todoschoollauncher_enuma_com_todoschoollauncher."):
                    # E.g. "todoschoollauncher_enuma_com_todoschoollauncher.5A23001564.lastlog.txt"
                    tablet_serial = basename[48:58]
                elif ("_user" in basename) and basename.endswith(".zip"):
                    # E.g. "6114000050_user0.zip"
                    tablet_serial = basename[0:10]
                elif basename.endswith("_.aux.zip"):
                    # E.g. "5A28000934_.aux.zip"
                    tablet_serial = basename[0:10]
                elif basename.startswith("crashlog."):
                    # E.g. "crashlog.com_enuma_todoschoollockscreen.txt" or "crashlog.library_todoschool_enuma_com_todoschoollibrary.txt"
                    # Skip, since the filename does not contain a tablet serial number.
                    continue
                print(os.path.basename(__file__), "tablet_serial: \"{}\"".format(tablet_serial))

                # Skip if the current filename does not contain a valid tablet serial number
                is_valid_tablet_serial_number = serial_number_util.is_valid(tablet_serial)
                print(os.path.basename(__file__), "is_valid_tablet_serial_number: {}".format(is_valid_tablet_serial_number))
                if not is_valid_tablet_serial_number:
                    raise ValueError("Invalid tablet_serial: \"{}\"".format(tablet_serial))
                elif tablet_serial not in tablet_serials:
                    tablet_serials.append(tablet_serial)

            # Sort tablet_serials by value (ascending)
            tablet_serials = sorted(tablet_serials)

            csv_row = ['KITKIT', village_id, date, len(tablet_serials), tablet_serials]
            print("Adding CSV row: {}".format(csv_row))
            csv_rows.append(csv_row)

        # Define columns
        csv_fieldnames = ['team', 'village_id', 'week_end_date', 'tablet_serials_count', 'tablet_serials']

        # Sort rows by village_id (2nd column)
        csv_rows = sorted(csv_rows, key=lambda x: x[1])

        # Export to a CSV file
        csv_filename = "tablets-uploading-data-KITKIT_" + date + ".csv"
        print("Writing tablet serials to the file \"" + csv_filename + "\"")
        with open(csv_filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, csv_fieldnames)
            csv_writer.writerow(csv_fieldnames)
            csv_writer.writerows(csv_rows)


if __name__ == "__main__":
    # Only run when not called via "import" in another file

    print(os.path.basename(__file__), "sys.version: {}".format(sys.version))

    # Expect an argument representing a directory containing one week of data, e.g. "../tablet-usage-data/2018-05-25"
    if len(sys.argv) < 2:
        # Abort execution
        exit("Directory argument missing. Example usage: python3 extract_tablet_serial_numbers.py ../tablet-usage-data/2018-05-25")
    dir_containing_weekly_data = sys.argv[1]
    print(os.path.basename(__file__), "dir_containing_weekly_data: \"{}\"".format(dir_containing_weekly_data))

    extract_from_week(dir_containing_weekly_data)
