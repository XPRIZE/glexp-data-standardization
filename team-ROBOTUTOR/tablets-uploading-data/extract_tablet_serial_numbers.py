# Extracts tablet serial numbers from log files collected from tablets, and stores them in a standardized format.
#
# Example usage:
#     python3 extract_tablet_serial_numbers.py ../tablet-usage-data/2019-03-01
#
# The extracted data will be stored in a file named `tablets-uploading-data-ROBOTUTOR_<DATE>.csv`.

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
            village_ids = [114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141]
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

                # Expect the following directory structure:
                #  - "2019-03-01/117/REMOTE/CRASH_20000102_034223_5B13002439_release_2.7.7.1.txt"
                #  - "2019-03-01/117/REMOTE/PERF_RoboTutor_release_sw_2.7.7.1_000000_2019.01.15.17.06.48_6116002649.json"
                #  - "2019-03-01/117/REMOTE/RoboTutor_release_sw_2.7.7.1_000000_2019.01.15.17.06.48_6116002649.json"

                # Skip if the current item is a directory
                if os.path.isdir(file_path):
                    # warnings.warn("os.path.isdir(file_path): {}".format(file_path))
                    continue

                # Get the filename, e.g. "CRASH_20000102_034223_5B13002439_release_2.7.7.1.txt"
                basename = ntpath.basename(file_path)
                print(os.path.basename(__file__), "basename: \"{}\"".format(basename))

                tablet_serial = ""
                if basename.startswith("CRASH_"):
                    # E.g. "CRASH_20000102_034223_5B13002439_release_2.7.7.1.txt"
                    tablet_serial = basename[22:32]
                elif basename.endswith(".json"):
                    # E.g. "PERF_RoboTutor_release_sw_2.7.7.1_000000_2019.01.15.17.06.48_6116002649.json" or
                    # "RoboTutor_release_sw_2.7.7.1_000000_2019.01.15.17.06.48_6116002649.json"
                    tablet_serial = basename[len(tablet_serial)-15:len(tablet_serial)-5]
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

            csv_row = ['ROBOTUTOR', village_id, date, len(tablet_serials), tablet_serials]
            print("Adding CSV row: {}".format(csv_row))
            csv_rows.append(csv_row)

        # Define columns
        csv_fieldnames = ['team', 'village_id', 'week_end_date', 'tablet_serials_count', 'tablet_serials']

        # Sort rows by village_id (2nd column)
        csv_rows = sorted(csv_rows, key=lambda x: x[1])

        # Export to a CSV file
        csv_filename = "tablets-uploading-data-ROBOTUTOR_" + date + ".csv"
        print("Writing tablet serials to the file \"" + csv_filename + "\"")
        with open(csv_filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, csv_fieldnames)
            csv_writer.writerow(csv_fieldnames)
            csv_writer.writerows(csv_rows)


if __name__ == "__main__":
    # Only run when not called via "import" in another file

    print(os.path.basename(__file__), "sys.version: {}".format(sys.version))

    # Expect an argument representing a directory containing one week of data, e.g. "../tablet-usage-data/2019-03-01"
    if len(sys.argv) < 2:
        # Abort execution
        exit("Directory argument missing. Example usage: python3 extract_tablet_serial_numbers.py ../tablet-usage-data/2019-03-01")
    dir_containing_weekly_data = sys.argv[1]
    print(os.path.basename(__file__), "dir_containing_weekly_data: \"{}\"".format(dir_containing_weekly_data))

    extract_from_week(dir_containing_weekly_data)
