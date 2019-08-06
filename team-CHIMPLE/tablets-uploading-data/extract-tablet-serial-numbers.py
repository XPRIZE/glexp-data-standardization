# Extracts tablet serial numbers from log files collected from tablets, and stores them in a standardized format.
#
# Example usage:
#     python3 extract-tablet-serial-numbers.py ../tablet-usage-data/2019-02-08
#
# The extracted data will be stored in a file named `tablets-uploading-data-CHIMPLE_<DATE>.csv`.

import sys
import datetime
import os
import warnings
import glob
import ntpath

import serial_number_util


def verify_date(date_text):
    print(os.path.basename(__file__), "verify_date")
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format. Should be YYYY-mm-dd")


print(os.path.basename(__file__), "sys.version: {}".format(sys.version))

# Expect an argument representing a directory containing one week of data, e.g. "../tablet-usage-data/2019-02-18"
if len(sys.argv) < 2:
    # Abort execution
    exit("Example usage: python3 extract-tablet-serial-numbers.py ../tablet-usage-data/2019-02-08")
directory_containing_weekly_data = sys.argv[1]
print(os.path.basename(__file__), "directory_containing_weekly_data: \"{}\"".format(directory_containing_weekly_data))

# Extract the date (the last 10 characters) from the directory path
date = directory_containing_weekly_data[len(directory_containing_weekly_data) - 10:len(directory_containing_weekly_data)]
print(os.path.basename(__file__), "date: \"{}\"".format(date))

# Verify that the directory name is on the format "YYYY-mm-dd"
verify_date(date)

# Iterate each subdirectory
date_directory_iterator = os.scandir(directory_containing_weekly_data)
print(os.path.basename(__file__), "date_directory_iterator: {}".format(date_directory_iterator))
with date_directory_iterator as village_id_dir_entries:
    for village_id_dir_entry in village_id_dir_entries:
        print(os.path.basename(__file__), "village_id_dir_entry: {}".format(village_id_dir_entry))

        # Skip if the current DirEntry is not a directory
        if not village_id_dir_entry.is_dir():
            warnings.warn("not village_id_dir_entry.is_dir(): {}".format(village_id_dir_entry))
            continue

        # Skip if the current Village ID is not valid
        village_ids = [29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 142]
        village_id = int(village_id_dir_entry.name)
        print(os.path.basename(__file__), "village_id: {}".format(village_id))
        if village_id not in village_ids:
            warnings.warn("village_id not in village_ids: {}".format(village_id))
            continue

        # Iterate all subdirectories and files contained within the village ID directory
        print(os.path.basename(__file__), "Iterating all subdirectories and files for village_id " + str(village_id) + ": \"{}/**/*\"".format(village_id_dir_entry.path))
        for file_path in glob.iglob(village_id_dir_entry.path + "/**/*", recursive=True):
            print(os.path.basename(__file__), "file_path: {}".format(file_path))

            # Expect the following directory structure: "2019-02-08/29/REMOTE/5A27001390"

            # Skip if the current item is not a directory
            if not os.path.isdir(file_path):
                # warnings.warn("not os.path.isdir(file_path): {}".format(file_path))
                continue

            # Skip if the current directory is not a valid tablet serial number (on the format "5A27001390")
            basename = ntpath.basename(file_path)
            print(os.path.basename(__file__), "basename: \"{}\"".format(basename))
            is_valid_tablet_serial_number = serial_number_util.is_valid(basename)
            print(os.path.basename(__file__), "is_valid_tablet_serial_number: {}".format(is_valid_tablet_serial_number))
            if not is_valid_tablet_serial_number:
                continue

            # Export to CSV
            # TODO
