# Extracts storybook events from log files collected from tablets, and stores them in a standardized format.
#
# Example usage:
#     cd storybook-events
#     python3 extract_storybook_events_from_txt.py ../tablet-usage-data/2018-05-25
#
# The extracted data will be stored in a file named `storybook-events-KITKIT_<DATE>.csv`.

import sys
import datetime
import os
import warnings
import glob
import ntpath
import csv
import json
import zipfile

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

                # Skip if the current file's name does not start with "library_todoschool_enuma_com_todoschoollibrary.",
                # e.g. "library_todoschool_enuma_com_todoschoollibrary.6129002346.lastlog.txt" or
                # "library_todoschool_enuma_com_todoschoollibrary.6118002503.A.log.zip"
                basename = ntpath.basename(file_path)
                print(os.path.basename(__file__), "basename: \"{}\"".format(basename))
                if not basename.startswith("library_todoschool_enuma_com_todoschoollibrary."):
                    warnings.warn("Skipping file: \"{}\"".format(basename))
                    continue

                # Extract the tablet serial number from the filename
                # E.g. "library_todoschool_enuma_com_todoschoollibrary.6111001905.lastlog.txt" or
                # "library_todoschool_enuma_com_todoschoollibrary.6118002503.A.log.zip"
                tablet_serial = basename[47:57]

                # If ZIP, unzip file. E.g. "library_todoschool_enuma_com_todoschoollibrary.6118002503.A.log.zip".
                # Expect the ZIP file to contain only one file, and with the same name as the ZIP file itself
                # (but with ".log.txt" extension instead of ".log.zip").
                unzipped_file_to_be_deleted = None
                if basename.endswith(".zip"):
                    print(os.path.basename(__file__), "Unzipping: {}".format(file_path))
                    with zipfile.ZipFile(file_path) as zip_ref:
                        # Extract log file temporarily to the storybook-events/ directory
                        zip_ref.extractall()

                        # Update the path of the current file so that it points to the unzipped file instead of the ZIP file
                        file_path = basename
                        file_path = file_path.replace(".log.zip", ".log.txt")
                        unzipped_file_to_be_deleted = file_path

                with open(file_path) as txt_file:
                    for txt_line in txt_file:
                        # Look for lines containing "action":"start_book"
                        if "start_book" in txt_line:
                            print(os.path.basename(__file__), "txt_line: {}".format(txt_line))
                            # Extract storybook event from JSON object
                            # Example: {"appName":"library.todoschool.enuma.com.todoschoollibrary","timeStamp":1483935746,"event":{"category":"library","action":"start_book","label":"sw_216","value":0},"user":"user0"}
                            json_object = json.loads(txt_line)

                            json_object_event = json_object["event"]
                            print(os.path.basename(__file__), "json_object_event: {}".format(json_object_event))

                            # label (e.g. "sw_216")
                            label = json_object_event["label"]
                            storybook_id = label[3:len(label)]

                            # timeStamp (e.g. 1483935746)
                            timestamp = json_object["timeStamp"]
                            storybook_start_time = timestamp

                            # Storybook end time is not stored, so set to None
                            # TODO: can "action":"finish_read" events be linked to "action":"start_book" events for the same book ID?
                            storybook_end_time = None

                            csv_row = [tablet_serial, storybook_id, storybook_start_time, storybook_end_time]
                            if csv_row not in csv_rows:
                                print(os.path.basename(__file__), "Adding CSV row: {}".format(csv_row))
                                csv_rows.append(csv_row)

                    txt_file.close()

                    # If the file was unzipped, delete it.
                    print(os.path.basename(__file__), "unzipped_file_to_be_deleted: {}".format(unzipped_file_to_be_deleted))
                    if unzipped_file_to_be_deleted is not None:
                        # Delete the unzipped file
                        os.remove(unzipped_file_to_be_deleted)

        # Define columns
        csv_fieldnames = ['tablet_serial', 'storybook_id', 'start_time', 'end_time']

        # Sort rows by tablet_serial (1st column), start_time (3rd column)
        csv_rows = sorted(csv_rows, key=lambda x: (x[0], x[2]))

        # Export to a CSV file
        csv_filename = "storybook-events-KITKIT_" + date + ".csv"
        print("Writing storybook events to the file \"" + csv_filename + "\"")
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
        exit("Directory argument missing. Example usage: python3 extract_storybook_events_from_txt.py ../tablet-usage-data/2018-05-25")
    dir_containing_weekly_data = sys.argv[1]
    print(os.path.basename(__file__), "dir_containing_weekly_data: \"{}\"".format(dir_containing_weekly_data))

    extract_from_week(dir_containing_weekly_data)
