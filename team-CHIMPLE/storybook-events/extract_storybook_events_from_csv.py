# Extracts storybook events from log files collected from tablets, and stores them in a standardized format.
#
# Example usage:
#     cd storybook-events
#     python3 extract_storybook_events_from_csv.py ../tablet-usage-data/2019-02-08
#
# The extracted data will be stored in a file named `storybook-events-CHIMPLE_<DATE>.csv`.

import sys
import datetime
import arrow
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
            village_ids = [29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 142]
            village_id = int(village_id_dir_entry.name)
            print(os.path.basename(__file__), "village_id: {}".format(village_id))
            if village_id not in village_ids:
                warnings.warn("village_id not in village_ids: {}".format(village_id))
                continue

            # Iterate all subdirectories and files contained within the village ID directory
            print(os.path.basename(__file__), "Iterating all subdirectories and files for village_id " + str(village_id) + ": \"{}/**/*\"".format(village_id_dir_entry.path))
            for file_path in glob.iglob(village_id_dir_entry.path + "/**/*", recursive=True):
                print(os.path.basename(__file__), "\n")
                print(os.path.basename(__file__), "file_path: {}".format(file_path))

                # Expect the following directory structure:
                #  - "2019-02-08/29/REMOTE/5A27001390/"
                #  - "2019-02-08/29/REMOTE/5A27001390/userlog.1548797314282.csv"
                #  - "2019-02-08/29/REMOTE/6116001424/crash.1545399083405.report"

                # Skip if the current file's name does not start with "userlog." (e.g. "userlog.1548797314282.csv")
                basename = ntpath.basename(file_path)
                print(os.path.basename(__file__), "basename: \"{}\"".format(basename))
                if not basename.startswith("userlog."):
                    warnings.warn("Skipping file: \"{}\"".format(basename))
                    continue

                # Extract tablet serial number from the parent folder's name
                # E.g. "2019-02-08/29/REMOTE/5A27001390/userlog.1548797314282.csv"
                #  --> "2019-02-08/29/REMOTE/5A27001390"
                #  --> "5A27001390"
                tablet_serial = file_path.replace("/" + basename, "")
                tablet_serial = tablet_serial[len(tablet_serial)-10:len(tablet_serial)]
                print(os.path.basename(__file__), "tablet_serial: \"{}\"".format(tablet_serial))

                # Skip if the parent folder's name does not represent a valid tablet serial number (on the format "5A27001390")
                is_valid_tablet_serial_number = serial_number_util.is_valid(tablet_serial)
                print(os.path.basename(__file__), "is_valid_tablet_serial_number: {}".format(is_valid_tablet_serial_number))
                if not is_valid_tablet_serial_number:
                    # Invalid serial number. Skip file.
                    warnings.warn("The parent folder does not represent a 10-character serial number: \"{}\"".format(tablet_serial))
                    continue

                # Extract storybook events from CSV
                with open(file_path) as csv_file:
                    csv_data = csv.reader(csv_file)

                    # Skip if corrupt file content
                    try:
                        for storybook_event_row in csv_data:
                            # Only check if the first line in the file is valid, and then skip iteration of the rest of the file.
                            break
                    except csv.Error:
                        # Handle "_csv.Error: line contains NULL byte"
                        # Example: 2018-09-07/35/REMOTE/6111001892/userlog.1532930088249.csv contains "^@^@^@^@^@^@^@^@^@^@"
                        warnings.warn("Skipping file which contains NULL byte")
                        continue

                    for storybook_event_row in csv_data:
                        print(os.path.basename(__file__), "storybook_event_row: {}".format(storybook_event_row))

                        # CSV columns: entityType,entityId,event,loggedAt,name

                        # In some cases a log file has not been written completely, causing incomplete rows of data.
                        # Example from 2019-02-08/42/remote/5B13002216/userlog.1549046021217.csv:
                        #  - "4","-1","3","Thu Jan 31 18:57:24 GMT+03:00 2019","Bali"
                        #  - "4","-1"
                        if len(storybook_event_row) < 5:
                            # Skip row if it is incomplete, to avoid "IndexError: list index out of range"
                            warnings.warn("Skipping incomplete row: \"{}\"".format(storybook_event_row))
                            continue

                        # Look for userlog rows where the "name"column begins with the name "storyId_" (e.g. "storyId_63")
                        # Row example: "4","1","2","Tue Jan 22 01:43:25 GMT+03:00 2019","storyId_63"
                        # See https://github.com/XPRIZE/GLEXP-Team-Chimple-goa/blob/master/Bali/app/src/main/java/org/chimple/bali/db/entity/UserLog.java#L80
                        userlog_name = storybook_event_row[4]
                        print(os.path.basename(__file__), "userlog_name: {}".format(userlog_name))
                        if userlog_name.startswith("storyId_"):
                            # Extract storybook ID from the "name" column
                            storybook_id = userlog_name[8:len(userlog_name)]
                            print(os.path.basename(__file__), "storybook_id: {}".format(storybook_id))

                            # Extract storybook start time from the "loggedAt" column
                            userlog_logged_at = storybook_event_row[3]
                            # Handle different types of timezones (see https://arrow.readthedocs.io/en/latest/#supported-tokens)
                            if len(userlog_logged_at) == 34:
                                # E.g. "Tue Jan 22 01:43:25 GMT+03:00 2019"
                                storybook_start_time = arrow.get(userlog_logged_at, "ddd MMM DD HH:mm:ss ZZZZZ YYYY").timestamp
                            elif len(userlog_logged_at) == 28:
                                # E.g. "Wed Jan 09 09:55:25 PST 2019"
                                try:
                                    storybook_start_time = arrow.get(userlog_logged_at, "ddd MMM DD HH:mm:ss ZZZ YYYY").timestamp
                                except arrow.parser.ParserError:
                                    # Handle "arrow.parser.ParserError: Could not parse timezone expression "AST"".
                                    # E.g. "Sun Jan 09 21:51:30 AST 2000" or "Thu Aug 29 15:25:29 EDT 2019"
                                    warnings.warn("Skipping invalid timezone expression")
                                    continue
                            print(os.path.basename(__file__), "storybook_start_time: {}".format(storybook_start_time))

                            # Storybook end time is not stored, so set to None
                            storybook_end_time = None

                            csv_row = [tablet_serial, storybook_id, storybook_start_time, storybook_end_time]
                            if csv_row not in csv_rows:
                                print(os.path.basename(__file__), "Adding CSV row: {}".format(csv_row))
                                csv_rows.append(csv_row)

        # Define columns
        csv_fieldnames = ['tablet_serial', 'storybook_id', 'start_time', 'end_time']

        # Sort rows by tablet_serial (1st column), start_time (3rd column)
        csv_rows = sorted(csv_rows, key=lambda x: (x[0], x[2]))

        # Export to a CSV file
        csv_filename = "storybook-events-CHIMPLE_" + date + ".csv"
        print("Writing storybook events to the file \"" + csv_filename + "\"")
        with open(csv_filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, csv_fieldnames)
            csv_writer.writerow(csv_fieldnames)
            csv_writer.writerows(csv_rows)


if __name__ == "__main__":
    # Only run when not called via "import" in another file

    print(os.path.basename(__file__), "sys.version: {}".format(sys.version))

    # Expect an argument representing a directory containing one week of data, e.g. "../tablet-usage-data/2019-02-08"
    if len(sys.argv) < 2:
        # Abort execution
        exit("Directory argument missing. Example usage: python3 extract_storybook_events_from_csv.py ../tablet-usage-data/2019-02-08")
    dir_containing_weekly_data = sys.argv[1]
    print(os.path.basename(__file__), "dir_containing_weekly_data: \"{}\"".format(dir_containing_weekly_data))

    extract_from_week(dir_containing_weekly_data)
