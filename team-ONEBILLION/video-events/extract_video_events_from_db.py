# Extracts video events from database, and stores them in a standardized format.
#
# Example usage:
#     cd video-events
#     python3 extract_video_events_from_db.py ../tablet-usage-data/2019-03-01
#
# The extracted data will be stored in a file named `video-events-ONEBILLION_<DATE>.csv`.

import sys
import datetime
import os
import warnings
import glob
import ntpath
import csv
import sqlite3

import serial_number_util


def verify_date(date_text):
    print(os.path.basename(__file__), "verify_date")
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format. Should be YYYY-mm-dd")


# Prepares a set of key:value pairs for the MAC addresses listed in tablet-mac-to-serial-mappings.csv. This will make it
# possible to map a MAC address (used in filenames before March 2018) to its corresponding tablet serial number.
tablet_mac_to_serial_mappings = {}
def initialize_tablet_mac_to_serial_mappings():
    print(os.path.basename(__file__), "initialize_tablet_mac_to_serial_mappings")

    if len(tablet_mac_to_serial_mappings) > 0:
        # The key set has already been initialized
        return tablet_mac_to_serial_mappings

    with open("../tablet-tracker/tablet-mac-to-serial-mappings.csv") as csv_file:
        csv_data = csv.reader(csv_file)
        csv_data_row_count = 0
        for csv_data_row in csv_data:
            csv_data_row_count += 1
            if csv_data_row_count == 1:
                # Skip header row
                continue
            print(os.path.basename(__file__), "csv_data_row: {}".format(csv_data_row))

            mac_address = csv_data_row[0]
            serial_number = csv_data_row[1]

            try:
                existing_key = tablet_mac_to_serial_mappings[mac_address]
                # The MAC address has already been added as a key.
                raise ValueError("MAC address has already been added: \"{}\". Skipping.".format(mac_address))
            except KeyError:
                # The MAC address has not yet been added as a key. Add it.
                print(os.path.basename(__file__), "Adding MAC address: \"{}\"".format(mac_address))
                tablet_mac_to_serial_mappings[mac_address] = serial_number

    print(os.path.basename(__file__), "tablet_mac_to_serial_mappings: {}".format(tablet_mac_to_serial_mappings))
    return tablet_mac_to_serial_mappings


def extract_from_week(directory_containing_weekly_data):
    print(os.path.basename(__file__), "extract_from_week")

    initialize_tablet_mac_to_serial_mappings()

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
            village_ids = [86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113]
            village_id = int(village_id_dir_entry.name)
            print(os.path.basename(__file__), "village_id: {}".format(village_id))
            if village_id not in village_ids:
                warnings.warn("village_id not in village_ids: {}".format(village_id))
                continue

            # Iterate all subdirectories and files contained within the village ID directory
            print(os.path.basename(__file__), "Iterating all subdirectories and files for village_id " + str(village_id) + ": \"{}/**/*\"".format(village_id_dir_entry.path))
            for file_path in glob.iglob(village_id_dir_entry.path + "/**/*", recursive=True):
                print(os.path.basename(__file__), "file_path: {}".format(file_path))

                # Expect the following directory structure:
                #  - "2017-12-29/105/REMOTE/80a589fd41_2017_12_24_12_23_29.db" (MAC address, 10 characters)
                #  - "2018-03-09/86/REMOTE/80a5896b547_2018_02_28_10_25_09.db" (MAC address, 11 characters)
                #  - "2018-03-09/99/REMOTE/80a589ae9551_2018_03_05_09_46_10.db" (MAC address, 12 characters)
                #  - "2018-03-23/86/REMOTE/5A29000653_2018_03_19_07_12_18.db" (serial number)
                #  - "2019-03-01/96/REMOTE/5B12002485_2019_02_23_12_20_22.db" (serial number)

                # Skip if the current item is a directory
                if os.path.isdir(file_path):
                    # warnings.warn("os.path.isdir(file_path): {}".format(file_path))
                    continue

                # Get the filename, e.g. "80a5895313a5_2018_02_19_06_48_38.db.db" or "5A29000653_2018_03_19_07_12_18.db"
                basename = ntpath.basename(file_path)
                print(os.path.basename(__file__), "\n")
                print(os.path.basename(__file__), "basename: \"{}\"".format(basename))

                # Up until March 2018, filenames were generated using tablet MAC addresses instead of tablet serial numbers.
                #  - Example MAC address:   "80a5895313a5_2018_02_19_06_48_38.db"
                #  - Example serial number: "5A29000653_2018_03_19_07_12_18.db"
                mac_address_or_serial_number = basename[0:len(basename) - 23]
                if not serial_number_util.is_valid(mac_address_or_serial_number):
                    # MAC address in filename

                    # Extract the MAC address from the filename (e.g. "80a5895313a5_2018_02_19_06_48_38.db" --> "80a5895313a5").
                    # Note that some of the filenames contained MAC addresses consisting of only 11 or 10 characters (e.g.
                    # "80a5896b547_2018_02_28_10_25_09.db" or "80a589fd41_2017_12_24_12_23_29.db"), so don't assume a
                    # length of 12 characters!
                    mac_address = basename[0:len(basename) - 23]
                    print(os.path.basename(__file__), "mac_address: \"{}\"".format(mac_address))

                    # Get the corresponding serial number
                    try:
                        tablet_serial = tablet_mac_to_serial_mappings[mac_address]
                    except KeyError:
                        # No match
                        tablet_serial = ""
                    print(os.path.basename(__file__), "tablet_serial looked up from tablet_mac_to_serial_mappings: \"{}\"".format(tablet_serial))
                else:
                    # Serial number in filename

                    # Extract the tablet serial number from the filename (e.g. "5A29000653_2018_03_19_07_12_18.db")
                    tablet_serial = basename[0:10]
                    print(os.path.basename(__file__), "tablet_serial: \"{}\"".format(tablet_serial))

                    # Skip if the current filename does not contain a valid tablet serial number (on the format "5B12002485_2019_02_23_12_20_22.db")
                    is_valid_tablet_serial_number = serial_number_util.is_valid(tablet_serial)
                    print(os.path.basename(__file__), "is_valid_tablet_serial_number: {}".format(is_valid_tablet_serial_number))
                    if not is_valid_tablet_serial_number:
                        raise ValueError("Invalid tablet_serial: \"{}\"".format(tablet_serial))

                # Connect to the database
                connection = sqlite3.connect(file_path)
                cursor = connection.cursor()

                # Extract instances of a child interacting with a video
                try:
                    cursor.execute("SELECT unitid, startTime, endTime FROM unitinstances WHERE unitid IN (SELECT unitid FROM units WHERE params LIKE \"%video=%\")")
                except sqlite3.DatabaseError as e:
                    # Handle "sqlite3.DatabaseError: database disk image is malformed"
                    warnings.warn("Skipping invalid database file: \"{}\"".format(e))
                    continue
                result = cursor.fetchall()
                print(os.path.basename(__file__), "len(result): {}".format(len(result)))
                for video_event_row in result:
                    print(os.path.basename(__file__), "video_event_row: {}".format(video_event_row))

                    # unitid (integer, e.g. 3888)
                    video_row_unitid = video_event_row[0]
                    # TODO: Introduce usage of one ID per video instead of multiple IDs per video?
                    video_id = video_row_unitid

                    # startTime (integer, e.g. 1535557047)
                    video_row_start_time = video_event_row[1]
                    # TODO: Skip event if incorrect timestamp (from year 2000 due to tablet running out of battery)?
                    video_start_time = video_row_start_time

                    # endTime (integer, e.g. 1535557102)
                    video_row_end_time = video_event_row[2]
                    # TODO: Skip event if incorrect timestamp (from year 2000 due to tablet running out of battery)?
                    video_end_time = video_row_end_time

                    csv_row = [tablet_serial, video_id, video_start_time, video_end_time]
                    if csv_row not in csv_rows:
                        print(os.path.basename(__file__), "Adding CSV row: {}".format(csv_row))
                        csv_rows.append(csv_row)

        # Define columns
        csv_fieldnames = ['tablet_serial', 'video_id', 'start_time', 'end_time']

        # Sort rows by tablet_serial (1st column), start_time (3rd column)
        csv_rows = sorted(csv_rows, key=lambda x: (x[0], x[2]))

        # Export to a CSV file
        csv_filename = "video-events-ONEBILLION_" + date + ".csv"
        print(os.path.basename(__file__), "Writing video events to the file \"" + csv_filename + "\"")
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
        exit("Directory argument missing. Example usage: python3 extract_video_events_from_db.py ../tablet-usage-data/2019-03-01")
    dir_containing_weekly_data = sys.argv[1]
    print(os.path.basename(__file__), "dir_containing_weekly_data: \"{}\"".format(dir_containing_weekly_data))

    extract_from_week(dir_containing_weekly_data)
