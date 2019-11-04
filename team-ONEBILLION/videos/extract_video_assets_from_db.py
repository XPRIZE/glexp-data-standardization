# Extracts video assets from database, and stores them in a standardized format.
#
# Example usage:
#     cd videos
#     python3 extract_video_assets_from_db.py ../tablet-usage-data/2019-03-01
#
# The extracted data will be stored in a file named `videos-ONEBILLION_<DATE>.csv`.

import sys
import datetime
import os
import warnings
import glob
import ntpath
import csv
import sqlite3

import serial_number_util

from Video import Video


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

                # Expect the following directory structure: "2019-03-01/96/REMOTE/5B12002485_2019_02_23_12_20_22.db"

                # Skip if the current item is a directory
                if os.path.isdir(file_path):
                    # warnings.warn("os.path.isdir(file_path): {}".format(file_path))
                    continue

                # Get the filename, e.g. "5B12002485_2019_02_23_12_20_22.db"
                basename = ntpath.basename(file_path)
                print(os.path.basename(__file__), "\n")
                print(os.path.basename(__file__), "basename: \"{}\"".format(basename))

                # Extract the tablet serial number from the filename
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

                # Extract the video's id and title
                try:
                    cursor.execute("SELECT unitid, config, params FROM units WHERE params LIKE \"%video=%\"")
                except sqlite3.DatabaseError as e:
                    # Handle "sqlite3.DatabaseError: database disk image is malformed"
                    warnings.warn("Skipping invalid database file: \"{}\"".format(e))
                    continue
                result = cursor.fetchall()
                print(os.path.basename(__file__), "len(result): {}".format(len(result)))
                for video_row in result:
                    print(os.path.basename(__file__), "video_row: {}".format(video_row))

                    video = Video()

                    # unitid (integer, e.g. 1)
                    video_row_unitid = video_row[0]
                    video.id = video_row_unitid

                    # params (text, e.g. "vps/video=motivational_song_sw")
                    video_row_params = video_row[2]
                    video.title = video_row_params[10:len(video_row_params)]

                    # There are multiple directories containing videos:
                    #  - onecourse-assets-sw-v3.0.1.tar.gz:assets/oc-video/img/movies/*.mp4
                    #  - onecourse-assets-sw-v3.0.1.tar.gz:assets/oc-videos-gen/local/sw/*.mp4
                    # The database file does not contain information about which of these directories a video belongs to,
                    # so we'll use an asterisk instead.
                    video.asset_path = "onecourse-assets-sw-v3.0.1.tar.gz:assets/oc-video*"

                    csv_row = [video.id, video.title, video.asset_path]
                    if csv_row not in csv_rows:
                        print(os.path.basename(__file__), "Adding CSV row: {}".format(csv_row))
                        csv_rows.append(csv_row)

        # Define columns
        csv_fieldnames = ['id', 'title', 'asset_path']

        # Sort rows by id (1st column)
        csv_rows = sorted(csv_rows, key=lambda x: x[0])

        # Export to a CSV file
        csv_filename = "videos-ONEBILLION_" + date + ".csv"
        print(os.path.basename(__file__), "Writing videos to the file \"" + csv_filename + "\"")
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
        exit("Directory argument missing. Example usage: python3 extract_video_assets_from_db.py ../tablet-usage-data/2019-03-01")
    dir_containing_weekly_data = sys.argv[1]
    print(os.path.basename(__file__), "dir_containing_weekly_data: \"{}\"".format(dir_containing_weekly_data))

    extract_from_week(dir_containing_weekly_data)
