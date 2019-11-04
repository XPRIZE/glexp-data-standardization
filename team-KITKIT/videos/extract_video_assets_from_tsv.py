# Extracts videos from library_video_data.tsv (copied from library_sw_tz.tar.gz), and stores them in a standardized format.
#
# Example usage:
#     cd videos
#     python3 extract_video_assets_from_tsv.py library_video_data.tsv
#
# The extracted data will be stored in a file named `videos-KITKIT.csv`.

import csv
import os
import sys
import warnings

from Video import Video


def extract_from_tsv(file_containing_videos):
    print(os.path.basename(__file__), "extract_from_tsv")

    csv_rows = []

    with open(file_containing_videos) as tsv_file:
        tsv_data = csv.reader(tsv_file, dialect="excel-tab")
        row_counter = 0
        video_id = 1
        for video_row in tsv_data:
            row_counter += 1

            if row_counter <= 7:
                # Skip the first 7 rows of the spreadsheets since they contain comments instead of data values.
                warnings.warn("Skipping row: {}".format(video_row))
                continue

            print(os.path.basename(__file__), "video_row: {}".format(video_row))

            # TSV columns: id,category,categoryname,title,thumbnail,filename

            video = Video()

            # id
            video.id = video_id

            # category
            # TODO

            # title
            video.title = video_row[3]

            # filename
            file_name = video_row[5]
            video.asset_path = "library_sw_tz.tar.gz:library_sw_tz/localized/sw-tz/res/raw/" + file_name

            csv_row = [video.id, video.title, video.asset_path]
            print("Adding CSV row: {}".format(csv_row))
            csv_rows.append(csv_row)

            video_id += 1

        # Define columns
        csv_fieldnames = ['id', 'title', 'asset_path']

        # Sort rows by id (1st column)
        csv_rows = sorted(csv_rows, key=lambda x: x[0])

        # Export to a CSV file
        csv_filename = "videos-KITKIT.csv"
        print("Writing videos to the file \"" + csv_filename + "\"")
        with open(csv_filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, csv_fieldnames)
            csv_writer.writerow(csv_fieldnames)
            csv_writer.writerows(csv_rows)


if __name__ == "__main__":
    # Only run when not called via "import" in another file

    print(os.path.basename(__file__), "sys.version: {}".format(sys.version))
    print(os.path.basename(__file__), "sys.path: {}".format(sys.path))

    # Expect an argument representing a TSV file containing videos
    if len(sys.argv) < 2:
        # Abort execution
        exit("File argument missing. Example usage: python3 extract_video_assets_from_tsv.py library_video_data.tsv")
    file_containing_videos = sys.argv[1]
    print(os.path.basename(__file__), "file_containing_videos: \"{}\"".format(file_containing_videos))

    extract_from_tsv(file_containing_videos)
