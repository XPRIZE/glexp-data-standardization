# Collects storybook events from multiple weeks of data and combines them into one file.
#
# Example usage:
#     cd storybook-events
#     python3 extract_storybook_events_from_multiple_weeks.py ../tablet-usage-data
#
# The extracted data will be stored in a file named `storybook-events-KITKIT.csv`.

import sys
import os

import extract_storybook_events_from_txt

# A directory containing multiple subdirectories on the format "2017-12-22", "2017-12-29", etc.
BASE_PATH = "../tablet-usage-data"
if len(sys.argv) > 1:
    BASE_PATH = sys.argv[1]
print(os.path.basename(__file__), "BASE_PATH: {}".format(BASE_PATH))

data_collection_week_end_dates = [
    '2017-12-22', '2017-12-29',
    '2018-01-05', '2018-01-12', '2018-01-19', '2018-01-26',
    '2018-02-02', '2018-02-09', '2018-02-16', '2018-02-23',
    '2018-03-02', '2018-03-09', '2018-03-16', '2018-03-23', '2018-03-30',
    '2018-04-06', '2018-04-13', '2018-04-20', '2018-04-27',
    '2018-05-04', '2018-05-11', '2018-05-18', '2018-05-25',
    '2018-06-01', '2018-06-08', '2018-06-15', '2018-06-22', '2018-06-29',
    '2018-07-06', '2018-07-13', '2018-07-20', '2018-07-27',
    '2018-08-03', '2018-08-10', '2018-08-17', '2018-08-24', '2018-08-31',
    '2018-09-07', '2018-09-14', '2018-09-21', '2018-09-28',
    '2018-10-05', '2018-10-12', '2018-10-19', '2018-10-26',
    '2018-11-02', '2018-11-09', '2018-11-16', '2018-11-23', '2018-11-30',
    '2018-12-07', '2018-12-14', '2018-12-21', '2018-12-28',
    '2019-01-04', '2019-01-11', '2019-01-18', '2019-01-25',
    '2019-02-01', '2019-02-08', '2019-02-22'
]
print(os.path.basename(__file__), "len(data_collection_week_end_dates): {}".format(len(data_collection_week_end_dates)))

# Extract storybook events and store them in a CSV file for each week of data
for week_end_date in data_collection_week_end_dates:
    directory_containing_weekly_data = BASE_PATH + os.sep + week_end_date
    print(os.path.basename(__file__), "directory_containing_weekly_data: \"{}\"".format(directory_containing_weekly_data))
    extract_storybook_events_from_txt.extract_from_week(directory_containing_weekly_data)

# Combine each CSV file for one week of data into one file
print(os.path.basename(__file__), "Writing data to \"storybook-events-KITKIT.csv\"...")
with open('storybook-events-KITKIT.csv', 'w') as outfile:
    infile_count = 0
    for week_end_date in data_collection_week_end_dates:
        print(os.path.basename(__file__), "\n\n"
                                          "**********\n")
        csv_filename_weekly = "storybook-events-KITKIT_" + week_end_date + ".csv"
        print(os.path.basename(__file__), "csv_filename: \"{}\"".format(csv_filename_weekly))
        with open(csv_filename_weekly) as infile:
            infile_row_count = 0
            for line in infile:
                print(os.path.basename(__file__), "line: {}".format(line))
                print(os.path.basename(__file__), "infile_row_count: {}".format(infile_row_count))

                # Column headers are included in each weekly file.
                # Only include them once, and skip them for each subsequent file.
                is_column_header_row = (infile_row_count == 0)
                is_first_infile = (infile_count == 0)
                skip_row = (is_column_header_row and not is_first_infile)

                if not skip_row:
                    outfile.write(line)

                infile_row_count += 1
        infile_count += 1
print(os.path.basename(__file__), "Writing data to \"storybook-events-KITKIT.csv\" complete!")
