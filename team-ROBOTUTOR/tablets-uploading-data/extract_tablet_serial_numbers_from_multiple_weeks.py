# Collects tablet serials numbers from multiple weeks of data and combines them into one file.
#
# Example usage:
#     python3 extract_tablet_serial_numbers_from_multiple_weeks.py ../tablet-usage-data
#
# The extracted data will be stored in a file named `tablets-uploading-data-ROBOTUTOR.csv`.

import sys
import os

import extract_tablet_serial_numbers

# A directory containing multiple subdirectories on the format "2017-12-22", "2017-12-29", etc.
BASE_PATH = "../tablet-usage-data"
if len(sys.argv) > 1:
    BASE_PATH = sys.argv[1]
print(os.path.basename(__file__), "BASE_PATH: {}".format(BASE_PATH))

# Data was not collected between 2017-12-22 and 2018-06-08 due to technical issues.
# Data was not collected during the week of 2018-06-15.
# Data was not collected during the week of 2019-02-15.
data_collection_week_end_dates = [
    '2018-06-08', '2018-06-22', '2018-06-29',
    '2018-07-06', '2018-07-13', '2018-07-20', '2018-07-27',
    '2018-08-03', '2018-08-10', '2018-08-17', '2018-08-24', '2018-08-31',
    '2018-09-07', '2018-09-14', '2018-09-21', #'2018-09-28',
    # '2018-10-05', '2018-10-12', '2018-10-19', '2018-10-26',
    '2018-10-19', '2018-10-26',
    '2018-11-02', '2018-11-09', '2018-11-16', '2018-11-23', '2018-11-30',
    '2018-12-07', '2018-12-14', '2018-12-21', '2018-12-28',
    '2019-01-04', '2019-01-11', '2019-01-18', '2019-01-25',
    '2019-02-01', '2019-02-08', '2019-02-22',
    '2019-03-01'
]
print(os.path.basename(__file__), "len(data_collection_week_end_dates): {}".format(len(data_collection_week_end_dates)))

# Extract tablet serial numbers and store them in a CSV file for each week of data
for week_end_date in data_collection_week_end_dates:
    directory_containing_weekly_data = BASE_PATH + os.sep + week_end_date
    print(os.path.basename(__file__), "directory_containing_weekly_data: \"{}\"".format(directory_containing_weekly_data))
    extract_tablet_serial_numbers.extract_from_week(directory_containing_weekly_data)

# Combine each CSV file for one week of data into one file
print(os.path.basename(__file__), "Writing data to \"tablets-uploading-data-ROBOTUTOR.csv\"...")
with open('tablets-uploading-data-ROBOTUTOR.csv', 'w') as outfile:
    infile_count = 0
    for week_end_date in data_collection_week_end_dates:
        print(os.path.basename(__file__), "\n\n"
                                          "**********\n")
        csv_filename_weekly = "tablets-uploading-data-ROBOTUTOR_" + week_end_date + ".csv"
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
print(os.path.basename(__file__), "Writing data to \"tablets-uploading-data-ROBOTUTOR.csv\" complete!")
