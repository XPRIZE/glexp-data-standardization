# Extracts tablet serial numbers from log files collected from tablets, and stores them in a standardized format.
#
# Example usage:
#     python extract-tablet-serial-numbers.py ../tablet-usage-data/2019-02-18
#
# The extracted data will be stored in a file named `tablets-uploading-data-CHIMPLE_<DATE>.csv`.

import sys
import datetime


def verify_date(date_text):
    print("verify_date")
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format. Should be YYYY-mm-dd")


print("sys.version: {}".format(sys.version))

# Expect an argument representing a folder containing one week of data, e.g. "../tablet-usage-data/2019-02-18"
if len(sys.argv) < 2:
    # Abort execution
    exit("Example usage: python extract-tablet-serial-numbers.py ../tablet-usage-data/2019-02-18")
folder_containing_weekly_data = sys.argv[1]
print("folder_containing_weekly_data: \"{}\"".format(folder_containing_weekly_data))

# Extract the date (the last 10 characters) from the folder path
date = folder_containing_weekly_data[len(folder_containing_weekly_data)-10:len(folder_containing_weekly_data)]
print("date: \"{}\"".format(date))

# Verify that the folder name is on the format "YYYY-mm-dd"
verify_date(date)


