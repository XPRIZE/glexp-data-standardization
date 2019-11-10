# Extracts serial numbers found in tablet usage data, but not in tablet tracker.
#
# Usage:
#     cd tablet-tracker
#     python3 extract_serial_numbers_not_found_in_tablet_tracker.py
#
# The resulting list of missing serial numbers will be stored in a file named `serial-numbers-not-found-in-tablet-tracker.csv`.

import csv
import logging

logging.basicConfig(format='%(levelname)s | %(asctime)s | %(message)s', level=logging.DEBUG)


# Extract serial numbers from tablet tracker, and add them to an array
serial_numbers_in_tablet_tracker = []
with open('tablet-tracker-KITKIT.csv') as in_file:
    csv_data = csv.reader(in_file)
    row_count = 0
    for csv_row in csv_data:
        row_count += 1
        if row_count == 1:
            # Skip header row
            continue

        logging.debug("csv_row: {}".format(csv_row))

        # Iterate serial columns ("Serial #1" --> "Serial #7")
        removal_count = 1
        column_index = 2
        while removal_count <= 7:
            serial_number = csv_row[column_index]
            logging.debug("Serial #{0}: {1}".format(removal_count, serial_number))

            if (serial_number != "") and (serial_number not in serial_numbers_in_tablet_tracker):
                serial_numbers_in_tablet_tracker.append(serial_number)

            removal_count += 1
            if removal_count == 2:
                column_index += 1
            else:
                column_index += 2
logging.debug("serial_numbers_in_tablet_tracker: {}".format(serial_numbers_in_tablet_tracker))
logging.debug("len(serial_numbers_in_tablet_tracker): {}".format(len(serial_numbers_in_tablet_tracker)))


# Iterate the serial numbers in `tablets-uploading-data-KITKIT.csv`
with open('../tablets-uploading-data/tablets-uploading-data-KITKIT.csv') as in_file:
    serial_numbers_not_found_in_tablet_tracker = []

    csv_data = csv.reader(in_file)
    row_count = 0
    for csv_row in csv_data:
        row_count += 1
        if row_count == 1:
            # Skip header row
            continue

        logging.debug("csv_row: {}".format(csv_row))

        tablet_serials = csv_row[4]
        logging.debug("tablet_serials: {}".format(tablet_serials))

        # Convert string to array. E.g. "['5B12002947', '6111002083', '6116000839', '6116001775', '6116002363', '6130000141']"
        tablets_serials_as_array = eval(tablet_serials)
        logging.debug("tablets_serials_as_array: {}".format(tablets_serials_as_array))
        logging.debug("len(tablets_serials_as_array): {}".format(len(tablets_serials_as_array)))

        # Iterate each serial number and check if it can be found in the tablet tracker
        for tablet_serial in tablets_serials_as_array:
            logging.debug("tablet_serial: {}".format(tablet_serial))

            if tablet_serial not in serial_numbers_in_tablet_tracker:
                logging.warning("tablet_serial not found in tablet tracker: {}".format(tablet_serial))
                if tablet_serial not in serial_numbers_not_found_in_tablet_tracker:
                    serial_numbers_not_found_in_tablet_tracker.append(tablet_serial)

    logging.debug("serial_numbers_not_found_in_tablet_tracker: {}".format(serial_numbers_not_found_in_tablet_tracker))
    logging.debug("len(serial_numbers_not_found_in_tablet_tracker): {}".format(len(serial_numbers_not_found_in_tablet_tracker)))

    # Write results to a CSV file
    csv_filename = "serial-numbers-not-found-in-tablet-tracker.csv"
    print("Writing list of missing tablet serials to the file \"" + csv_filename + "\"")
    with open(csv_filename, mode='w') as csv_file:
        csv_fieldnames = ['serial_number']
        csv_writer = csv.writer(csv_file, csv_fieldnames)
        csv_writer.writerow(csv_fieldnames)
        for serial_number in serial_numbers_not_found_in_tablet_tracker:
            serial_number_as_list = [serial_number]
            csv_writer.writerow(serial_number_as_list)
