# Extracts storybooks from library_book_data.tsv (copied from library_sw_tz.tar.gz), and stores them in a standardized format.
#
# Example usage:
#     cd storybooks
#     python3 extract_storybook_assets_from_tsv.py library_book_data.tsv
#
# The extracted data will be stored in a file named `storybooks-KITKIT.csv`.

import csv
import os
import sys
import warnings

from Storybook import Storybook


def extract_from_tsv(file_containing_storybooks):
    print(os.path.basename(__file__), "extract_from_tsv")

    csv_rows = []

    with open(file_containing_storybooks) as tsv_file:
        tsv_data = csv.reader(tsv_file, dialect="excel-tab")
        row_counter = 0
        for storybook_row in tsv_data:
            row_counter += 1

            if row_counter <= 7:
                # Skip the first 7 rows of the spreadsheets since they contain comments instead of data values.
                warnings.warn("Skipping row: {}".format(storybook_row))
                continue

            print(os.path.basename(__file__), "storybook_row: {}".format(storybook_row))

            # TSV columns: id,category,categoryname,title,author,thumbnail,foldername

            storybook = Storybook()

            # id (e.g. "sw_102")
            storybook_row_id = storybook_row[0]
            storybook.id = storybook_row_id[3:len(storybook_row_id)]

            # category
            # TODO

            # title
            storybook.title = storybook_row[3]

            # TODO: check if the book contains comprehension questions
            storybook.comprehension_questions = None

            # foldername
            folder_name = storybook_row[6]
            storybook.asset_path = "mainapp_sw_tz.tar.gz:mainapp_sw_tz/Resources/localized/sw-tz/games/books/bookdata/" + folder_name + "/"

            csv_row = [storybook.id, storybook.title, storybook.comprehension_questions, storybook.asset_path]
            print("Adding CSV row: {}".format(csv_row))
            csv_rows.append(csv_row)

        # Define columns
        csv_fieldnames = ['id', 'title', 'comprehension_questions', 'asset_path']

        # Sort rows by id (1st column)
        csv_rows = sorted(csv_rows, key=lambda x: x[0])

        # Export to a CSV file
        csv_filename = "storybooks-KITKIT.csv"
        print("Writing storybooks to the file \"" + csv_filename + "\"")
        with open(csv_filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, csv_fieldnames)
            csv_writer.writerow(csv_fieldnames)
            csv_writer.writerows(csv_rows)


if __name__ == "__main__":
    # Only run when not called via "import" in another file

    print(os.path.basename(__file__), "sys.version: {}".format(sys.version))
    print(os.path.basename(__file__), "sys.path: {}".format(sys.path))

    # Expect an argument representing a TSV file containing storybooks
    if len(sys.argv) < 2:
        # Abort execution
        exit("File argument missing. Example usage: python3 extract_storybook_assets_from_tsv.py library_book_data.tsv")
    file_containing_storybooks = sys.argv[1]
    print(os.path.basename(__file__), "file_containing_storybooks: \"{}\"".format(file_containing_storybooks))

    extract_from_tsv(file_containing_storybooks)
