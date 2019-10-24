# Extracts storybooks from titles.json (copied from https://github.com/XPRIZE/GLEXP-Team-Chimple-goa/blob/master/goa/Resources/res/story/swa/titles.json),
# and stores them in a standardized format.
#
# Example usage:
#     cd storybooks
#     python3 extract_storybook_assets_from_json.py titles.json
#
# The extracted data will be stored in a file named `storybooks-CHIMPLE.csv`.

import csv
import os
import sys
import json

from Storybook import Storybook


def extract_from_json(file_containing_storybooks):
    print(os.path.basename(__file__), "extract_from_json")

    csv_rows = []

    with open(file_containing_storybooks) as json_file:
        storybook_json_object = json.load(json_file)
        print(os.path.basename(__file__), "storybook_json_object: {}".format(storybook_json_object))
        json_file.close()

        # Iterate each attribute of the JSON object
        storybook_id = 1
        for storybook_attribute in storybook_json_object:
            print(os.path.basename(__file__), "storybook_attribute: {}".format(storybook_attribute))

            storybook = Storybook()
            storybook.id = storybook_id
            storybook.title = storybook_json_object[storybook_attribute]
            storybook.comprehension_questions = None
            storybook.asset_path = "goa/Resources/res/story/swa/" + storybook_attribute + "/"

            csv_row = [storybook.id, storybook.title, storybook.comprehension_questions, storybook.asset_path]
            print("Adding CSV row: {}".format(csv_row))
            csv_rows.append(csv_row)

            storybook_id += 1

        # Define columns
        csv_fieldnames = ['id', 'title', 'comprehension_questions', 'asset_path']

        # # Sort rows by id (1st column)
        # csv_rows = sorted(csv_rows, key=lambda x: x[0])

        # Export to a CSV file
        csv_filename = "storybooks-CHIMPLE.csv"
        print("Writing storybooks to the file \"" + csv_filename + "\"")
        with open(csv_filename, mode='w') as csv_file:
            csv_writer = csv.writer(csv_file, csv_fieldnames)
            csv_writer.writerow(csv_fieldnames)
            csv_writer.writerows(csv_rows)


if __name__ == "__main__":
    # Only run when not called via "import" in another file

    print(os.path.basename(__file__), "sys.version: {}".format(sys.version))
    print(os.path.basename(__file__), "sys.path: {}".format(sys.path))

    # Expect an argument representing a JSON file containing storybooks
    if len(sys.argv) < 2:
        # Abort execution
        exit("File argument missing. Example usage: python3 extract_storybook_assets_from_json.py titles.json")
    file_containing_storybooks = sys.argv[1]
    print(os.path.basename(__file__), "file_containing_storybooks: \"{}\"".format(file_containing_storybooks))

    extract_from_json(file_containing_storybooks)
