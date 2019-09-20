# Contains utility functions for working with tablet serial numbers

import os
import re


# Expects serial numbers to be on the format "5A27001390"
def is_valid(serial_number):
    print(os.path.basename(__file__), "is_valid")

    if not serial_number.strip():
        return False

    if len(serial_number) != 10:
        return False

    pattern = re.compile("[0-9A-F]{10}")
    if not pattern.match(serial_number):
        return False

    return True
