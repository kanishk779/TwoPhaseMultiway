import sys
import re
from collections import OrderedDict


class TwoPhaseSort:
    def __init__(self, input, output, column_list, main_memory):
        self.info_file = OrderedDict()

    def meta_info(self):
        """
        Reads the metadata.txt file to read in the schema of the table containing the records.
        """
        try:
            meta_file = open('metadata.txt', 'r')
        except FileNotFoundError:
            print('metadata.txt not found')
        else:
            for line in meta_file.readlines():
                line = line.strip()
                data = line.split(',')
                if len(data) != 2:
                    raise NotImplementedError("metadata.txt is not in correct format")
                col_name = data[0].strip()
                col_size = int(data[1].strip())
                self.info_file[col_name] = col_size

    
