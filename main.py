import sys
import re
from collections import OrderedDict


class TwoPhaseSort:
    def __init__(self, input_file, output, column_list, main_memory):
        self.info_file = OrderedDict()
        self.input_file = input_file
        self.output_file = output
        self.column_list = column_list
        self.main_memory = main_memory
        self.buffer = []  # this is used to keep the unsorted data chunk for writing to temp files

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

    def write_temp_file(self, index):
        """
        It writes the current sorted buffer into a temporary file, and clears the buffer for future operations
        :param index: used for naming the current file by adding index as suffix
        :return: nothing
        """
        file_name = "temp" + str(index)
        curr_file = open(file_name, 'w')
        total_columns = len(self.info_file)
        start = True
        if len(self.buffer) == 0:
            raise NotImplementedError("The buffer size is 0, cannot write to disk")
        for row in self.buffer:
            if start:
                curr_file.write("\n")
                start = False
            i = 0
            for data in row:
                if i == total_columns - 1:
                    curr_file.write(str(data))
                else:
                    curr_file.write(str(data) + ", ")
                i += 1
        self.buffer.clear()
        