import sys
import re
from collections import OrderedDict


class TwoPhaseSort:
    def __init__(self, input_file, output, column_name_list, main_memory, descending):
        self.info_file = OrderedDict()
        self.input_file = input_file
        self.output_file = output
        self.column_name_list = column_name_list  # list of names which represent the order of columns
        self.column_list = []  # list of numbers(indices) which represent the order of columns
        self.main_memory = main_memory
        self.descending = descending
        self.buffer = []  # this is used to keep the unsorted data chunk for writing to temp files
        self.temp_file_count = 0
        self.record_size = 0
        self.meta_info()
        self.fill_column_list()
        self.calc_record_size()

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

    def fill_column_list(self):
        """
        Fills the column_list which contains the indices representing the order in which sorting should take place
        :return: nothing
        """
        ind = 0
        temp = OrderedDict()
        for key in self.info_file.keys():
            if key in self.column_name_list:
                temp[key] = ind
            ind += 1
        for name in self.column_name_list:
            self.column_list.append(temp[name])

    def write_temp_file(self, index):
        """
        It writes the current sorted buffer into a temporary file, and clears the buffer for future operations.
        The files are name beginning from 1, i.e temp1, temp2, ....... tempN.
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
        self.temp_file_count += 1

    def calc_record_size(self):
        """
        calculates the record size i.e tuple size of the relation
        :return: nothing
        """
        temp_sum = 0
        for key, val in self.info_file.items():
            temp_sum += val
        self.record_size = temp_sum

    def write_one_row(self, row):
        """
        Fills one row in the buffer in the column order specified by column list
        :param row:
        :return:
        """
        temp = []
        for ind in self.column_list:
            temp.append(row[ind])
        total_columns = len(self.info_file)
        for i in range(total_columns):
            if i not in self.column_list:
                temp.append(row[i])
        temp = tuple(temp)
        self.buffer.append(temp)

    def phase_one(self):
        """
        Phase one sorts the data by dividing it into chunks, and write them back to disk
        :return: nothing
        """
        read_file = open(self.input_file, 'r')
        processed_size = 0
        for row in read_file.readlines():
            processed_size += self.record_size
            if processed_size > self.main_memory:
                self.buffer.sort()
                if self.descending:
                    self.buffer.reverse()
                self.write_temp_file(self.temp_file_count + 1)
                processed_size = self.record_size
            self.write_one_row(row)



