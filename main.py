import sys
import os
import heapq
import threading
from collections import OrderedDict


class TwoPhaseSort:
    def __init__(self, input_file, output_file, column_name_list, main_memory, descending, col_sizes, info_file):
        self.info_file = info_file
        self.input_file = input_file
        self.output_file = output_file
        self.column_name_list = column_name_list  # list of names which represent the order of columns
        self.column_list = []  # list of numbers(indices) which represent the order of columns
        self.main_memory = main_memory
        self.descending = descending
        self.buffer = []  # this is used to keep the unsorted data chunk for writing to temp files
        self.col_sizes = col_sizes
        self.new_col_sizes = []  # after the reordering of columns is completed
        self.reverse_dict = OrderedDict()
        self.temp_file_count = 0
        self.record_size = 0
        self.fill_column_list()
        self.calc_record_size()
        self.reorder_columns()

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
        file_name = "temp" + str(index) + '.txt'
        curr_file = open(file_name, 'w')
        total_columns = len(self.info_file)
        if len(self.buffer) == 0:
            raise NotImplementedError("The buffer size is 0, cannot write to disk")
        for row in self.buffer:
            i = 0
            for data in row:
                if i == total_columns - 1:
                    curr_file.write(str(data))
                else:
                    curr_file.write(str(data) + "  ")
                i += 1
            curr_file.write("\n")
        self.buffer.clear()
        self.temp_file_count += 1
        curr_file.close()

    def calc_record_size(self):
        """
        calculates the record size i.e tuple size of the relation
        :return: nothing
        """
        temp_sum = 0
        for key, val in self.info_file.items():
            temp_sum += val
        self.record_size = temp_sum
        print(self.record_size)

    def write_one_row(self, row):
        """
        Fills one row in the buffer in the column order specified by column list. We change the column order in this
        function
        :param row: a string
        :return:
        """
        ind = 0
        row_list = []
        for sz in self.col_sizes:
            row_list.append(row[ind: ind + sz])
            ind += sz + 2
        temp = []
        for ind in self.column_list:
            temp.append(row_list[ind])
        total_columns = len(self.info_file)
        for i in range(total_columns):
            if i not in self.column_list:
                temp.append(row_list[i])
        temp = tuple(temp)
        self.buffer.append(temp)

    # TODO: Complete this function
    def reorder_columns(self):
        """
        reorder the columns sizes, for final printing in the output file, so that the column order is same as input file
        :return:
        """
        new_order = OrderedDict()
        j = 0
        for ind in self.column_list:
            new_order[j] = ind
            j += 1

        total_columns = len(self.info_file)

        for i in range(total_columns):
            if i not in self.column_list:
                new_order[j] = i
                j += 1

        for key, val in new_order.items():
            self.new_col_sizes.append(self.col_sizes[val])
            self.reverse_dict[val] = key

    def phase_one(self):
        """
        Phase one sorts the data by dividing it into chunks, and write them back to disk, readlines() should not be used
        :return: nothing
        """
        read_file = open(self.input_file, 'r')
        processed_size = 0
        while True:
            row = read_file.readline()
            if not row:
                break
            processed_size += self.record_size
            if processed_size > self.main_memory:
                self.buffer.sort()
                if self.descending:
                    self.buffer.reverse()
                self.write_temp_file(self.temp_file_count + 1)
                processed_size = self.record_size
            self.write_one_row(row)  # write into buffer

        self.buffer.sort()
        if self.descending:
            self.buffer.reverse()
        self.write_temp_file(self.temp_file_count + 1)
        read_file.close()

    def append_output(self):
        """
        appends the output_file with the buffer data, change the order as well (restore to initial order)
        :return: nothing
        """
        write_file = open(self.output_file, 'a')
        total_columns = len(self.info_file)
        for row in self.buffer:
            i = 0
            for j in range(total_columns):
                data = row[self.reverse_dict[j]]
                if i < total_columns - 1:
                    write_file.write(str(data) + "  ")
                else:
                    write_file.write(data)
                i += 1
            write_file.write('\n')
        self.buffer.clear()
        write_file.close()

    def write_one_row_phase_two(self, row):
        """
        Fills one row in the buffer in the column order specified by column list. We change the column order in this
        function
        :param row: a string
        :return:
        """
        self.buffer.append(row)

    def give_list(self, row):
        """
        it is a substitute for split function, helps in splitting according to column sizes
        :param row: string which is read from temp files
        :return: list of row data
        """
        row_list = []
        i = 0
        for sz in self.new_col_sizes:
            row_list.append(row[i: i + sz])
            i += sz + 2
        return row_list

    def phase_two(self):
        """
        Phase two of two phase multi-way merge-sort
        :return: nothing
        """
        not_processed = [1] * self.temp_file_count
        temp_files = {}
        temp_list = []

        for i in range(1, self.temp_file_count + 1):
            temp_files[i] = open('temp' + str(i) + '.txt', 'r')
            first_line = temp_files[i].readline().strip('\n')
            # create a new tuple by appending the index of this file
            first_list = self.give_list(first_line)  # split the line
            first_list.append(str(i))
            first_line = tuple(first_list)
            temp_list.append(first_line)

        heapq.heapify(temp_list)
        written_size = 0
        while any(not_processed):
            top = heapq.heappop(temp_list)
            written_size += self.record_size
            if written_size > self.main_memory:
                self.append_output()
                written_size = self.record_size
            #  write the record to the output file, read records will be in the new order
            self.write_one_row_phase_two(tuple(list(top[:-1])))
            file_num = top[-1]
            new_line = temp_files[int(file_num)].readline()
            new_line.strip('\n')
            if new_line:
                new_list = self.give_list(new_line) # split the line
                new_list.append(file_num)
                new_line = tuple(new_list)
                heapq.heappush(temp_list, new_line)
            else:
                not_processed[int(file_num) - 1] = 0
        self.append_output()
        # Remember to close the files
        for i in range(1, self.temp_file_count + 1):
            temp_files[i].close()


def meta_info():
    """
    Reads the metadata.txt file to read in the schema of the table containing the records.
    """
    try:
        meta_file = open('metadata.txt', 'r')
    except FileNotFoundError:
        print('metadata.txt not found')
    else:
        info_file = OrderedDict()
        col_sizes = []
        for line in meta_file.readlines():
            line = line.strip()
            data = line.split(',')
            if len(data) != 2:
                raise NotImplementedError("metadata.txt is not in correct format")
            col_name = data[0].strip()
            col_size = int(data[1].strip())
            info_file[col_name] = col_size
            col_sizes.append(col_size)
        print("column sizes : ", end='')
        print(col_sizes)
        meta_file.close()
        return info_file, col_sizes


class MyThread(threading.Thread):
    def __init__(self, thread_id):
        threading.Thread.__init__(self)
        self.thread_id = thread_id


def split_input_file(partitions, input_file_name):
    total_size = os.stat(input_file_name).st_size  # size of the file in bytes
    _, col_sizes = meta_info()
    record = 2 * len(col_sizes) - 2
    for i in col_sizes:
        record += i
    record += 1  # 1 is added for accounting newline character
    num_record = total_size // record
    div = num_record // partitions
    rem = num_record % partitions
    num_record_per_file = [div] * (partitions - 1)
    num_record_per_file.append(div + rem)
    




def main():
    arg_count = len(sys.argv)
    if arg_count < 5:
        print("Usage : python3 main.py input.txt output.txt 50 asc c1 c2")
        raise NotImplementedError("Number of arguments are not correct")
    input_file = str(sys.argv[1]).strip()
    output_file = str(sys.argv[2]).strip()
    main_memory = int(str(sys.argv[3]).strip())
    sorting_type = str(sys.argv[4]).strip()
    column_list = []
    split_input_file(2, input_file)
    if 'a' <= sorting_type[0] <= 'z':  # threads are not used
        for i in range(5, arg_count):
            column_list.append(str(sys.argv[i]).strip())
        desc = False
        if sorting_type == "desc":
            desc = True
        info_file, col_sizes = meta_info()
        # first divide the input file equally among the threads and main memory as well
        two_phase = TwoPhaseSort(input_file=input_file, output_file=output_file, column_name_list=column_list,
                                 descending=desc, main_memory=main_memory, info_file=info_file, col_sizes= col_sizes)

        two_phase.phase_one()
        two_phase.phase_two()
    else:
        num_threads = sorting_type
        sorting_type = str(sys.argv[5]).strip()
        desc = False
        if sorting_type == "desc":
            desc = True
        for i in range(6, arg_count):
            column_list.append(str(sys.argv[i]).strip())
        main_memory_per_thread = main_memory // int(num_threads)


if __name__ == '__main__':
    main()
