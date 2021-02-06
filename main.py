import sys
import os
import heapq
import threading
import copy
import time
from collections import OrderedDict


class TwoPhaseSort:
    def __init__(self, input_file, output_file, column_name_list, main_memory, descending, col_sizes, info_file, thread_num=1, thread_used=False):
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
        self.thread_used = thread_used
        self.thread_num = thread_num
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
        file_name = "./new_data/temp" + str(self.thread_num) + str(index) + '.txt'
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
            temp_sum += val + 2
        temp_sum -= 1
        self.record_size = temp_sum

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
        print('Phase one started by thread {a}'.format(a=self.thread_num))
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
        print('Phase one end for thread {a}'.format(a=self.thread_num))

    def append_output(self):
        """
        appends the output_file with the buffer data, change the order as well (restore to initial order)
        :return: nothing
        """
        write_file = open(self.output_file[:-4] + str(self.thread_num) + '.txt', 'a')
        total_columns = len(self.info_file)
        for row in self.buffer:
            i = 0
            for j in range(total_columns):
                if self.thread_used:
                    data = row[j]
                else:
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
        :param row: a tuple
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
        print('Phase two started by thread {a}'.format(a=self.thread_num))
        not_processed = [1] * self.temp_file_count
        temp_files = {}
        temp_list = []

        for i in range(1, self.temp_file_count + 1):
            temp_files[i] = open('./new_data/temp' + str(self.thread_num) + str(i) + '.txt', 'r')
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

        print('Phase two end for thread {a}'.format(a=self.thread_num))


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

        meta_file.close()
        return info_file, col_sizes


class MyThread(threading.Thread):
    def __init__(self, thread_num, input_file, output_file, column_name_list, main_memory, desc, col_sizes, info_file):
        threading.Thread.__init__(self)
        self.thread_num = thread_num
        self.input_file = input_file
        self.output_file = output_file
        self.column_name_list = column_name_list
        self.main_memory = main_memory
        self.desc = desc
        self.col_sizes = col_sizes
        self.info_file = info_file

    def run(self):
        # first divide the input file equally among the threads and main memory as well
        two_phase = TwoPhaseSort(input_file=self.input_file, output_file=self.output_file, column_name_list=self.column_name_list,
                                 descending=self.desc, main_memory=self.main_memory, info_file=self.info_file, col_sizes=self.col_sizes,
                                 thread_num=self.thread_num, thread_used=True)

        two_phase.phase_one()
        two_phase.phase_two()


buffer = []
OUTPUT_FILE_NAME = ""
TOTAL_COLUMNS = 1
REVERSE_DICT = OrderedDict()
NEW_COL_SIZES = []


def give_list(row):
    row_list = []
    i = 0
    for sz in NEW_COL_SIZES:
        row_list.append(row[i: i + sz])
        i += sz + 2
    return row_list


def append_output():
    write_file = open(OUTPUT_FILE_NAME, 'a')
    for row in buffer:
        i = 0
        for j in range(TOTAL_COLUMNS):
            data = row[REVERSE_DICT[j]]
            if i < TOTAL_COLUMNS - 1:
                write_file.write(str(data) + "  ")
            else:
                write_file.write(data)
            i += 1
        write_file.write('\n')
    buffer.clear()
    write_file.close()


def merge_thread_output(num_threads, record_size, main_memory):
    """
    merge the individual output of threads into one output file
    :param num_threads:
    :param record_size:
    :param main_memory:
    :return:
    """
    not_processed = [1] * num_threads
    temp_files = {}
    temp_list = []

    for i in range(1, num_threads + 1):
        temp_files[i] = open('./new_data/output' + str(i) + str(i) + '.txt', 'r')
        first_line = temp_files[i].readline().strip('\n')
        # create a new tuple by appending the index of this file
        first_list = give_list(first_line)  # split the line
        first_list.append(str(i))
        first_line = tuple(first_list)
        temp_list.append(first_line)

    heapq.heapify(temp_list)
    written_size = 0
    while any(not_processed):
        top = heapq.heappop(temp_list)
        written_size += record_size
        if written_size > main_memory:
            append_output()
            written_size = record_size
        #  write the record to the output file, read records will be in the new order
        buffer.append(tuple(list(top[:-1])))
        file_num = top[-1]
        new_line = temp_files[int(file_num)].readline()
        new_line.strip('\n')
        if new_line:
            new_list = give_list(new_line)  # split the line
            new_list.append(file_num)
            new_line = tuple(new_list)
            heapq.heappush(temp_list, new_line)
        else:
            not_processed[int(file_num) - 1] = 0
    append_output()
    # Remember to close the files
    for i in range(1, num_threads + 1):
        temp_files[i].close()


def split_input_file(partitions, input_file_name, main_memory):
    """
    splits the input file when threads are used. Splits the data equally among the threads
    :param partitions: number of partitions to be created
    :param input_file_name: input_file
    :param main_memory: maximum main memory that can be used
    :return: nothing
    """
    print('File splitting started')
    total_size = os.stat(input_file_name).st_size  # size of the file in bytes
    _, col_sizes = meta_info()
    record_size = 2 * len(col_sizes) - 2
    for i in col_sizes:
        record_size += i
    record_size += 1  # 1 is added for accounting newline character
    num_record = total_size // record_size
    div = num_record // partitions
    rem = num_record % partitions
    num_record_per_file = [div] * (partitions - 1)
    num_record_per_file.append(div + rem)

    max_possible = main_memory // record_size
    input_file = open(input_file_name, 'r')

    file_num = 1
    for records in num_record_per_file:
        if max_possible >= records:  # we can read all the records at one go
            lines = input_file.readlines(records * (record_size - 1))
            new_file = open('./new_data/input' + str(file_num) + '.txt', 'w')
            for line in lines:
                new_file.write(line)
            new_file.close()
        else:
            temp = records
            new_file = open('./new_data/input' + str(file_num) + '.txt', 'w')
            while temp > 0:
                to_read = min(max_possible, temp)
                lines = input_file.readlines(to_read * (record_size - 1))
                for line in lines:
                    new_file.write(line)
                temp -= to_read
            new_file.close()
        file_num += 1
    input_file.close()
    print('File splitting end')


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
    global OUTPUT_FILE_NAME
    global TOTAL_COLUMNS
    global REVERSE_DICT
    global NEW_COL_SIZES

    if not os.path.isdir('./new_data'):
        os.mkdir('./new_data')
    total_size = os.stat(input_file).st_size
    div = (total_size + int(main_memory) - 1) // int(main_memory)

    info_file, col_sizes = meta_info()
    record_size = 0
    for key, val in info_file.items():
        record_size += val + 2
    record_size -= 1

    max_records = int(main_memory) // record_size
    if div < max_records:
        print(div)
        print(max_records)
        raise NotImplementedError('This sorting is not feasible')

    t0 = time.time()
    if 'a' <= sorting_type[0] <= 'z':  # threads are not used
        for i in range(5, arg_count):
            column_list.append(str(sys.argv[i]).strip())
        desc = False
        if sorting_type == "desc":
            desc = True

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

        two_phase = TwoPhaseSort(input_file=input_file, output_file=output_file, column_name_list=column_list,
                                 descending=desc, main_memory=main_memory, info_file=info_file, col_sizes=col_sizes)
        OUTPUT_FILE_NAME = output_file
        REVERSE_DICT = two_phase.reverse_dict
        NEW_COL_SIZES = copy.deepcopy(two_phase.new_col_sizes)
        TOTAL_COLUMNS = len(NEW_COL_SIZES)
        split_input_file(int(num_threads), input_file, main_memory)
        thread_list = []

        for i in range(1, int(num_threads) + 1):
            print('thread {a} started '.format(a=i))
            i_file = './new_data/' + input_file[:-4] + str(i) + '.txt'
            o_file = './new_data/' + output_file[:-4] + str(i) + '.txt'
            curr_thread = MyThread(i, i_file, o_file, column_list, main_memory_per_thread, desc, col_sizes, info_file)
            curr_thread.start()
            thread_list.append(curr_thread)

        for t in thread_list:
            t.join()

        record_size = 2 * len(col_sizes) - 1
        for sz in col_sizes:
            record_size += sz
        print('merging output of individual threads')
        merge_thread_output(int(num_threads), record_size, int(main_memory))

    t_f = time.time()
    print(t_f - t0)


if __name__ == '__main__':
    main()
