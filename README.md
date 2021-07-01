# TwoPhaseMultiway
Two Phase Sorting in Database.

This was a project done as part of the Data-Systems course (Advanced DBMS) for
learning how large amount data which cannot fit the main memory is sorted.

## Phase one
In this phase the file needs to be read in chunks less than the main memory
allowed. Then these chunks need to be sorted either in ascending or descending
order as provided by the user. Each row data is reshuffled according to the
column list provided by user. These sorted sub-lists are written back to the
secondary storage.

## Phase two
This phase is performed once the file is stored in secondary memory as sorted
sub-lists. This phase is similar to the merge step in merge-sort algorithm, the
difference being that instead of only two lists, we have multiple of such lists
which needed to be merged and hence the name **Multi-way**. We maintain one
pointer for each of the sub-lists. Then in each iteration we compare the tuples
pointed at by these pointers and pick the smallest of them, and write that tuple
to the output file. That pointer is advanced by one step. This way all the
tuples are written to the output file.

## Sequential Implementation
Threads are not used for in this implementation of the assignment.

#### How to run this
```
python3 main.py input.txt output.txt main_memory order C1 C3 ....
```
Here **input.txt** is the file containing the raw records which we need to sort.
**output.txt** is the file which will contain the sorted records.
**main_memory** is the allowed main memory. **order** can be either ascending or
descending. Finally we need to give a list of columns according to which sorting
needs to happen.

## Parallel Implementation
Multiple threads are used in this implementation.
A class is created which extends from the Thread class, which runs the
Sequential Implementation.
Further we first need to split the single input file into multiple files for
each thread to parallelly work on. Then we sort these files individually.
Finally the sorted files are merged by the first thread and result is written to
the output file.

#### How to run this
```
python3 main.py input.txt output.txt main_memory no_of_threads order C4 C2 ....
```
Here we also need to provide the **number of threads** as an additional input
compared with the sequential case.

