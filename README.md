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

## Sequential Implementation

#### How to run this
```
python3 main.py input.txt output.txt main_memory order C1 C3 ....
```
Here **input.txt** is the file containing the raw records which we need to sort.
**output.txt** is the file which will contain the sorted records.
**main_memory** is the allowed main memory. **order** can be either ascending or
descending. Finally we need to give a list of columns according to which sorting
needs to happen.

