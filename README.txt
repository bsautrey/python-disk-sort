Written by Ben Autrey: https://github.com/bsautrey

---Overview---

disk_sort.py allows you to:

* Sort large datasets that are too large to sort in memory.

* Read the sorted dataset group-by-group, i.e. all the data for a given key is returned with each call to next_group()

* Set the maximum amount of memory used to sort the data.

* Sort datasets that contain python data structures.

---Example---

To run this example:

1) Change dir to where disk_sort.py is.

2) Set WORKING_DIR in the code below and run it in a python terminal:

import random
from time import time
from math import fmod
from disk_sort import DiskSort

# Add the data to be sorted. Time it.
ds = DiskSort('WORKING_DIR')
indicies = range(1000)
len_indicies = len(indicies)
random.shuffle(indicies)
rand_str = 'Cow tipping in Elysian Fields.'
number_of_lines = 1000000
start_time = time()
for i in xrange(number_of_lines):
    rand_int = indicies[int(fmod(i,len_indicies))]
    data = (rand_int,rand_str)
    ds.append(data)
    if i == (number_of_lines-1):
        end_time = time()
        diff_time = end_time - start_time
        print 'TIME:',diff_time
    
# Return the sorted data by group. Time it.
start_time = time()
while True:
    try:
        group = ds.next_group()
        rand_inx = group[0][0]
        l = len(group)
        print 'KEY AND NUMBER ITEMS PER KEY:',rand_inx,l
    except StopIteration:
        end_time = time()
        diff_time = end_time - start_time
        print 'TIME:',diff_time
        break