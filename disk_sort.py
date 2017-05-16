# disk_sort.py is a disk-based merge sort for large datasets containing python data structures.

import heapq,uuid,os,cPickle,sys,random
from math import fmod
from time import time

# you can use other implementations without changing the code, e.g. import ujson as json.
import json as json

# these globals are used to calibrate the amount of memory usage.
# small_item_size*small_total_lines = mex memory usage per server (process) 
small_item_size = 60.0 # bytes
small_total_lines = 2500000.0
number_of_servers_per_location = 1 # number of servers (processes) running disk_sory.py simultaneously.

class DiskSort():
    
    def __init__(self,working_dir):
        # init
        self.working_dir = working_dir
        self.max_number_items = 500000
        
        # shared references
        self.number_items = 0
        self.current_list = []
        self.file_names = []
        self.item_sizes = []
        self.current_item = None
        self.write_mode = True
        self.finished = False
        self.file = None
        
    def append(self,item):
        heapq.heappush(self.current_list,item)
        self._get_max_number_items(item)
        self.number_items = self.number_items + 1
        if fmod(self.number_items,self.max_number_items) == 0:
            self._reset()
        
    def next_group(self,disk_based=False):
        group = []
        
        if disk_based:
            group = DiskList(self.working_dir)
        
        if self.write_mode:
            self._reset()
            
            # initialize files
            files = []
            for fn in self.file_names:
                df = DeserializedFile(fn)
                files.append(df)
                
            # initialize merge
            self.file = heapq.merge(*files)
            
            # initialize first item
            self.current_item = self.file.next()
            
            self.write_mode = False
            
        elif self.finished:
            raise StopIteration
        
        item = self.current_item
        while item[0] == self.current_item[0]:
            group.append(item)
            try:
                item = self.file.next()
            except StopIteration:
                self.finished = True
                break
  
        self.current_item = item
        return group
            
    def _reset(self):
        self._write()
        self.current_list = []
        
    def _get_file_reference(self):
        file_name = self.working_dir +'/SORT_'+str(uuid.uuid4())+'.data'
        file = open(file_name,'w')
        return file_name,file
        
    def _write(self):
        file_name,file = self._get_file_reference()
        while True:
            try:
                item = heapq.heappop(self.current_list)
                s = json.dumps(item)
                file.write(s+'\n')
            except IndexError:
                break
                
        self.file_names.append(file_name)
        file.close()
    
    def _get_max_number_items(self,item):
        flag = False
        
        l = len(self.item_sizes)
        if l < 100:
            flag = True
            
        elif fmod(self.number_items,250000) == 0:
            del(self.item_sizes[0])
            flag = True
            
        if flag:
            s = cPickle.dumps(item)
            size = sys.getsizeof(s)
            self.item_sizes.append(size)
            self.item_sizes.sort(reverse=True)
            largest_items = self.item_sizes[0:100]
            s = sum(largest_items)
            l  = float(len(largest_items))
            mean_size = s/l
            ratio = min(small_item_size/mean_size,1.0)
            lines_per_server = small_total_lines/number_of_servers_per_location
            self.max_number_items = int(ratio*lines_per_server)
            print 'MAX NUMBER ITEMS IN MEMORY:',self.max_number_items


# create an iterable object, from a file, that returns deserialized items.      
class DeserializedFile():
    
    def __init__(self,file_name):
        self.file_name = file_name
        self.file = open(file_name)
        
    def __iter__(self):
        return self
        
    def next(self):  
        try:
            s = self.file.next()
            item = json.loads(s)
            return item
        except StopIteration:
            self._delete()
            raise StopIteration
            
    def _delete(self):
        self.file.close()
        os.remove(self.file_name)
        
        
# disk_list.py offers ephemeral list-like access off of disk, one item at a time. It can be used in cases where holding large lists in memory is not feasible.

class DiskList():
    
    def __init__(self,working_dir):
        self.working_dir = working_dir
        self.working_file = working_dir +'/LIST_'+str(uuid.uuid4())+'.data'
        self.file = open(self.working_file,'w')
        self.mode = 'PUSH'
        self.len = 0
        self.current_item = None
        
    def __iter__(self):
        return self
        
    def __len__(self):
        return self.len
        
    def next(self):
        if self.mode != 'POP':
            self._reset()
            self.mode = 'POP'  
        try:
            item = cPickle.load(self.file)
            return item
        except EOFError:
            self._delete()
            raise StopIteration
            
    def next_group(self):
        group = []
        if self.current_item == None:
            item = self.next()
            self.current_item = item
        elif self.current_item == 'STOP':
            raise StopIteration
            
        item = self.current_item
        while item[0] == self.current_item[0]:
            try:
                group.append(item)
                item = self.next()
            except StopIteration:
                self.current_item = 'STOP'
                return group
                
        self.current_item = item
        return group

    def append(self,item):
        cPickle.dump(item,self.file)
        self.len = self.len + 1
        
    def _reset(self):
        self.file.close()
        self.file = open(self.working_file,'r')
        
    def _delete(self):
        self.file.close()
        os.remove(self.working_file)
        