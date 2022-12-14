# Single-threaded (serial) implementation with support for distribution

import os
import json
import settings
from multiprocessing import Process

# This class handles file operations - These include splitting and joining files for mapping and reducing respectively
class FileOps(object):
    
    def __init__(self, input_path='input', output_path='output'):
        """
        Constructor to initialize input and output filepaths.
        These have been currently hardcoded, therefore: 
        - They need to be on the same level as the current mapreduce.py file
        - They need to be called 'inputFiles' and 'outputFiles' respectively
        """
        self.input_file_path = input_path
        self.output_dir = output_path

    # This function splits the file by first inserting an index in the beginning of the file
    def create_indexed_file(self, file_split_point, index):
        split = open(settings.get_input_split_file(file_split_point-1), "w+")
        split.write(str(index) + "\n")
        return split


    def is_on_split_position(self, character, index, split_size, current_split):
        """Check if it is the right time to split.
        i.e: character is a space and the limit has been reached.

        :param character: the character we are currently on.
        :param index: the index we are currently on.
        :param split_size: the size of each single split.
        :param current_split: the split we are currently on.

        """
        return index>split_size*current_split+1 and character.isspace()


    def split_file(self, number_of_splits):
        """split a file into multiple files.
        note: this has not been optimized to avoid overhead.

        :param number_of_splits: the number of chunks to
        split the file into.

        """
        file_size = os.path.getsize(self.input_file_path)
        unit_size = file_size / number_of_splits + 1
        original_file = open(self.input_file_path, "r")
        file_content = original_file.read()
        original_file.close()
        (index, current_split_index) = (1, 1)
        current_split_unit = self.create_indexed_file(current_split_index, index)
        for character in file_content:
            current_split_unit.write(character)
            if self.is_on_split_position(character, index, unit_size, current_split_index):
                current_split_unit.close()
                current_split_index += 1
                current_split_unit = self.create_indexed_file(current_split_index, index)
            index += 1
        current_split_unit.close()
        

    def join_files(self, number_of_files, clean = False, sort = True, decreasing = True):
        """join all the files in the output directory into a
        single output file.

        :param number_of_files: total number of files.
        :param clean: if True the reduce outputs will be deleted,
        by default takes the value of self.clean.
        :param sort: sort the outputs.
        :param decreasing: sort by decreasing order, high value
        to low value.

        :return output_join_list: a list of the outputs
        """
        output_join_list = []
        for reducer_index in xrange(0, number_of_files):
            f = open(settings.get_output_file(reducer_index), "r")
            output_join_list += json.load(f)
            f.close()
            if clean:
                os.unlink(settings.get_output_file(reducer_index))
        if sort:
            from operator import itemgetter as operator_ig
            # sort using the key
            output_join_list.sort(key=operator_ig(1), reverse=decreasing)
        output_join_file = open(settings.get_output_join_file(self.output_dir), "w+")
        json.dump(output_join_list, output_join_file)
        output_join_file.close()
        return output_join_list
    


class MapReduce(object):
    """MapReduce class
    Note: mapper and reducer functions need to be implemented.
    
    """

    def __init__(self, input_dir = 'input', output_dir = 'output',
                 n_mappers = 4, n_reducers = 4,
                 clean = True):
        """

        :param input_dir: directory of the input files,
        taken from the default settings if not provided
        :param output_dir: directory of the output files,
        taken from the default settings if not provided
        :param n_mappers: number of mapper threads to use,
        taken from the default settings if not provided
        :param n_reducers: number of reducer threads to use,
        taken from the default settings if not provided
        :param clean: optional, if True temporary files are
        deleted, True by default.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.clean = clean
        self.file_handler = FileOps(settings.get_input_file(self.input_dir), self.output_dir)
        self.file_handler.split_file(self.n_mappers)


    def mapper(self, key, value):
        """outputs a list of key-value pairs, where the key is
        potentially new and the values are of a potentially different type.
        Note: this function is to be implemented.

        :param key: key of the current mapper
        :param value: value for the corresponding key
        note: this method should be implemented
        """
        pass


    def reducer(self, key, values_list):
        """Outputs a single value together with the provided key.
        Note: this function is to be implemented.

        :param key: key of the reducer
        :param value_list: list of values for the key
        note: this method should be implemented
        """
        pass


    def check_position(self, key, position):
        """Checks if we are on the right position

        """
        return position == (hash(key) % self.n_reducers)


    def run_mapper(self, index):
        """Runs the implemented mapper

        :param index: the index of the thread to run on
        """
        input_split_file = open(settings.get_input_split_file(index), "r")
        key = input_split_file.readline()
        value = input_split_file.read()
        input_split_file.close()
        if(self.clean):
            os.unlink(settings.get_input_split_file(index))
        mapper_result = self.mapper(key, value)
        for reducer_index in range(self.n_reducers):
            temp_map_file = open(settings.get_temp_map_file(index, reducer_index), "w+")
            json.dump([(key, value) for (key, value) in mapper_result 
                                        if self.check_position(key, reducer_index)]
                        , temp_map_file)
            temp_map_file.close()
        

    def run_reducer(self, index):
        """Runs the implemented reducer

        :param index: the index of the thread to run on
        """
        print "Inside run_reducer, index:",index
        key_values_map = {}
        for mapper_index in range(self.n_mappers):
            temp_map_file = open(settings.get_temp_map_file(mapper_index, index), "r")
            mapper_results = json.load(temp_map_file)
            for (key, value) in mapper_results:
                if not(key in key_values_map):
                    key_values_map[key] = []
                try:
                    key_values_map[key].append(value)
                except Exception, e:
                    print "Exception while inserting key: "+str(e)
            temp_map_file.close()
            if self.clean:
                os.unlink(settings.get_temp_map_file(mapper_index, index))
        key_value_list = []
        for key in key_values_map:
            key_value_list.append(self.reducer(key, key_values_map[key]))
        output_file = open(settings.get_output_file(index), "w+")
        json.dump(key_value_list, output_file)
        output_file.close()


    def mapper_mode(self):

        # initialize mappers list
        map_workers = []

        # run the map step
        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))  
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]

    
    def reducer_mode(self, join=False, thread_id=0):

        # initialize reducers list
        rdc_workers = []

        # run the reduce step
        # for thread_id in range(self.n_reducers):
        print "Thread ID:",thread_id
        p = Process(target=self.run_reducer, args=(thread_id,))
        p.start()
        rdc_workers.append(p)
        [t.join() for t in rdc_workers]

        if join:
            self.join_outputs()


    def run(self, join=False, mode='mapreduce', tid=0):
        """Executes the map and reduce operations

        :param join: True if we need to join the outputs, False by default.
        """

        if 'map' in mode:
            self.mapper_mode()
        if 'reduce' in mode:
            if self.n_reducers > 1:
                for i in range(self.n_reducers):
                    self.reducer_mode(thread_id=i)
            else:
                self.reducer_mode(thread_id=tid)

        # # initialize mappers list
        # map_workers = []
        # # initialize reducers list
        # rdc_workers = []

        # # run the map step
        # for thread_id in range(self.n_mappers):
        #     p = Process(target=self.run_mapper, args=(thread_id,))
        #     # pickle.dump(p, open("p1.p","wb"))   
        #     # sdfsdf    
        #     p.start()
        #     map_workers.append(p)
        # [t.join() for t in map_workers]

        # # run the reduce step
        # for thread_id in range(self.n_reducers):
        #     p = Process(target=self.run_reducer, args=(thread_id,))
        #     p.start()
        #     rdc_workers.append(p)
        # [t.join() for t in rdc_workers]
        # if join:
        #     self.join_outputs()



    def join_outputs(self, clean = True, sort = True, decreasing = True, final_flag='n'):
        """Join all the reduce output files into a single output file.
        
        :param clean: if True the reduce outputs will be deleted, by default takes the value of self.clean
        :param sort: sort the outputs
        :param decreasing: sort by decreasing order, high value to low value
        
        """
        #TODO - Possibly better to take in a new parameter for number of files to reduce
        if final_flag == 'final':
            self.n_reducers = self.n_mappers

        try:
            return self.file_handler.join_files(self.n_reducers, clean, sort, decreasing)
        except Exception, e:
            print "Exception occured while joining: maybe the join has been performed already  -- "+str(e)
            return []