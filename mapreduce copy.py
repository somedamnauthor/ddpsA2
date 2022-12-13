import os
import json
import settings
from multiprocessing import Process


class FileHandler(object):
    """FileHandler class
    Manages splitting input files and joining outputs together.
    
    """
    def __init__(self, input_file_path, output_dir):
        """
        Note: the input file path should be given for splitting.
        The output directory is needed for joining the outputs.

        :param input_file_path: input file path
        :param output_dir: output directory path

        """
        self.input_file_path = input_file_path
        self.output_dir = output_dir

    def initiate_file_split(self, split_index, index):
        """initialize a split file by opening and adding an index.

        :param split_index: the split index we are currently on, to be used for naming the file.
        :param index: the index given to the file.

        """
        file_split = open(settings.get_input_split_file(split_index-1), "w+")
        file_split.write(str(index) + "\n")
        return file_split

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
        current_split_unit = self.initiate_file_split(current_split_index, index)
        for character in file_content:
            current_split_unit.write(character)
            if self.is_on_split_position(character, index, unit_size, current_split_index):
                current_split_unit.close()
                current_split_index += 1
                current_split_unit = self.initiate_file_split(current_split_index, index)
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

    def __init__(self, input_dir = settings.default_input_dir, output_dir = settings.default_output_dir,
                 n_mappers = settings.default_n_mappers, n_reducers = settings.default_n_reducers,
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
        self.file_handler = FileHandler(settings.get_input_file(self.input_dir), self.output_dir)
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

    def run(self, join=False):
        """Executes the map and reduce operations

        :param join: True if we need to join the outputs, False by default.
        """
        # initialize mappers list
        map_workers = []
        # initialize reducers list
        rdc_workers = []
        # run the map step
        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]
        # run the reduce step
        for thread_id in range(self.n_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in rdc_workers]
        if join:
            self.join_outputs()
        
    def join_outputs(self, clean = True, sort = True, decreasing = True):
        """Join all the reduce output files into a single output file.
        
        :param clean: if True the reduce outputs will be deleted, by default takes the value of self.clean
        :param sort: sort the outputs
        :param decreasing: sort by decreasing order, high value to low value
        
        """
        try:
            return self.file_handler.join_files(self.n_reducers, clean, sort, decreasing)
        except Exception, e:
            print "Exception occured while joining: maybe the join has been performed already  -- "+str(e)
            return []