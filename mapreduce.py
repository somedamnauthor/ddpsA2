# Single-threaded (serial) implementation with support for distribution
import fileNameRetriever
from operator import itemgetter as o
from multiprocessing import Process
import os
import json


# This class handles file operations - These include splitting and joining files for mapping and reducing respectively
class FileOps(object):
    
    def __init__(self, input_path='input', output_path='output'):
        
        """
        Constructor to initialize input and output directories.
        These have been currently hardcoded, therefore: 
        - They need to be on the same level as the current mapreduce.py file
        - They need to be called 'input' and 'output' respectively
        """

        # oth the input and output paths are hardcoded to 'input' and 'output'
        self.input_file_path = input_path
        self.output_dir = output_path

    
    def create_indexed_file(self, file_split_point, index):

        """
        This function splits the file by first inserting an index in the beginning of the file

        Inputs - 
        - file_split_point: This is the point where the input file is going to be split to create the current split to be returned
        - index: The index with which the resulting split file is created

        Outputs - 
        - split: This is the split file created from the given split point and created with the given index
        """

        # open the file at the given split point
        split = open(fileNameRetriever.get_input_split_file(file_split_point-1), "w+")

        # write new file with the index in the beginning of the file
        split.write(str(index) + "\n")

        return split


    def validate_split(self, check_char, split_index, size, split_number):

        """
        Function to check if the split is happening in the right place, i.e: 
        - The split cannot be in the middle of a string/number
        - The split has to happen according to the split_index specified

        Inputs - 
        check_char: the character on which the split index falls
        split_index: the point at which the file is to be split
        size: The number of characters in each split. The splits are largely equal 
        split_number: The number of the current split - for example if there are 4 splits, this number can be between 0-3

        Outputs - 
        Boolean True/False signifying whether the split is correct or not

        """

        # check if the split is happening at the right index and check if the index is on a space character
        if split_index > size*split_number+1 and check_char.isspace():
            return True
        else:
            return False


    def split_controller(self, num_chunks):

        """
        Master function to carry out the file split by invoking the create_indexed_file() and check_split() methods

        Input:
        nums_split: Number of files to split the current file into. This is set to the number of mappers specified by the user
        """
        
        # Get file and unit size size - unit size is needed for checking the split validity
        original_size = os.path.getsize(self.input_file_path)
        chunk_size = (original_size / num_chunks) + 1

        # Read the file contents and store in memory
        file = open(self.input_file_path, "r")
        original_content = file.read()
        file.close()

        # Initialize index and current split - both needed for checking split validity
        # (index, current_split_index) = (1, 1)
        intended_index = 1
        chunk_index = 1

        # initialize the split at the required index
        chunk = self.create_indexed_file(1,1)

        # Create the split
        # Traverse the file contents character by character 
        # (TODO - This is not ideal)
        for token in original_content:

            chunk.write(token)

            # Check if the split can happen after the current character
            if self.validate_split(token, intended_index, chunk_size, chunk_index):

                #Split can happen - Create the indexed file here and close
                chunk.close()
                chunk_index += 1
                chunk = self.create_indexed_file(chunk_index, intended_index)

            #Split cannot happen - move onto the next index
            intended_index += 1
        
        # File has been split, close the created file
        chunk.close()
        

    def consolidate_chunks(self, num_chunks):
        
        """
        Function to consolidate all the output chunks into a single file

        Input - 
        num_chunks: Number of chunks to consolidate
        
        Output - 
        final_list: Sorted list containing all words and their counts
        """

        # Initialize list that holds the final output
        final_list = []

        # Traverse through bunch of chunks
        for chunk_number in range(0, num_chunks):

            # open chunk
            f = open(fileNameRetriever.get_output_file(chunk_number), "r")

            # append to list and close
            final_list += json.load(f)
            f.close()

            # Remove chunks once done
            os.unlink(fileNameRetriever.get_output_file(chunk_number))

        # Sort the final list in descending order of maximum count
        final_list.sort(key=o(1), reverse=True)

        # Create a final output file to store the result 
        output_join_file = open(fileNameRetriever.get_output_join_file(self.output_dir), "w+")

        # Populate file with the list we've created
        json.dump(final_list, output_join_file)
        output_join_file.close()

        # Return list contents, used later to print to stdout
        return final_list
    

# Class containing the functions that implement the MapReduce system
# The map and reduce methods are virtual and are to be overridden by any program that uses the system 
class MapReduce(object):

    def __init__(self, input_path = 'input', output_path = 'output', num_of_chunks = 4, num_of_reducers = 4):
        
        """
        Constructor to initialize directories and user inputs/options
        """

        self.input_path = input_path
        self.output_path = output_path
        self.num_of_mappers = num_of_chunks
        self.num_of_reducers = num_of_reducers
        self.fileOps = FileOps(fileNameRetriever.get_input_file(self.input_path), self.output_path)
        self.fileOps.split_controller(self.num_of_mappers)


    # Mapper and reducer virtual functions are given below
    # Mapper takes in information in a key and value pair - for example key is line and value is word
    # Reducer takes in the key and the index it should reduce at
    # Both these functions are to be overridden

    def mapper(self, key, value):
        pass

    def reducer(self, key, values):
        pass


    def validate_pos(self, key, position):
        
        """
        Function to check if the mapper is splitting the file positions correctly for the reducer
        Invoked by run_mapper

        Inputs - 
        key: Key for the mapping
        position: intended position for the reduction

        Output - 
        Boolean True/False to check if the position valid
        """

        # Check if position is 
        if position == (hash(key) % self.num_of_reducers):
            return True
        
        else:
            return False



    def mapper_controller(self, file_index):
        
        """
        Master function that executes the mapper code by first creating key,value pairs

        Input - 
        file_index: number/identifier for the file which is being split
        """
        
        # Get the contents of the input file
        input_chunk = open(fileNameRetriever.get_input_split_file(file_index), "r")

        # Get a single line (the index on top of the chunk) and store it as key
        key = input_chunk.readline()

        # Get the entire chunk's content as the value, close and delete the chunk file
        value = input_chunk.read()
        input_chunk.close()
        os.unlink(fileNameRetriever.get_input_split_file(file_index))

        # Call the mapper and store the result, for example the word counts into a variable
        mapper_result = self.mapper(key, value)

        # Create files containing the outputs for the reducers to later work on
        for reducer_num in range(self.num_of_reducers):

            # Open a temp file
            map_intermediate = open(fileNameRetriever.get_temp_map_file(file_index, reducer_num), "w+")

            # Create a list containing the keys and values to be written out to temp file
            out = [(key, value) for (key, value) in mapper_result if self.validate_pos(key, reducer_num)]

            # Populate the temp file with the list of keys and values grabbed from the mapper
            json.dump(out, map_intermediate)
            map_intermediate.close()
        

    def reducer_controller(self, index):

        """
        Master function that runs the reducer method by first preparing the input chunk
        Invoked by mapper_mode()

        Input - 
        index: The index of the file being reduced
        """

        print "Inside run_reducer, index:",index

        # Dictionary that will eventually contain all keys and values
        kv_dict = {}

        # Iterate through the required number of mappers/splits
        for i in range(self.num_of_mappers):

            # Read the contents of each intermediate map file onto a variable
            map_intermediate = open(fileNameRetriever.get_temp_map_file(i, index), "r")

            # Convert the contents to JSON for ease of processsing
            map = json.load(map_intermediate)

            # Populate the dictionary
            for (key, value) in map:

                # Check for key not existing and create an empty corresponding value
                if not(key in kv_dict):
                    kv_dict[key] = []

                # Populate the required value
                try:
                    kv_dict[key].append(value)
                except Exception, e:
                    print "cannot retrieve key"

            # Close intermediate file as it's not needed anymore, we have the populated dictionary
            map_intermediate.close()

            # Delete the intermediate file
            os.unlink(fileNameRetriever.get_temp_map_file(i, index))
        
        # Input the dictionary into the reducer and store the outputs onto a list
        kv_list = []
        for key in kv_dict:
            kv_list.append(self.reducer(key, kv_dict[key]))

        # Create an output file for the current reducer
        output_file = open(fileNameRetriever.get_output_file(index), "w+")

        # Populate output file with list and close the former
        json.dump(kv_list, output_file)
        output_file.close()


    def mapper_mode(self):

        """
        Function that invokes the mapper controller to create mappers for the required number of splits
        Invoked in run()
        """

        # initialize mappers list
        mapper_list = []

        # Iterate for the given number of mappers
        for tid in range(self.num_of_mappers):

            # Create a mapper process
            p = Process(target=self.mapper_controller, args=(tid,))  

            # This kicks off the mapper
            p.start()

            # Store the process in the list, since the processes need to be joined later
            mapper_list.append(p)

        # Call the in-built join() method on all the mapper processes
        [t.join() for t in mapper_list]

    
    def reducer_mode(self, join=False, thread_id=0):

        """
        Invoked in run()
        Function that invokes the reducer controller to create ONE reducer for one split
        This is different to the mapper_mode() method, where all mappers are invoked from the mapper_mode()
        In this case, we reduce only a single chunk. This is because we intend to invoke this method from various systems
        Eah system/node can run multiple reducer processes, but they will be from different instantiations

        Input - 
        join: Boolean flag to signify whether the outputs of the current and all previous reducers needs to be composited
        thread_id: Executes the reducer for the given thread_id. Separate instances execute separate threads
        """

        # initialize reducers list
        # TODO - remove list usage
        reducer_list = []

        print "Thread ID:",thread_id

        # Create the reduce process
        p = Process(target=self.reducer_controller, args=(thread_id,))

        # Kick off the reduction
        p.start()

        # Add reducer to reducer list
        # TODO - Avoid this
        reducer_list.append(p)

        # Join the reducer process to close it off
        [t.join() for t in reducer_list]

        # Only invoke join_outputs() for the very final reduction, where the outputs of each reducer needs to be composited
        if join:
            self.join_outputs()


    def execute_mapreduce(self, join=False, mode='mapreduce', tid=0):
        
        """
        Master function to run the map and reduce operations - invokes mapper_mode() and reducer_mode()

        Inputs - 
        join: Boolean flag to signify whether the outputs of the current and all previous reducers needs to be composited
        mode: String that will control whether the maps, reduce, or both are executed. In the current distributed implementation it is only supposed to be one of the two at once, i.e either 'map' or 'reduce'
        tid: Thread ID for which the reduce is supposed to be run
        """

        # Check if maps are to be run
        if 'map' in mode:
            self.mapper_mode()

        # Check if reduce is to be run 
        if 'reduce' in mode:

            # In the case of the non-distributed implementation we run multiple reducers off the same program on a single system
            if self.num_of_reducers > 1:
                for i in range(self.num_of_reducers):
                    self.reducer_mode(thread_id=i)

            # In the case of the distributed implementation we run only a single reducer for a single thread ID
            else:
                self.reducer_mode(thread_id=tid)


    def join_outputs(self, final_flag='n'):
        
        """
        Function to composite all outputs into a single output file. This is to be invoked by the reducer_mode() method when the final reducer is being run
        
        Input - 
        final_flag: This signifies that the operation is a reduce and the reduce in question is the final reduce operation, thus the join should indeed go through
        """

        # TODO - Possibly better to take in a new parameter for number of files to reduce
        # Currently we decide that, if we need to reduce, the number of files to reduce is same as the number of files to map
        # !!!!WARNING - HIGHLY DELICATE and NON-IDEAL!!!!
        if final_flag == 'final':
            self.num_of_reducers = self.num_of_mappers

        # Run the consolidation
        try:
            return self.fileOps.consolidate_chunks(self.num_of_reducers)
        except Exception, e:
            print "Could not perform current join"
            return []