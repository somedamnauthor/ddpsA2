import sys
from mapreduce import MapReduce

print "Python Major Version",sys.version_info[0],"Minor Version",sys.version_info[1]

# The WordCount class inherits off the MapReduce class and implements/overrides the mapper and reducer functions
class WordCount(MapReduce):

    # Initialize/Instantiate the attributes using the base class
    def __init__(self, input_dir, output_dir, n_mappers, n_reducers):
        MapReduce.__init__(self,  input_dir, output_dir, n_mappers, n_reducers)

    # Implement the mapper
    def mapper(self, key, value):

        """
        Function that implements the mapper and overrides the corresponding MapReduce class method 

        Input - 
        key, value: Value stores the line, the keys are words and the values then become the count

        Output - 
        results: list containing key-value pairs where the key is word and value is count
        """

        # Initialize list to store intermediate mapper output
        results = []

        # At minimum, an encountered word appears once
        count = 1

        # split each line (value) into words (keys)
        for single_word in value.split():

            # Skip word if invalid
            if self.is_word(single_word):

                # attach a value (count) to each key
                results.append((single_word.lower(), count))

        return results

    def is_word(self, word):
        
        """
        Function to check if the given word is a valid word, i.e does not contain any special characters

        Input - 
        word: word given by the mapper() method
        """

        return all(64 < ord(character) < 128 for character in word)

    def reducer(self, key, values):

        """
        Function that implements the reducer and overrides the corresponding MapReduce class method 

        Input - 
        key, values

        Outputs - 
        key, wordcount - the key-value pair output that will later be consolidated
        """

        wordcount = sum(value for value in values)
        return key, wordcount


if __name__ == '__main__':

    # Example argument list - 4 4 mapreduce final

    # Hard-coding input and output directories
    input_dir = 'input'
    output_dir = 'output'

    # Accept number of mappers and reducers for task from user
    n_mappers = int(sys.argv[1])
    n_reducers = int(sys.argv[2])

    # Mode will be passed onto the run() method and decide whether the mapper controller, reducer controller or both will be invoked
    # Can be 'map', 'reduce' or 'mapreduce' - the latter is used only in the case of non-distributed implementation
    # This is given by the controller script
    mode = sys.argv[3]

    # Boolean flag to signify whether the reduce operation (assuming mode is either 'reduce' or 'mapreduce') is the final one
    # If the reduce operation is the final one the outputsd of the reducers are to be joined
    # This is given by the controller script
    final_flag = sys.argv[4]

    # The ID of the thread that is used to decide the chunk the reducer should work on
    # Only used when mode is either 'reduce' or 'mapreduce'
    # This is given by the controller script
    tid = sys.argv[5]

    # Instantiate WordCount class with the user inputs
    word_count = WordCount(input_dir, output_dir, n_mappers, n_reducers)

    # Run the map/reduce (or both) operations with the script inputs
    word_count.execute_mapreduce(mode=mode, tid=tid)

    # Only consolidate results if the operation in question is the final reduce operation
    if final_flag == 'final':

        # Instantiate variable to read the joined outputs
        reducer_result = (word for word in word_count.join_outputs(final_flag=final_flag))

        print "\n\nPrinting top 25 most frequent words and their frequencies:"
        for i in range(25):

            try:
                word, count = reducer_result.next()
                print word, count

            except Exception:
                pass

    else:
        print "Non-final Map/Reduce Completed"
