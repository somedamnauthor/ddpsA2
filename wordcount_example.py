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

        results = []
        default_count = 1
        # seperate line into words
        for word in value.split():
            if self.is_valid_word(word):
                # lowercase words
                results.append((word.lower(), default_count))
        return results

    def is_valid_word(self, word):
        """Checks if the word is in the defined character range

        :param word: word to check
        """
        return all(64 < ord(character) < 128 for character in word)

    def reducer(self, key, values):
        """Reduce function implementation for the word count example
        Note: Each line needs to be separated into words, and each word
        needs to be converted to lower case.
        """
        wordcount = sum(value for value in values)
        return key, wordcount


if __name__ == '__main__':

    input_dir = 'input'
    output_dir = 'output'
    n_mappers = int(sys.argv[1])
    n_reducers = int(sys.argv[2])
    mode = sys.argv[3]
    final_flag = sys.argv[4]
    tid = sys.argv[5]

    word_count = WordCount(input_dir, output_dir, n_mappers, n_reducers)
    word_count.execute_mapreduce(mode=mode, tid=tid)

    if final_flag == 'final':
        result = (word for word in word_count.join_outputs(final_flag=final_flag))
        print "-- Results of wordcount with parameters:", input_dir, output_dir, n_mappers, n_reducers
        results_to_show = 50
        print "-- Showing ", results_to_show," words:"
        for i in range(results_to_show):
            try:
                word, count = result.next()
                print word, count
            except Exception:
                pass
    else:
        print "Non-final Map/Reduce Completed"
