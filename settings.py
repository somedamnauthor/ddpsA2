# return the name of the input file to be split into chunks
def get_input_file(input_dir = None, extension = ".ext"):
    if not(input_dir is None):
        return input_dir+"/file" + extension
    return "input/file" + extension


# return the name of the current split file corresponding to the given index
def get_input_split_file(index, input_dir = None, extension = ".ext"):
    if not(input_dir is None):
        return input_dir+"/file_"+ str(index) + extension
    return "input/file_" + str(index) + extension


# return the name of the temporary map file corresponding to the given index
def get_temp_map_file(index, reducer, output_dir = None, extension = ".ext"):
    if not(output_dir is None):
        return output_dir + "/map_file_" + str(index)+"-" + str(reducer) + extension
    return "output/map_file_" + str(index) + "-" + str(reducer) + extension


# return the name of the output file given its corresponding index
def get_output_file(index, output_dir = None, extension = ".out"):
    if not(output_dir is None):
        return output_dir+"/reduce_file_"+ str(index) + extension
    return "output/reduce_file_" + str(index) + extension


# return the name of the output file
def get_output_join_file(output_dir = None, extension = ".out"):
    if not(output_dir is None):
        return output_dir +"/output" + extension
    return "output/output" + extension
