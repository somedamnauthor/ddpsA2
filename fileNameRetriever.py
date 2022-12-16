def get_filename(input_dir = None, ext = ".ext"):

    """
    Function to retrieve the name of the primary input file, given an input directory
    """

    # Navigate to input directory and access the specific file
    if not(input_dir is None):
        return input_dir+"/file" + ext

    # Input directory is 'input' by default
    return "input/file" + ext


def get_split_filename(index, input_dir = None, extension = ".ext"):

    """
    These files are output by the mapper. This function accesses the corresponding files
    """

    # Navigate to input directory and access the specific file
    if not(input_dir is None):
        return input_dir+"/file_"+ str(index) + extension

    # Input directory is 'input' by default  
    return "input/file_" + str(index) + extension


def get_intermediate_file(index, reducer, output_dir = None, extension = ".ext"):

    """
    These files are output by the mapper as intermediate files. This function accesses the corresponding files
    """

    # Navigate to output directory and access the specific file
    if not(output_dir is None):
        return output_dir + "/map_file_" + str(index)+"-" + str(reducer) + extension

    # Output directory is 'output' by default 
    return "output/map_file_" + str(index) + "-" + str(reducer) + extension



def get_reduce_filename(file_index, output_dir = None, ext = ".out"):

    """
    These files are output by the reducer as intermediate files. This function accesses the corresponding files
    """

    # Navigate to output directory and access the specific file
    if not(output_dir is None):
        return output_dir+"/reduce_file_"+ str(file_index) + ext

    # Output directory is 'output' by default 
    return "output/reduce_file_" + str(file_index) + ext


def get_output_filename(out_directory = None, ext = ".out"):

    """
    This is the final output file. This function accesses the corresponding file
    """
    
    # Navigate to output directory and access the specific file
    if not(out_directory is None):
        return out_directory +"/output" + ext

    # Output directory is 'output' by default 
    return "output/output" + ext