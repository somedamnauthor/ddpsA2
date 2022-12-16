# A Python Implementation of MapReduce Using Processes

### Usage

Place the data/intended input files in the ```input/data``` directory

The experiments use the erwik9 dataset. This can be obtained using: 

```wget http://mattmahoney.net/dc/enwik9.zip```

Details on the dataset can be found here - http://mattmahoney.net/dc/textdata.html


#### For the Distributed Implementation

Run the ```44controller.sh``` file with the following command - 

```sh 44controller.sh <taskfile> <data file> <node list>```

An example of the command is: 

```sh 44controller.sh wordcount_example.py large.txt node109 node110 node111 node112 node113 node114```


