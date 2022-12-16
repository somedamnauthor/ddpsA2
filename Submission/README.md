# A Python Implementation of MapReduce Using Processes

### Usage

<ul>
<li>Place the data/intended input files in the input/data directory
<li>The experiments use the erwik9 dataset. This can be obtained using: 
wget http://mattmahoney.net/dc/enwik9.zip
<li>Details on the dataset can be found here - http://mattmahoney.net/dc/textdata.html


#### For the Distributed Implementation

Run the ```44controller.sh``` file with the following command - 

```
sh 44controller.sh <taskfile> <data file> <node list>
```

An example of the command is: 

```
sh 44controller.sh wordcount_example.py large.txt node109 node110 node111 node112 node113 node114
```

#### For the Non-Distributed Implementation using Threads

Run the ```44NDcontroller.sh``` file with the following command - 

```
sh 44controller.sh <taskfile> <data file>
```

An example of the command is: 

```
sh 44NDcontroller.sh wordcount_example.py large.txt
```

#### For the Non-Distributed Implementation using Processes

Run the ```44controller.sh``` file with the following command - 

```
sh 44controller.sh <taskfile> <data file> <node> <node> <node> <node> <node> 
```

An example of the command is: 

```
sh 44controller.sh wordcount_example.py large.txt node109 node109 node109 node109 node109 node109
```

