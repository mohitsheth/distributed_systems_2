Problem Statement:
Implement a map reduce algorithm to determine mutual friends given a  list of friends.
Assume input is given  in a file with  friend ID  List of friends   in  each  line.   Consider 20 to 30  friends and the corresponding friends list.
1   2 3  5 
2    1 3  5 7
3    1 2  5 9 
4     1 2 5 7 12 
5     2 3 5 9

Write a program with  2 mapper threads and 2 reduce threads such  that the output files written by the reducer contains for each mutual pair, the list of  common friends. Assume the input file is partitioned into two files each containing  friendID, list of friends.  The mapper threads should output  key value pairs in intermediate files and the reducer should read from the intermediate files  and write into its output file. For example,  a sample of  the  output written by the reducer should be 
[1 2]  [3 5]
[1 3]  [2 5]     
...

You can use a simple naming convention for files:  inp1.txt  inp2.txt;  imf1.txt, imf2.txt; out1.txt, out1.txt.   You can decide on the  partition of the friends list into two files. The main thread should wait for the mapper threads to finish and then execute the reduce threads.

You should submit your map reduce program and all the files


Idea:
My program creates two subprocesses for Mapper and two for Reducer, and reads from two partitioned input files. These numbers can be arbitrarily changed, as long as the input files follow the format "person\tfriend1,friend2...\n" and are named as "inp-<number>.txt". Also, the only files named with this convention in the working directory must be the input files. 

During the run, the mappers output to two intermediate files, int-1.txt, int-2.txt. If you decide to increase the Pool size for mapper (and reducer) then it will generate that many intermediate files. For each line in the input file, the mapper creates all possible (sorted) combinations of friends on the right hand side, and emits the left hand side as a mutual friend of the pair. 

Once the mappers finish executing, the shuffle/sort phase sorts the keys and returns a generator that yields one key and all its associated values to a reducer, as passed by the multiprocessing module. The reducer merely aggregates all the mutual friends of the current pair and emits it to a output file. Output files are named out-1.txt, out-2.txt. If you decide to increase the number of reducers, there will be more output files. A pair will only appear in the sorted order in the output, thus, if (A,B) is in the file, you will not see a (B,A).

Please note that since the two reducers may be called multiple times with different chunks of data (implementation detail of the multiprocessing library), I am appending to the same output file for a particular reducer. Therefore, if you run the program again, make sure the out-* files are deleted first. The intermediate files will be overwritten.

Prerequisites:
I have used the xsorted library to externally sort the keys, thus replicating a "shuffle/sort" phase. While external sorting is not technically required with this dataset, I have tried to make it compatible with scaling as much as possible, and thus mainly work with generators, etc. to avoid holding the entire data in memory at once. One thing to note is, for a single key, all its associated values will be in memory at the same time, in order to pass it to the reducer, which was unavoidable, but can theoretically cause problems depending on system specifications.

There will also have to be at least one inp-<number>.txt file in the working directory.

Also note, the number of mappers and reducers are equal. While this can be changed, I have followed the question's requirements. This number can be changed by passing a command line parameter, and the default is 2.

Execution:
python ds2.py <number of mappers/reducers>
