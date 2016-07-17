
In the zip file, we are submitting the data related to all three stock exchanges in three folders, London related, NY related and Bombay related. These folders have the input datasets, map reduce code we wrote for profiling, output we got after profiling, hive output and index values of the respective stock exchanges. 

The complete source code of the project is in the src folder. 

Output of the final analytic, the values of correlations is in the folder named correlations. 

Steps of execution: 

Profiling:

The input data of three cities should be given as input to respective profiling codes. after this step, we have three folders with filtered data. 

Hive for computing market capitalization:

Run the code for hive, HiveCreateTable.java by giving the profiling output as input path to this file and add all the jars that are provided in the jars for hive folder. This creates tables in hive. Then run HiveWriteTable.java. This writes table data to files. Put all the files in a single directory. 

Computing Index:

This directory is given as input to the next map reduce code that is in index calculation folder. The output part-r-00000 file contains index values of the stock market for each input day. 

Correlation: 

Put the index files of any given pair of stock exchanges in a directory and give this as input to correlation mapreduce job. Create a project with 5 files in the correlation folder to run this map reduce code. This comprises of 2 map reduce jobs. The output1 folder contains the values of correlation of those stock exchanges for each year in the dataset.  


