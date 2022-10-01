# Pre-requisites - 

Wiki articles edge data needs to be present of the hdfs file system. The below command can be used to copy the files/folder from local to hdfs : 

hadoop fs -put -r /path/to/local/folder hdfs://<hdfs name node ip>:9000/path/to/dest/folder

# Instructions to run -

Assuming spark is present in the same folder, the below command can be used to run the spark job :

./spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://<spark master ip>:7077 pagerank_1.py -i hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* -o hdfs://10.10.1.1:9000/path/to/output/folder

OR

if spark bin is present in PATH,

spark-submit --master spark://<spark master ip>:7077 pagerank_1.py -i hdfs://<hdfs name node ip>:9000/path/to/dest/folder/* -o hdfs://10.10.1.1:9000/path/to/output/folder

OR

we can also directly call run.sh <spark master url> <hdfs source path> <hdfs output path>, assuming spark bin is present in PATH.
