# elasticsearch-index-spark-offline

## This project is based on below two projects

[elasticsearch-lambda project] (https://github.com/MyPureCloud/elasticsearch-lambda) and [elasticsearch-spark-offline project] (https://github.com/IgorBerman/elasticsearch-spark-offline).

elasticsearch-spark-offline project has few issues which are fixed in this project.

1) Not able to generate correct mapping file on index root location
2) Appending timestamp at the end of index name doesn't match with index name in template json hence Elasticsearch creates its default JSON in metadata file matching with input data which has missing nested tags in it
3) Used custom partitioner with same hash function as Elasticsearch uses internally instead of Spark's
 hash partitioner which allows to map all the data going to one partition to only one shard, this shard directory will be moved to actual snapshot repo location at
 the end

## How it works
1) Spark reads raw data (JSON or plain text) sitting on S3
2) Spark creates PairRDD from input data where key is the Random number (between 0 to number of shards) and value is the 
raw data read in step #1 (You can also use any field as a key within your raw data if that guarantees to equally distributed data 
across all partitions)
2) Spark uses Custom Partitioner to partition the PairRDD into same number as shards we want to build within an Index.
3) Custom Partitioner uses same hash function as Elasticsearch uses internally to decide given document is going to store on which
shard.
4) Spark executes a function on each partition where
 a) Function creates in-memory elasticsearch node instance
 b) Create an Index on node instance with the number of shards equal to number of shards we want to build in the final output
 c) Iterate through each element into pairRDD and creates bulk insert request
 d) while creating bulk insert request, use the id of an pairRDD as routing so all the values in that partition will go to the same   Shard and other shards will remain empty
 d) Submit list of values as bulkupload while inserting into Index created in #b
 e) Once all the values submitted to the Index, snapshots that index to the intermediate location
 f) Finally move the directory with max size (the only shard which has all the values) from intermediate location to the final snapshot destination
 g) Also move the manifest file generated in each partition to the 'indexes' location within the final destination. This manifest file has template mapping json file provided by user
5) On the Driver, creates an empty index with final number of shards as #4 and snapshots it too
6) Uploads metadata of the last snapshot to final destination as a top level manifest files

## How to Run:

Please make sure you have right JSON mapping file under s3 bucket if you run it on cluster or under resource folder if you run
this functionality as unit test.

### You can kick off index creation job on cluster by below command:
```
/usr/lib/spark/bin/spark-submit --class com.dy.spark.elasticsearch.ESIndexSnapshotJob  --driver-memory=20g  --num-executors=20 --executor-memory=45g --conf spark.master=yarn --conf spark.submit.deployMode=cluster --conf spark.executor.heartbeatInterval=30 
s3://<PATH to Spark executable Jar>
s3://<PATH to Input JSON>
s3://<PATH to final output of snapshot repo>
<Number of shards>
s3://<PATH to mapping JSON temploate file>
<Index_name>
<Index_type>
``` 
Below is the description of command line arguements:

1) s3 location of input files
2) final output snapshot repo location
3) Number of Shards
4) s3 path where JSON template mapping file located
5) Index Name
6) Index Type

Note: #5 and #6 must match with mapping name in template JSON file.

### To debug and run this functionality quickly on your local machine, you can also read input and write
final output to local directory:
Run src/test/ESIndexShardSnapshotCreatorTest class with below arguments:
```
Java ESIndexShardSnapshotCreatorTest 
<local path to user input JSON dump>
/tmp/es-test/my_backup_repo/
<Number of shards>
<Name of the json mapping file available in /src/test/resources/com/parmarh/elasticsearch>
<Index_name>
<Index_type>
```


