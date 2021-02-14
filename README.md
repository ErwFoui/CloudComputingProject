This repository contains the code developped for completion of the Cloud Computing Specialization capstone, which consists in analyzing flight data from the US Bureau of Transportation Statistics between 1987 and 2008, in two distinct fashions: batch processing and streaming.

Task #1 is carried out by using the docker-hive image with PrestoDB connector provided by https://hub.docker.com/r/bde2020/hive/. To download datasets concurrently, add them to the HDFS and load your Hive table, run `bash get_data.sh` and then `bash setup_hdfs_hive.sh`.

Task #2 is carried out by using PySpark to extract and process data, with optional saving to Cassandra (see init_cassandra.cql file). The data can be queried directly in Cassandra with CQL statements, or from PySpark DataFrames in a SQLContext with Hive-like statements.
