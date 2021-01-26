# 1. Add data & config folders to hadoop namenode & hdfs.
docker exec -it docker-hive_namenode_1 /bin/bash -c "
  mkdir -p Data
  mkdir -p Config
"

docker exec -it docker-hive_namenode_1 /bin/bash -c "
  hdfs dfs -mkdir /Data
  hdfs dfs -mkdir /Config
"

# 2. Copy-paste data to hadoop namenode
docker cp "../Data/." docker-hive_namenode_1:/Data/

# 3. Add data to Hadoop Distributed File System
docker exec -it docker-hive_namenode_1 /bin/bash -c "hdfs dfs -put /Data/. /Data"

# 4. Check creation of data files
docker exec -it docker-hive_namenode_1 /bin/bash -c "hdfs dfs -cat /Data/1997/1/1.csv | head"

# 4. Hive set-up
# 4.1 If not already done, run snippet below to add partition statements in "create_hive_table.hql" 
# for year in `seq 1987 2008`
#   do
#     for month in `seq 1 12`
#       do
#         echo "ALTER TABLE flights ADD PARTITION(yeard=$year, monthd=$month) LOCATION \"hdfs:/Data/$year/$month/\";" >> create_hive_table.hql
#       done
#   done

# 4.2 Add create_hive_table.hql to hdfs
docker cp create_hive_table.hql docker-hive_namenode_1:/Config/
docker exec -it docker-hive_namenode_1 /bin/bash -c "hdfs dfs -put -f /Config/create_hive_table.hql /Config/"

# 4.3 Create hive table as specified in create_hive_table.hql
docker exec -it docker-hive_hive-server_1 /bin/bash -c "hive -f hdfs:/Config/create_hive_table.hql"
