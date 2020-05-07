#@Auther : Tanaji Sutar
#This file is created to start the hadoop cluster
#Install hadoop from https://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/
# To run this file use ./starthadoop.sh


cd $HADOOP_HOME/sbin
./start-yarn.sh
./start-dfs.sh

#You can access the hadoop from web browser as well
#HDFS : http://localhost:50070/
#RM UI: http://localhost:8088/


#Start Spark
cd $SPARK_HOME/sbin
./start-all.sh