
#Install hadoop from https://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/

cd $HADOOP_HOME/sbin
./stop-yarn.sh
./stop-dfs.sh

#Start Spark
cd $SPARK_HOME/sbin
./stop-all.sh