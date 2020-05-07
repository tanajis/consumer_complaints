from pyspark.sql import SparkSession
import json
import csv
from pyspark.sql.functions import col
from dependencies.dqRules import DQCheck

def readConfig(ConfigPath):

    f = open(ConfigPath)
    configData = json.load(f)
    return configData

def processTable(table_info):

    try:
        # Read source csv 
        src_file_path = table_info['src_path']
        src_df = spark.read.csv(header=True,path=src_file_path)

        src_df = src_df.limit(100)
        temp = DQCheck()

        # Apply DQ_NullCheck
        good,bad = temp.isNotNull(src_df,['Company','State','Sub-product'])

        # Apply DQ_MAXLENGTH
        #good,bad = temp.hasMaxLength(good,'Company',20)

        #Save good Data and bad data as hive table.

        good.write.csv(path=table_info['tgt_path']+'/good_data.csv',mode='overwrite',header=True)
        bad.write.csv(path=table_info['tgt_path']+'/bad_data.csv',mode='overwrite',header=True)

    except Exception as e:
        print(str(e))

if __name__ == "__main__":

    # Create Spark Session 
    global spark
    spark = SparkSession.builder.appName('DQ-Layer').getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("ERROR")
    

    # Read Config File
    configData = readConfig('/home/tms/projects/code_consumer_complaints/configs/etl_config.json')
    # dim_company
    table_info = {}
    table_info['src_path'] = configData['data_quality']['src_path']
    table_info['tgt_path'] = configData['data_quality']['tgt_path']
    processTable(table_info)
