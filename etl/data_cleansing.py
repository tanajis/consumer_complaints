from pyspark.sql import SparkSession
import json
import csv
from pyspark.sql.functions import col

def readDBConfig(dbConfigPath,db_id):

    f = open(dbConfigPath)
    configData = json.load(f)
    return configData[db_id]

def readConfig(ConfigPath):

    f = open(ConfigPath)
    configData = json.load(f)
    return configData

def applyColMapping(input_df,mapping_path):
    try:
        f = open(mapping_path,'r')
        output_df = input_df
        col_list = []
        for row in csv.DictReader(f):
            
            col_list.append(row['tgt_name'])

            # Rename column and change data types.
            output_df = output_df.withColumn(row['tgt_name'],output_df[row['src_name']]\
                .cast("{}".format(row['tgt_type']))
                )

        # Take only required columns.
        output_df = output_df.select(col_list)
        
        return output_df
    except Exception as e:
        print('Error in applyColMapping')
        raise Exception(str(e))

def processTable(table_info):

    try:
        # Read source csv 
        src_file_path = table_info['src_path']
        src_df = spark.read.csv(header=True,path=src_file_path)
        # Create DIM table

        mapping_path = table_info['mapping_path']
        tgt_df = applyColMapping(src_df,mapping_path)
        
        # Save table
        tgt_df.write.csv(
            path=table_info['tgt_path']+'/'+table_info['table_name']+'.csv'\
            ,mode='overwrite'\
            ,header=True)

        print('Written data to target successfully!!')
    except Exception as e:
        print(str(e))

if __name__ == "__main__":

    try:

        # Create Spark Session 
        global spark
        spark = SparkSession.builder.getOrCreate()

        # Set log level
        spark.sparkContext.setLogLevel("ERROR")
        
        # Read Config File
        configData = readConfig('/home/tms/projects/code_consumer_complaints/configs/etl_config.json')
        # dim_company
        table_info = {}
        table_info['src_path'] = configData['data_cleansing']['src_path']
        table_info['tgt_path'] = configData['data_cleansing']['tgt_path']

        table_info['table_name'] = 'dim_company'
        table_info['dbConfigPath'] = "/home/tms/projects/code_consumer_complaints/configs/dbConfig.json"
        table_info['mapping_path'] = "/home/tms/projects/code_consumer_complaints/mappings/mapping_dim_company.csv"
        
        processTable(table_info)


        # dim_product
        table_info = {}
        table_info['src_path'] = configData['data_cleansing']['src_path']
        table_info['tgt_path'] = configData['data_cleansing']['tgt_path']

        table_info['table_name'] = 'dim_product'
        table_info['dbConfigPath'] = "/home/tms/projects/code_consumer_complaints/configs/dbConfig.json"
        table_info['mapping_path'] = "/home/tms/projects/code_consumer_complaints/mappings/mapping_dim_product.csv"
        
        processTable(table_info)

        
        # fact_compaints
        table_info = {}
        table_info['src_path'] = configData['data_cleansing']['src_path']
        table_info['tgt_path'] = configData['data_cleansing']['tgt_path']

        table_info['table_name'] = 'fact_compaints'
        table_info['dbConfigPath'] = "/home/tms/projects/code_consumer_complaints/configs/dbConfig.json"
        table_info['mapping_path'] = "/home/tms/projects/code_consumer_complaints/mappings/mapping_fact_compaints.csv"

        processTable(table_info)

    except Exception as e:
        print('Error in main',str(e))