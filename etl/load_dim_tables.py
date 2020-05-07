from pyspark.sql import SparkSession
import json
import csv
from pyspark.sql.functions import col

def readDBConfig(dbConfigPath,db_id):

    f = open(dbConfigPath)
    configData = json.load(f)
    return configData[db_id]


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
        src_file_path = table_info['src_file_path']
        src_df = spark.read.csv(header=True,path=src_file_path)

        src_df = src_df.limit(5)

        # Create DIM table

        mapping_path = table_info['mapping_path']
        tgt_df = applyColMapping(src_df,mapping_path)
        
        
        print('check 1')
        # Save to DWH
        dbConfigPath = table_info['dbConfigPath']
        db_id = table_info['db_id']
        print('check 2')
        configData = readDBConfig(dbConfigPath,db_id)
        print('check 4')
        URL = configData['url']
        USER = configData['user']
        PASSWORD = configData['password']
        DBTABLE = configData['dbtable']
        DRIVER = configData['driver']

        """
        tgt_df.write\
            .format('jdbc')\
            .option('url',URL)\
            .option('dbtable',DBTABLE)\
            .option('user',USER)\
            .option('password',PASSWORD)\
            .option('driver',DRIVER)\
            .mode('append')\
            .save()
        """
        tgt_df.printSchema()
        tgt_df.show(5)

    except Exception as e:
        print(str(e))

if __name__ == "__main__":

    # Create Spark Session 
    global spark
    spark = SparkSession.builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("ERROR")
    
    # dim_company
    table_info = {}
    table_info['table_name'] = 'dim_company'
    table_info['dbConfigPath'] = "/home/tms/tmsCodeGround/awsstudy/consumer_complaints/configs/dbConfig.json"
    table_info['db_id'] = 'postgres_datamart'
    table_info['src_file_path'] = '/home/tms/tmsCodeGround/awsstudy/consumer_complaints/data/complaints.csv'
    table_info['mapping_path'] = "/home/tms/tmsCodeGround/awsstudy/consumer_complaints/mappings/mapping_dim_company.csv"
    processTable(table_info)

    # dim_product
    table_info = {}
    table_info['table_name'] = 'dim_product'
    table_info['src_file_path'] = '/home/tms/tmsCodeGround/awsstudy/consumer_complaints/data/complaints.csv'
    table_info['mapping_path'] = "/home/tms/tmsCodeGround/awsstudy/consumer_complaints/mappings/mapping_dim_product.csv"
    processTable(table_info)

    """
    # fact_compaints
    table_info = {}
    table_info['table_name'] = 'fact_compaints'
    table_info['src_file_path'] = '/home/tms/tmsCodeGround/awsstudy/consumer_complaints/data/complaints.csv'
    table_info['mapping_path'] = "/home/tms/tmsCodeGround/awsstudy/consumer_complaints/mappings/mapping_fact_compaints.csv"
    processTable(table_info)
    """