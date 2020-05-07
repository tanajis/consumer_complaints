from pyspark.sql import SparkSession
import json
import csv
from pyspark.sql.functions import col,lit
import sys


class DQCheck:

    def __init__(self):
        pass

    def remove_ducplicates(self):
        pass
    
    def isNotNull(self,input_df,col_list):
        """
        Seperate good data and bad data dataframes and return.
        Parameter:
            input_df         :input dataframe 
            col_list         :List of columns on which rules is to be applied.
        Return:
            good_data_df     :Datframe of accepted data.
            bad_data_df      :Datframe of rejected data.
        """
        try:
            good_data_df = input_df
            bad_data_df = input_df.limit(0)
            bad_data_df = bad_data_df.withColumn('reason' ,lit('DQRULE:ISNOTNULL'))
            for column in col_list:
                temp_bad_data_df  = good_data_df.filter(good_data_df["{}".format(column)].isNull() )
                good_data_df = good_data_df.filter(good_data_df["{}".format(column)].isNotNull() )
                
                # Add to bad data
                temp_bad_data_df = temp_bad_data_df.withColumn('reason',lit("DQRULE:ISNOTNULL COLUMN:{}".format(column)))
                bad_data_df = bad_data_df.union(temp_bad_data_df)
            
            return good_data_df,bad_data_df
        except Exception as e:
            print('Error in DQCheck')
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            raise Exception(str(e))

    def hasMaxLength(self,input_df,col_name,max_length):
        """
        Seperate good data and bad data dataframes and return.
        Parameter:
            input_df         :input dataframe 
            col_list         :List of varchar columns on which rules is to be applied.
        Return:
            good_data_df     :Datframe of accepted data.
            bad_data_df      :Datframe of rejected data.
        """
        try:
            bad_data_df  = input_df.filter("length({}) > {}".format(col_name,max_length))
            good_data_df = input_df.filter("length({}) <= {}".format(col_name,max_length))
                
            # Apply Rule
            bad_data_df = bad_data_df.withColumn('reason',lit("DQRULE:hasMaxLength COLUMN:{} EXPECTED LENGTH {}".format(col_name,max_length)))
            
            return good_data_df,bad_data_df
        except Exception as e:
            print('Error in DQCheck.hasMaxLength')
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))
            raise Exception(str(e))
