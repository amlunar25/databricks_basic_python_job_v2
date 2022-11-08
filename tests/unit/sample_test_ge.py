from basic_python_job.tasks.sample_etl_task import SampleETLTask
from basic_python_job.datasets import customer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pathlib import Path
from basic_python_job.utilities import functions
import logging, re

def test_jobs_ge(spark: SparkSession, tmp_path: Path):
    logging.info("Testing the ETL job to include etl columns")

    #CONFIG
    
    common_config = {"database": "default", "table": "default_table"}
    test_etl_config = {"output": common_config}


    #RUN TEST
    
    #CREATING TEST DATAFRAME
    logging.info("Creating ETL Sample Task")
    etl_job = SampleETLTask(spark, test_etl_config)

    logging.info("Running Transformation with Test Data")
    test_df = etl_job.spark.read.json(
                etl_job.sc.parallelize(
                    customer.get_initial_records()
                    )
                ) 
    output_df = etl_job._transform(test_df)

    logging.info("Checking for Columns")
    col = ['customer_id','first_name','email','gender']
    functions.exists_columns(col,output_df)

    logging.info("Checking for Columns without nulls")
    col = ['customer_id','first_name']
    functions.columns_without_null(col,output_df)

    logging.info("Checking for Columns without nulls")
    col = ['customer_id']
    functions.columns_values_between(col,output_df,10000,50000)
    
    logging.info("Checking for Columns Types")
    dic = {'customer_id':'IntegerType','first_name':'StringType'}
    functions.validate_dtype(dic,output_df)

    logging.info("Testing the ETL job - done")