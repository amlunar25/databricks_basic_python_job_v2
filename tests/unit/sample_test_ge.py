from basic_python_job.tasks.sample_etl_task import SampleETLTask
from basic_python_job.datasets import customer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pathlib import Path
from basic_python_job.uti
import logging, re

def test_jobs(spark: SparkSession, tmp_path: Path):
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

    logging.info("Checking for Null Values")


    logging.info("Checking for ISO Format")
    def is_not_iso_format(row):
        return None == re.match('^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}(?:\.\d*)?)((-(\d{2}):(\d{2})|Z)?)$',row.etl_created_dt)
    
    rdd_not_iso_format = output_df.select('etl_created_dt').rdd
    rdd_not_iso_format = rdd_not_iso_format.filter(lambda x: is_not_iso_format(x))
    assert rdd_not_iso_format.count() == 0 

    logging.info("Testing the ETL job - done")