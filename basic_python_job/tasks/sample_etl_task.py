from basic_python_job.common import Task
from basic_python_job.datasets import customer
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


class SampleETLTask(Task):
    def __init__(self, spark=None, init_conf=None):
        super().__init__(spark, init_conf)
        #Get Job Configuration
        self.db = self.conf["output"].get("database", "default")
        self.table = self.conf["output"]["table"]

    def _extract(self) -> DataFrame:
        df = self.spark.read.json(
            self.sc.parallelize(
                customer.get_initial_records()
                )
            )
        return df

    def _transform(self,df) -> DataFrame:
        self.logger.info("Addubg reqired ETL metadata fields.")
        df =  df.withColumn('etl_created_dt', lit(datetime.now().isoformat())) \
                .withColumn('etl_task_name', lit(type(self).__name__))
        return df

    def _load(self,df):
        df.write.format("delta").mode("append").saveAsTable(f"{self.db}.{self.table}")
        self.logger.info("Dataset successfully written")
        pass

    def launch(self):
        self.logger.info("Launching sample ETL task")
        self._load(
            self._transform(
                self._extract()
            )
        )
        self.logger.info("Sample ETL task finished!")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = SampleETLTask()
    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
