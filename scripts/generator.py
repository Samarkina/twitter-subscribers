import names

from common import save_file
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField, StringType
from pyspark.sql.dataframe import DataFrame
from datetime import datetime
from essential_generators import DocumentGenerator
import random


class FilesGenerator():
    """Creating 4 Parquet files (each for table) for Twitter Task, which described in the README.md file"""

    USER_COUNT: int = 10
    MESSAGE_COUNT: int = 20
    RETWEET_COUNT: int = 20

    # datetime object containing current date and time
    now: datetime = datetime.now()
    curr_date: str = now.strftime("%Y-%m-%d")
    curr_time: str = now.strftime("%H-%M-%S")

    path = ".././data/" + curr_date + "/recieved=" + curr_time + "/"

    def __init__(self,
                 user_count: int = USER_COUNT,
                 message_count: int = MESSAGE_COUNT,
                 retweet_count: int = RETWEET_COUNT,
                 path: str = path):
        self.user_count = user_count
        self.message_count = message_count
        self.retweet_count = retweet_count
        self.path = path

    def generate(self):
        print("I will generate 4 files for 4 tables")
        spark = SparkSession.builder.appName('FileGenerator').getOrCreate()

        data: list = [self.user_dir_generator(spark),
                      self.message_dir_generator(spark),
                      self.message_generator(spark),
                      self.retweet_generator(spark)]

        save_file(data, self.path)

    def user_dir_generator(self, spark: SparkSession) -> (DataFrame, str):
        """Method generating 1 file for User Dir table

        :param spark: SparkSession
        :return: user_dir table content and table name
        """
        print("I will generate 1 file for User Dir table")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('FIRST_NAME', StringType(), True),
            StructField('LAST_NAME', StringType(), True)
        ])

        table: DataFrame = spark.createDataFrame([], schema)
        for i in range(self.user_count):
            name: str = names.get_first_name()
            lastname: str = names.get_last_name()
            table: DataFrame = table.union(spark.createDataFrame(
                [{'USER_ID': i, 'FIRST_NAME': name, 'LAST_NAME': lastname}])
            )

        return table, "user_dir"

    def message_dir_generator(self, spark) -> (DataFrame, str):
        """Method generating 1 file for Message Dir table

        :param spark: SparkSession
        :return: message_dir table content and table name
        """
        print("I will generate 1 file for Message Dir table")

        schema = StructType([
            StructField('MESSAGE_ID', IntegerType(), True),
            StructField('TEXT', StringType(), True)
        ])

        gen: DocumentGenerator = DocumentGenerator()
        table: DataFrame = spark.createDataFrame([], schema)
        for i in range(self.message_count):
            text: str = gen.word()
            table: DataFrame = table.union(spark.createDataFrame(
                [{'MESSAGE_ID': i, 'TEXT': text}])
            )

        return table, "message_dir"

    def message_generator(self, spark) -> (DataFrame, str):
        """Method generating 1 file for Message table

        :param spark: SparkSession
        :return: message table content and table name
        """
        print("I will generate 1 file for Message table")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        table: DataFrame = spark.createDataFrame([], schema)
        for j in range(self.message_count):
            i: int = int(random.uniform(0, self.user_count))
            table: DataFrame = table.union(spark.createDataFrame(
                [{'USER_ID': i, 'MESSAGE_ID': j}])
            )

        return table, "message"

    def retweet_generator(self, spark) -> (DataFrame, str):
        """Method generating 1 file for Retweet table

        :param spark: SparkSession
        :return: retweet table content and table name
        """
        print("I will generate 1 file for Retweet table")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('SUBSCRIBER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        table: DataFrame = spark.createDataFrame([], schema)
        for r in range(self.retweet_count):
            i: int = int(random.uniform(0, self.user_count))
            while True:
                j: int = int(random.uniform(0, self.user_count))
                if j != i:
                    break
            m: int = int(random.uniform(0, self.message_count))
            table: DataFrame = table.union(spark.createDataFrame(
                [{'USER_ID': i, 'SUBSCRIBER_ID': j, 'MESSAGE_ID': m}])
            )

        return table, "retweet"
