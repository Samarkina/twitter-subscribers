import names
from common import save_file
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.dataframe import DataFrame


class FilesGenerator():
    """Creating 4 Parquet files (each for table) for Twitter Task, which described in the README.md file"""

    COUNT: int = 10

    def __init__(self,
                 count_strings: int = COUNT):
        self.count_strings = count_strings

    def generate(self):
        print("I will generate 4 files for 4 tables")
        spark = SparkSession.builder.appName('FileGenerator').getOrCreate()

        self.user_dir_generator(spark)
        self.message_dir_generator()
        self.message_generator()
        self.retweet_generator()

    def user_dir_generator(self, spark):
        """Method generating 1 file for User Dir table

        :return:
        """
        print("I will generate 1 file for User Dir table")

        schema = StructType([
            StructField('USER_ID', StringType(), True),
            StructField('FIRST_NAME', StringType(), True),
            StructField('LAST_NAME', StringType(), True)
        ])

        table: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
        for i in range(self.count_strings):
            name: str = names.get_first_name()
            lastname: str = names.get_last_name()
            table: DataFrame = table.union(spark.createDataFrame([{'USER_ID': i, 'FIRST_NAME': name, 'LAST_NAME': lastname}]))

        save_file(table, "user_dir")

    def message_dir_generator(self):
        """Method generating 1 file for Message Dir table

        :return:
        """
        print("I will generate 1 file for Message Dir table")

    def message_generator(self):
        """Method generating 1 file for Message table

        :return:
        """
        print("I will generate 1 file for Message table")

    def retweet_generator(self):
        """Method generating 1 file for Retweet table

        :return:
        """
        print("I will generate 1 file for Retweet table")


