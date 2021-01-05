import names
from common import save_file, convert_list_to_df
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, IntegerType, StructField, StringType
from datetime import datetime
from essential_generators import DocumentGenerator
import random

class FilesGenerator():
    """Creating 4 Parquet files (each for table) for Twitter Task, which described in the README.md file"""

    USER_COUNT: int = 1000
    MESSAGE_COUNT: int = 2000
    RETWEET_COUNT: int = 3000

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
        print("Creating 4 files for 4 tables...")
        spark = SparkSession.builder.appName('FileGenerator').getOrCreate()

        message_data: tuple = self.message_generator(spark)
        data: list = [
            convert_list_to_df(spark, *self.message_dir_generator(spark)),
            convert_list_to_df(spark, *self.user_dir_generator(spark)),
            convert_list_to_df(spark, *message_data),
            convert_list_to_df(spark, *self.retweet_generator(spark, message_data[0]))
            ]
        save_file(data, self.path)

    def user_dir_generator(self, spark: SparkSession) -> (list, StructType, str):
        """Method generating 1 file for User Dir table

        :param spark: SparkSession
        :return: user_dir table content, table schema and table name
        """
        print("Generating 1 file for User Dir table...")

        schema: StructType = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('FIRST_NAME', StringType(), True),
            StructField('LAST_NAME', StringType(), True)
        ])

        table: list = []
        for user_id in range(self.user_count):
            name: str = names.get_first_name()
            lastname: str = names.get_last_name()
            table_row = (user_id, name, lastname)
            table.append(table_row)

        return table, schema, "user_dir"

    def message_dir_generator(self, spark: SparkSession) -> (list, StructType, str):
        """Method generating 1 file for Message Dir table

        :param spark: SparkSession
        :return: message_dir table content, table schema and table name
        """
        print("Generating 1 file for Message Dir table...")

        schema = StructType([
            StructField('MESSAGE_ID', IntegerType(), True),
            StructField('TEXT', StringType(), True)
        ])

        gen: DocumentGenerator = DocumentGenerator()
        table: list = []
        for message_id in range(self.message_count):
            text: str = gen.word()
            table_row: tuple = (message_id, text)
            table.append(table_row)

        return table, schema, "message_dir"

    def message_generator(self, spark: SparkSession) -> (list, StructType, str):
        """Method generating 1 file for Message table

        :param spark: SparkSession
        :return: message table content, table schema and table name
        """
        print("Generating 1 file for Message table...")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        table: list = []
        for message_id in range(self.message_count):
            user_id: int = int(random.uniform(0, self.user_count))
            table_row: tuple = (user_id, message_id)
            table.append(table_row)

        return table, schema, "message"

    def retweet_generator(self, spark: SparkSession, message_data: list) -> (list, StructType, str):
        """Method generating 1 file for Retweet table

        :param spark: SparkSession
        :param message_data: Messages from the table with schema (USER_ID, MESSAGE_ID)
        :return: retweet table content, table schema and table name
        """
        print("Generating 1 file for Retweet table...")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('SUBSCRIBER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        table: list = []
        for retweet_id in range(self.retweet_count):
            num_message_row: int = int(random.uniform(0, self.message_count))
            message_row: Row = message_data[num_message_row]

            user_id: int = message_row[0]
            message_id: int = message_row[1]

            # find subscriber_id
            subscriber_id: int = int(random.uniform(0, self.user_count))
            while subscriber_id == user_id:
                subscriber_id: int = int(random.uniform(0, self.user_count))

            table_row: tuple = (user_id, subscriber_id, message_id)
            table.append(table_row)

        return table, schema, "retweet"
