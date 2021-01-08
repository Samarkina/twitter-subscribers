from scripts.common import save_file, convert_list_to_df, names_generator, get_random_string
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField, StringType
from datetime import datetime
import random


class FilesGenerator():
    """Creating 4 Parquet files (each for table) for Twitter Task, which described in the README.md file"""

    USER_COUNT: int = 20
    MESSAGE_COUNT: int = 30
    RETWEET_COUNT: int = 20
    RETWEET_COUNT_WAVE_2: int = 30

    # datetime object containing current date and time
    now: datetime = datetime.now()
    curr_date: str = now.strftime("%Y-%m-%d")
    curr_time: str = now.strftime("%H-%M-%S")

    path = ".././retweets-analytics/src/main/resources/" + curr_date + "/received=" + curr_time + "/"

    def __init__(self,
                 user_count: int = USER_COUNT,
                 message_count: int = MESSAGE_COUNT,
                 retweet_count: int = RETWEET_COUNT,
                 retweet_count_wave_2: int = RETWEET_COUNT_WAVE_2,
                 path: str = path):
        self.user_count = user_count
        self.message_count = message_count
        self.retweet_count = retweet_count
        self.retweet_count_wave_2 = retweet_count_wave_2
        self.path = path

    def generate(self):
        """Generating content for user_dir, message_dir, message, retweet, retweet_second_wave tables.
        And executing function with saving the tables to Parquet file.
        :return:
        """
        print("Creating Parquet files for tables...")
        spark: SparkSession = SparkSession.builder \
            .master("local[2]") \
            .appName('FileGenerator') \
            .getOrCreate()

        message_data: tuple = self.message_generator()
        retweet_data: tuple = self.retweet_generator(message_data[0])
        data: list = [
            convert_list_to_df(spark, *self.message_dir_generator()),
            convert_list_to_df(spark, *self.user_dir_generator()),
            convert_list_to_df(spark, *message_data),
            convert_list_to_df(spark, *retweet_data),
            convert_list_to_df(spark, *self.retweet_generator_wave_2(retweet_data[0])),
            ]
        save_file(data, self.path)

    def user_dir_generator(self) -> (set, StructType, str):
        """Method generating 1 file for User Dir table

        :return: user_dir table content, table schema and table name
        """
        print("Generating 1 file for User Dir table...")

        schema: StructType = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('FIRST_NAME', StringType(), True),
            StructField('LAST_NAME', StringType(), True)
        ])

        table: set = set()
        for user_id in range(self.user_count):
            name, lastname = names_generator()
            table_row = (user_id, name, lastname)
            table.add(table_row)

        return table, schema, "user_dir"

    def message_dir_generator(self) -> (set, StructType, str):
        """Method generating 1 file for Message Dir table

        :return: message_dir table content, table schema and table name
        """
        print("Generating 1 file for Message Dir table...")

        schema = StructType([
            StructField('MESSAGE_ID', IntegerType(), True),
            StructField('TEXT', StringType(), True)
        ])

        table: set = set()
        for message_id in range(self.message_count):
            text: str = get_random_string(random.randint(3, 25))
            table_row: tuple = (message_id, text)
            table.add(table_row)

        return table, schema, "message_dir"

    def message_generator(self) -> (set, StructType, str):
        """Method generating 1 file for Message table

        :return: message table content, table schema and table name
        """
        print("Generating 1 file for Message table...")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        table: set = set()
        for message_id in range(self.message_count):
            user_id: int = int(random.uniform(0, self.user_count))
            table_row: tuple = (user_id, message_id)
            table.add(table_row)

        return table, schema, "message"

    def retweet_generator(self, message_data: set) -> (set, StructType, str):
        """Method generating 1 file for Retweet table

        :param message_data: Messages from the table with schema (USER_ID, MESSAGE_ID)
        :return: retweet table content, table schema and table name
        """
        print("Generating 1 file for Retweet table...")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('SUBSCRIBER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])
        message_data_list: list = list(message_data)
        table: set = set()
        while len(table) < self.retweet_count:
            message_row_id: int = int(random.uniform(0, self.message_count))
            message_row: tuple = message_data_list[message_row_id]

            user_id: int = message_row[0]
            message_id: int = message_row[1]

            # find subscriber_id
            subscriber_id: int = int(random.uniform(0, self.user_count))
            while subscriber_id == user_id:
                subscriber_id: int = int(random.uniform(0, self.user_count))

            table_row: tuple = (user_id, subscriber_id, message_id)
            table.add(table_row)

        return table, schema, "retweet"

    def retweet_generator_wave_2(self, retweet_data: set) -> (set, StructType, str):
        """Method generating 1 file for Retweet table Wave 2
        "Later, every subscriber's subscriber does retweet too."

        :param retweet_data: Retweets data from the table with schema (USER_ID, SUBSCRIBER_ID, MESSAGE_ID)
        :return: retweet table content for wave 2, table schema and table name
        """
        print("Generating 1 file for Retweet table for second wave...")

        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('SUBSCRIBER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        retweet_data_list: list = list(retweet_data)
        table: set = set()
        while len(table) < self.retweet_count_wave_2:
            retweet_row_id: int = int(random.uniform(0, self.retweet_count))
            retweet_row: tuple = retweet_data_list[retweet_row_id]

            user_id: int = retweet_row[0]
            old_subscriber_id: int = retweet_row[1]
            message_id: int = retweet_row[2]

            # find subscriber_id
            new_subscriber_id: int = int(random.uniform(0, self.user_count))
            while new_subscriber_id == user_id or \
                    new_subscriber_id == old_subscriber_id:
                new_subscriber_id: int = int(random.uniform(0, self.user_count))

            table_row: tuple = (user_id, new_subscriber_id, message_id)
            table.add(table_row)

        return table, schema, "retweet_second_wave"
