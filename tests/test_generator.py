from typing import NoReturn
import unittest
from datetime import datetime
from pyspark.sql.types import StructType, IntegerType, StringType, StructField

from scripts.generator import FilesGenerator


class FilesGeneratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(FilesGeneratorTest, cls).setUpClass()
        cls.errorMessage: str = "The answer {0} should be {1}"

        USER_COUNT: int = 10
        MESSAGE_COUNT: int = 20
        RETWEET_COUNT: int = 30
        RETWEET_COUNT_WAVE_2: int = 25

        # datetime object containing current date and time
        now: datetime = datetime.now()
        curr_date: str = now.strftime("%Y-%m-%d")
        curr_time: str = now.strftime("%H-%M-%S")

        path = ".././tests/data/" + curr_date + "/received=" + curr_time + "/"

        cls.files_generator: FilesGenerator = FilesGenerator(user_count=USER_COUNT,
                                                             message_count=MESSAGE_COUNT,
                                                             retweet_count=RETWEET_COUNT,
                                                             retweet_count_wave_2=RETWEET_COUNT_WAVE_2,
                                                             path=path)

    def test_user_dir_generator(self) -> NoReturn:
        table: list = [(0, "Tanya", "Smith")]
        schema: StructType = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('FIRST_NAME', StringType(), True),
            StructField('LAST_NAME', StringType(), True)
        ])

        correct_answer: tuple = (table, schema, "user_dir")
        answer: tuple = self.files_generator.user_dir_generator()

        answer_user_id = type(answer[0][0][0])
        answer_first_name = type(answer[0][0][1])
        answer_last_name = type(answer[0][0][2])

        correct_answer_user_id = type(correct_answer[0][0][0])
        correct_answer_first_name = type(correct_answer[0][0][1])
        correct_answer_last_name = type(correct_answer[0][0][2])

        answer_schema = answer[1]

        answer_table_name = answer[2]
        correct_answer_table_name = correct_answer[2]

        # types of the table
        assert answer_user_id == correct_answer_user_id, \
            self.errorMessage.format(answer_user_id, correct_answer_user_id)
        assert answer_first_name == correct_answer_first_name, \
            self.errorMessage.format(answer_first_name, correct_answer_first_name)
        assert answer_last_name == correct_answer_last_name, \
            self.errorMessage.format(answer_last_name, correct_answer_last_name)

        # schema of the table
        assert answer_schema == schema, \
            self.errorMessage.format(answer_schema, schema)

        # name of the table
        assert answer_table_name == correct_answer_table_name, \
            self.errorMessage.format(answer_table_name, correct_answer_table_name)

    def test_message_dir_generator(self) -> NoReturn:
        table: list = [(0, "Hello")]
        schema = StructType([
            StructField('MESSAGE_ID', IntegerType(), True),
            StructField('TEXT', StringType(), True)
        ])

        correct_answer: tuple = (table, schema, "message_dir")
        answer: tuple = self.files_generator.message_dir_generator()

        answer_message_id = type(answer[0][0][0])
        answer_text = type(answer[0][0][1])

        correct_answer_message_id = type(correct_answer[0][0][0])
        correct_answer_text = type(correct_answer[0][0][1])

        answer_schema = answer[1]

        answer_table_name = answer[2]
        correct_answer_table_name = correct_answer[2]

        # types of the table
        assert answer_message_id == correct_answer_message_id, \
            self.errorMessage.format(answer_message_id, correct_answer_message_id)
        assert answer_text == correct_answer_text, \
            self.errorMessage.format(answer_text, correct_answer_text)

        # schema of the table
        assert answer_schema == schema, \
            self.errorMessage.format(answer_schema, schema)

        # name of the table
        assert answer_table_name == correct_answer_table_name, \
            self.errorMessage.format(answer_table_name, correct_answer_table_name)

    def test_message_generator(self) -> NoReturn:
        table: list = [(0, 2)]
        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])

        correct_answer: tuple = (table, schema, "message")
        answer: tuple = self.files_generator.message_generator()

        answer_user_id = type(answer[0][0][0])
        answer_message_id = type(answer[0][0][1])

        correct_answer_user_id = type(correct_answer[0][0][0])
        correct_answer_message_id = type(correct_answer[0][0][1])

        answer_schema = answer[1]

        answer_table_name = answer[2]
        correct_answer_table_name = correct_answer[2]

        # types of the table
        assert answer_user_id == correct_answer_user_id, \
            self.errorMessage.format(answer_user_id, correct_answer_user_id)
        assert answer_message_id == correct_answer_message_id, \
            self.errorMessage.format(answer_message_id, correct_answer_message_id)

        # schema of the table
        assert answer_schema == schema, \
            self.errorMessage.format(answer_schema, schema)

        # name of the table
        assert answer_table_name == correct_answer_table_name, \
            self.errorMessage.format(answer_table_name, correct_answer_table_name)

    def test_retweet_generator(self) -> NoReturn:
        table: list = [(0, 2, "Hello")]
        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('SUBSCRIBER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])
        message_data: list = [(1, 0), (5, 1), (9, 2), (5, 3), (8, 4), (3, 5), (1, 6), (4, 7), (2, 8), (7, 9),
                              (8, 10), (8, 11), (9, 12), (5, 13), (6, 14), (8, 15), (8, 16), (7, 17), (1, 18), (8, 19)]

        correct_answer: tuple = (table, schema, "retweet")
        answer: tuple = self.files_generator.retweet_generator(message_data)

        answer_user_id = type(answer[0][0][0])
        answer_subscriber_id = type(answer[0][0][1])
        answer_message_id = type(answer[0][0][1])

        correct_answer_user_id = type(correct_answer[0][0][0])
        correct_answer_subscriber_id = type(correct_answer[0][0][1])
        correct_answer_message_id = type(correct_answer[0][0][1])

        answer_schema = answer[1]

        answer_table_name = answer[2]
        correct_answer_table_name = correct_answer[2]

        # types of the table
        assert answer_user_id == correct_answer_user_id, \
            self.errorMessage.format(answer_user_id, correct_answer_user_id)
        assert answer_subscriber_id == correct_answer_subscriber_id, \
            self.errorMessage.format(answer_subscriber_id, correct_answer_subscriber_id)
        assert answer_message_id == correct_answer_message_id, \
            self.errorMessage.format(answer_message_id, correct_answer_message_id)

        # schema of the table
        assert answer_schema == schema, \
            self.errorMessage.format(answer_schema, schema)

        # name of the table
        assert answer_table_name == correct_answer_table_name, \
            self.errorMessage.format(answer_table_name, correct_answer_table_name)

    def test_retweet_generator_wave_2(self) -> NoReturn:
        table: list = [(0, 2, "Hello")]
        schema = StructType([
            StructField('USER_ID', IntegerType(), True),
            StructField('SUBSCRIBER_ID', IntegerType(), True),
            StructField('MESSAGE_ID', IntegerType(), True)
        ])
        retweet_data: list = [(1, 0, 2), (5, 1, 2), (9, 2, 6), (5, 3, 8), (8, 4, 2),
                              (3, 5, 8), (1, 6, 2), (4, 7, 7), (2, 8, 6), (7, 9, 3),
                              (8, 3, 10), (8, 2, 11), (9, 7, 12), (5, 6, 13), (6, 4, 14),
                              (8, 3, 15), (8, 2, 16), (7, 1, 17), (1, 5, 18), (8, 7, 19),
                              (8, 2, 10), (8, 1, 11), (9, 7, 12), (5, 3, 13), (6, 2, 14),
                              (8, 0, 15), (8, 0, 16), (7, 3, 17), (1, 1, 18), (8, 2, 19)]

        correct_answer: tuple = (table, schema, "retweet")
        answer: tuple = self.files_generator.retweet_generator(retweet_data)

        answer_user_id = type(answer[0][0][0])
        answer_subscriber_id = type(answer[0][0][1])
        answer_message_id = type(answer[0][0][1])

        correct_answer_user_id = type(correct_answer[0][0][0])
        correct_answer_subscriber_id = type(correct_answer[0][0][1])
        correct_answer_message_id = type(correct_answer[0][0][1])

        answer_schema = answer[1]

        answer_table_name = answer[2]
        correct_answer_table_name = correct_answer[2]

        # types of the table
        assert answer_user_id == correct_answer_user_id, \
            self.errorMessage.format(answer_user_id, correct_answer_user_id)
        assert answer_subscriber_id == correct_answer_subscriber_id, \
            self.errorMessage.format(answer_subscriber_id, correct_answer_subscriber_id)
        assert answer_message_id == correct_answer_message_id, \
            self.errorMessage.format(answer_message_id, correct_answer_message_id)

        # schema of the table
        assert answer_schema == schema, \
            self.errorMessage.format(answer_schema, schema)

        # name of the table
        assert answer_table_name == correct_answer_table_name, \
            self.errorMessage.format(answer_table_name, correct_answer_table_name)
