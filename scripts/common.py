from pyspark.shell import sqlContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, Row
from pyspark.context import SparkContext
import random
import string


def save_file(data: list, path: str):
    """Method saving the data to parquet format file

    :param data: content of the table
    :return:
    """
    print("Saving the data to parquet file...")

    for table, name in data:
        file_path: str = path + name
        table.write.parquet(file_path)
        print("Data was saved to parquet file: {}".format(file_path))


def convert_list_to_df(spark: SparkSession, table: set, schema: StructType, table_name: str) -> (DataFrame, str):
    """Converting tuple of table (list type), schema, table_name to tuple table (DataFrame type), table_name

    :param spark: SparkSession
    :param table: list - Data from the table
    :param schema: StructType - Schema of the table
    :param table_name: str - Table name
    :return: Table in format DataFrame with table name (str type)
    """
    table: list = convert_set_to_list(table)
    print("Converting content of the \"{}\" table from List to DataFrame format...".format(table_name))
    sc: SparkContext = spark.sparkContext
    rdd = sc.parallelize(table)
    df: DataFrame = sqlContext.createDataFrame(rdd, schema)

    return df, table_name


def convert_set_to_list(table: set) -> list:
    """Converting set to list

    :param table: set - Content of the table in Set format
    :return: Content of the table in list format
    """
    table: list = list(table)
    return table


def names_generator() -> (str, str):
    """Method generating firstnames and lastnames from dictionary

    :return: random firstname and lastname in strings format
    """
    tuple_first_names: tuple = ('John', 'Andy', 'Joe', 'Liam', 'Olivia', 'Noah', 'Emma', 'Oliver', 'Ava', 'William',
                                'Sophia', 'Logan', 'Isabella', 'James', 'Charlotte', 'Benjamin', 'Amelia', 'Lucas',
                                'Mia', 'Mason', 'Harper', 'Ethan', 'Evelyn', 'Jack', 'Matthew', 'Joseph', 'Levi',
                                'Austin', 'Jordan', 'Adam', 'Xavier', 'Jose', 'Jace', 'Everett', 'Declan', 'Evan',
                                'Kayden', 'Parker', 'Wesley', 'Kai', 'Brayden', 'Bryson', 'Weston', 'Jason',
                                'Emmett', 'Sawyer', 'Silas', 'Bennett', 'Brooks', 'Micah', 'Damian', 'Harrison',
                                'Waylon', 'Ayden', 'Vincent', 'Ryder')
    tuple_last_names: tuple = ('Smith', 'Johnson', 'Williams', 'Brown', 'Jones',
                               'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez',
                               'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez',
                               'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
                               'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill',
                               'Flores', 'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell')

    first_name: str = random.choice(tuple_first_names)
    last_name: str = random.choice(tuple_last_names)

    return first_name, last_name


def get_random_string(length: int) -> str:
    """Generating random string with given number

    :param length: length of the word
    :return: random word
    """
    letters = string.ascii_lowercase
    word = ''.join((random.choice(letters) for i in range(length)))

    return word
