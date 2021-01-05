from pyspark.shell import sqlContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, Row
from pyspark.context import SparkContext


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


def convert_list_to_df(spark: SparkSession, table: list, schema: StructType, table_name: str) -> (DataFrame, str):
    """Converting tuple of table (list type), schema, table_name to tuple table (DataFrame type), table_name

    :param spark: SparkSession
    :param table: list - Data from the table
    :param schema: StructType - Schema of the table
    :param table_name: str - Table name
    :return: Table in format DataFrame with table name (str type)
    """
    print("Converting content of the \"{}\" table from List to DataFrame format...".format(table_name))
    sc: SparkContext = spark.sparkContext
    rdd = sc.parallelize(table)
    df: DataFrame = sqlContext.createDataFrame(rdd, schema)

    return df, table_name
