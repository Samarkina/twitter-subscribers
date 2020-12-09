from datetime import datetime
from pyspark.sql.dataframe import DataFrame

# datetime object containing current date and time
NOW: datetime = datetime.now()
CURR_DATE: datetime = NOW.strftime("%Y-%m-%d")
CURR_TIME: datetime = NOW.strftime("%H-%M-%S")

def save_file(data: DataFrame, name: str):
    """Method saving the data to parquet format file

    :param data: content of the table
    :return:
    """
    print("I will save the data to parquet file")
    path = ".././data/" + CURR_DATE + '/recieved=' + CURR_TIME + '/' + name
    data.write.parquet(path)

    print("I saved the data to parquet file:" + path)
