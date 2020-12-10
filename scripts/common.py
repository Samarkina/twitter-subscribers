def save_file(data: list, path: str):
    """Method saving the data to parquet format file

    :param data: content of the table
    :return:
    """
    print("I will save the data to parquet file")

    for table, name in data:
        file_path: str = path + name
        table.write.parquet(file_path)
        print("I saved the data to parquet file:" + file_path)
