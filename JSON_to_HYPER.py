from tableauhyperapi import Connection, HyperProcess, SqlType, TableDefinition, \
    escape_string_literal, escape_name, NOT_NULLABLE, Telemetry, Inserter, CreateMode, TableName
import pandas as pd
import csv
import json

# Bắt đầu tiến trình HyperProcess
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    print('The Hyper Process has started')

    #Tạo kết nối đến file Hyper. Chú ý phần CreateMode
    with Connection(endpoint=hyper.endpoint, database="Jason_to_hyper.hyper", create_mode = CreateMode.CREATE_AND_REPLACE) as connection:
        print("The Connection to the Hyper file is open")



        #Định nghĩa bảng, cấu trúc bảng
        table_definition = TableDefinition(table_name= TableName("Schema","Extract"), columns=[
            TableDefinition.Column("Col1", SqlType.text()),
            TableDefinition.Column("Col2", SqlType.big_int())
        ])
        print("The table is defined")



        # Tạo schema, tạo bảng
        connection.catalog.create_schema("Schema")
        connection.catalog.create_table(table_definition)
        print("The table is created")



        #Kết nối và định nghĩa dữ liệu cần thực hiện
        path_to_json = r"C:\Users\T450\PycharmProjects\pythonProject\test.json"
        df = pd.read_json(path_to_json)
        data = df.values


        # Đẩy dữ liệu vào file Hyper
        with Inserter(connection, table_definition) as inserter:
            inserter.add_rows(data)
            inserter.execute()
            print('Adding data to the hyper file...')
        print('Adding data completed')
print('Close all connections and close the HyperProcess')
