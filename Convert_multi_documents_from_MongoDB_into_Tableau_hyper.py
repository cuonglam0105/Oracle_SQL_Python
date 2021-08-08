import pandas as pd
from pymongo import MongoClient
import pantab
from tableauhyperapi import TableName
import numpy as np


# Đường dẫn xuất hyper
path_to_hyper = r'C:\Users\T450\Documents\Custonmer.hyper'


# Tạo kết nối đến server MongoDB
client = MongoClient("mongodb://localhost:27017/")
# Database
db = client["demo"]
# Collection
collection= db["customer"]


# Tìm documents
data_from_db = collection.find()
print(data_from_db)



# Chuyển documents thành dataframe
df = pd.DataFrame(data_from_db)
print(df)
data = df.iloc[1:,1:]
print(data)


# Khai báo schema và table
table = TableName("schema",'table_name')

# Convert dataframe thành Hyper và xuất file
pantab.frame_to_hyper(data,path_to_hyper,table=table)