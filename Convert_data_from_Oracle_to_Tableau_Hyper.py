import cx_Oracle
import pandas as pd
import pantab
import config
from tableauhyperapi import TableName
import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\T450\Documents\instantclient_19_11",
                             config_dir=r"C:\Users\T450\Documents\instantclient_19_11")

# Location for the hyper output:



# Main
try:
    # Connect to the Oracle DB
    con = cx_Oracle.connect('c##cuong/cuong123@localhost:1521/orcl')
    cur = con.cursor()

    # The query statement
    table_source = input('Nhập tên source table cần convert thành hyper: ')
    query = "select * from " + table_source
    path_to_hyper = table_source + '.hyper'

    # Excute the query
    cur.execute(query)


    # Collect the columns name of the querry
    index = cur.description
    #print(index)
    row = list()
    for i in range(len(index)):
        row.append(index[i][0])


    # fetchall() is used to fetch all records from result set
    print('In process query')
    data = cur.fetchall()

    #print(data)
    data_to_convert = pd.DataFrame(list(data), columns = row)
    #print(data_to_convert)


    # fetchmany(int) is used to fetch limited number of records from result set based on integer argument passed in it
    #data = cur.fetchmany(3)
    #print(data)
    #data_to_convert = pd.DataFrame(list(data), columns = row)
    #print(data_to_convert)


    # fetchone() is used fetch one record from top of the result set
    #data = cur.fetchone()
    #data_to_convert = pd.DataFrame(list(data), columns = row)
    #print(data_to_convert)


    # Hyper file's structure definition
    table_definition = TableName('Schema', 'Table')
    pantab.frame_to_hyper(data_to_convert, path_to_hyper, table=table_definition)
    print('Đã convert xong !')


except cx_Oracle.DatabaseError as er:
    print('There is an error in the Oracle database:', er)

except Exception as er:
    print('Error:' + str(er))

finally:
    if cur:
        cur.close()
    if con:
        con.close()


