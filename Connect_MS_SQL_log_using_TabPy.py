import pandas as pd
import pyodbc
import pantab
from tableauhyperapi import TableName
import tabpy

def get_CDC_data(a=1):
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=DESKTOP-0O33O0F;'
                          'Database=ReadingDBLog;'
                          'Trusted_Connection=yes;')


    path_to_hyper = r'C:\Users\T450\PycharmProjects\MS_SQL_Server_connection\Output.hyper'
    cursor = conn.cursor()
    query = '''
    SELECT TOP (1000)
          [Sr.No]
          ,[__$start_lsn] AS START_LSN
          ,[Date]
          ,[City]
          ,[tran_begin_time]
          ,[tran_end_time]
          ,[__$command_id],
          CASE [__$operation]
		  WHEN 1 THEN 'DELETE'
		  WHEN 2 THEN 'INSERT'
		  WHEN 3 THEN 'VALUE BEFORE UPDATE STATEMENT'
		  ELSE 'VALUE AFTER UPDATE STATEMENT'
		  END AS OPERATION
    FROM [ReadingDBLog].[cdc].[dbo_Location_CT] b inner join [ReadingDBLog].[cdc].[lsn_time_mapping] a on [start_lsn] = [__$start_lsn]
    '''

    sql_query = pd.read_sql_query(query, conn)
    #sql_query = pd.read_sql_query('select * from ReadingDBLog.dbo.Location',conn)
    print(sql_query)
    print(type(sql_query))
    data = sql_query
    data['Date'] = data['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data['tran_begin_time'] = data['tran_begin_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data['tran_end_time'] = data['tran_end_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    #data['START_LSN'] = data['START_LSN'].str.decode(encoding='utf-8', errors='ignore')
    data['START_LSN'] = data['START_LSN'].convert_dtypes(convert_string=True)
    print(data.dtypes)
    print(data['START_LSN'])

    return data


def get_output_schema():
  return pd.DataFrame({
    'OPERATION' : prep_string(),
    'START_LSN': prep_string(),
    'Sr.No' : prep_int(),
    'Date' : prep_string(),
    'City' : prep_string(),
    'tran_begin_time' : prep_string (),
    'tran_end_time' : prep_string (),
    '__$command_id' : prep_int()
    })

-- print(get_CDC_data(1))
