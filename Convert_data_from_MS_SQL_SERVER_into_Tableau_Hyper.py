import pandas as pd
import pyodbc
import pantab
from tableauhyperapi import TableName
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-0O33O0F;'
                      'Database=ReadingDBLog;'
                      'Trusted_Connection=yes;')


path_to_hyper = r'C:\Users\T450\PycharmProjects\MS_SQL_Server_connection\Output.hyper'
cursor = conn.cursor()
query = '''
SELECT TOP (1000)
      [__$operation]
      ,[Sr.No]
      ,[Date]
      ,[City]
	  ,[tran_begin_time]
      ,[tran_end_time]
      ,[__$command_id]
FROM [ReadingDBLog].[cdc].[dbo_Location_CT] b inner join [ReadingDBLog].[cdc].[lsn_time_mapping] a on [start_lsn] = [__$start_lsn]
'''

sql_query = pd.read_sql_query(query, conn)
#sql_query = pd.read_sql_query('select * from ReadingDBLog.dbo.Location',conn)
print(sql_query)
print(type(sql_query))
data = sql_query
table = TableName("schema",'table_name')
pantab.frame_to_hyper(data,path_to_hyper,table=table)