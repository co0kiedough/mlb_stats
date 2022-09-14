import pyodbc, re, json
from datetime import datetime as dt
from collections import Counter as cntr
from dateutil.parser import parse as dp

class mlbsql:
	
	def __init__(self, db='mlb_predict'):
		
		self.db = db
		self.cnxn = pyodbc.connect(r'Driver=SQL Server;Server=MSI\SQLEXPRESS;Database={0};Trusted_Connection=yes;'.format(self.db))
		self.cursor = self.cnxn.cursor()
		
	def sql_chk_exist(self, tbl_name=None, col_list=None, cursor=self.cursor):
		
		if tbl_name == None:
			print('please provide a table name')
			if col_list == None:
				print('you can optionally include a list of columns')
			return False
		return_dict = {'table':False, 'columns':False}
		c = cursor
		tbl_sql = """select * from INFORMATION_SCHEMA.TABLES where TABLE_NAME='{0}'""".format(tbl_name)
		c.execute(tbl_sql)
		tbl = c.fetchall()
		if tbl_name in tbl:
			return_dict['table'] = True
			
		col_sql = """select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='{0}'""".format(tbl_name)
		c.execute(col_sql)
		cols = c.fetchall()
		
		if len(col_list) > 0:
			not_in = []
			return_dict['columns'] = {col:False for col in col_list}
			for col in col_list:
				if col in cols:
					return_dict['columns'][col] = True
		else:
			return_dict['columns'] = [col for col in cols]
		return return_dict
	
	def boxscore_tbl(self):
		cx = self.cnxn
		c = self.cursor
		
	def create_tbl(self, tbl_name, col_type_dict):
		
		cols = [*col_type_dict.keys()]
		types = [*col_type_dict.values()]
		
		
		
		sql_string = '''create table {0} '''
		for i in range:
			code
		
	
	
	#code
