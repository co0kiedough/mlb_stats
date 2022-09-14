import pyodbc, os, re, json

class pyodbc_functions:
	
	def __init__(self, db=None):
		if db == None:
			db = 'tsdd'
			
		self.db = db
		self.cnxn = pyodbc.connect(r'Driver=SQL Server;Server=MSI\SQLEXPRESS;Database=%s;Trusted_Connection=yes;' % db)
		self.cursor = self.cnxn.cursor()
		
	def select_sql(self, tbl_name, col_list=None, col_func_list = None, var_cond_list=None, van_cond_func_list = None, order_by_dict=None):
		
		c = self.cursor
		
		col_names_sql = """select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s'""" % tbl_name
		c.execute(col_names_sql)
		col_names = c.fetchall()
		col_names_list = []
		for col in col_names:
			col_names_list.append(col[3])
		self.cur_tbl_cols = col_names_list.copy()

		sql = '''SELECT '''
		if col_list != None:
			if len(col_list) >1:
				for col in col_list:
					try:
						col in col_names_list
					except Exception as e:
						print(repr(e), 'column not in table')
					if col_func_list != None:
						if col in col_func_list:
							cfi = col_func_list.index(col)
							col_func = col_func_list[cfi]['function']
							col = col_func + '(' + col + ')'
						else:
							raise Exception()
						
					if col_list.index(col) == len(col_list) -1:
						sql = sql + col + ' FROM %s' % tbl_name
					else:
						sql = sql + col + ''', '''
			if len(col_list) == 1:
				sql = sql + ' ' + col_list[0]  + ' FROM %s ' % tbl_name
		else:
			sql = sql + ' * FROM %s ' % tbl_name
			
		if var_cond_list != None:
			
			if len(var_cond_list) > 1:
				for condition in var_cond_list:
					if var_cond_list.index(condition) == 0:
						first_cond_col = condition['column']
						first_cond_op = condition['operator']
						first_cond_var = condition['variable']
						if 'function' in condition:
							first_cond_func = condition['function']
							first_cond_col = first_cond_func +'(' + first_cond_col + ')'
						sql = sql + " WHERE %s %s '%s'" % (first_cond_col, first_cond_op, first_cond_var)
					
					else:
						if 'andor' in condition:
							cond_andor = condition['andor']
						else:
							cond_andor = 'AND'
						cond_col = condition['column']
						if 'function' in condition:
							cond_func = condition['function']
							cond_col = cond_func +'(' + cond_col + ')'
						cond_op = condition['operator']
						cond_var = condition['variable']
						sql = sql + " %s %s %s '%s'" % (cond_andor, cond_col, cond_op, cond_var)
				
						
			if len(var_cond_list) == 1:
				for condition in var_cond_list:
					
					first_cond_col = condition['column']
					first_cond_op = condition['operator']
					first_cond_var = condition['variable']
					if 'function' in condition:
							first_cond_func = condition['function']
							first_cond_col = first_cond_func +'(' + first_cond_col + ')'
					sql = sql + " WHERE %s %s '%s'" % (first_cond_col, first_cond_op, first_cond_var)	
		
		if order_by_dict != None:
			order_col = order_by_dict['column']
			order_dir = order_by_dict['direction']
			
			sql = sql + ' ORDER BY %s %s' % (order_col, order_dir)
		
		
		print(sql)
		
		# c.execute()
	
