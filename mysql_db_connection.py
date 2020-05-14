import MySQLdb.connections as con
from MySQLdb.connections import Error
import time

class SqlConnection(object):

	def __init__(self,user='root',password='P@ssword1',host='127.0.0.1'):
		self.connect = con.Connection(user=user,password=password,host=host)
		self.cursor = self.connect.cursor()


	def create_database(self,DB_NAME):
		try:
			self.cursor.execute(
				"CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))

		except Error as err:
			print("Failed creating Database: {}".format(err))
			exit(1)


	def insert_data(self,table_template,input_data):
		self.cursor.execute(table_template,input_data)
		self.connect.commit()
		
	def erase_table_data(self):
		trnc_tbl = ("""TRUNCATE TABLE IAMDB.iamdb_tbl""")		
		self.cursor.execute(trnc_tbl)
		self.connect.commit()

	def drop_db(self):
		drp_tbl = ("""DROP TABLE IAMDB.iamdb_tbl""")	
		self.cursor.execute(drp_tbl)
		self.connect.commit()

	def read_query(self,query):
		self.execute_query(query)
		return self.cursor

	def execute_query(self,query):
		self.cursor.execute(query)

	def commit(self):
		self.connect.commit()

	def close(self):
		self.cursor.close()
		self.connect.close()


#obj = SqlConnection(user='root',password='P@ssword1',host = '127.0.0.1')
obj = SqlConnection(user='databot_server_connection',password='Pa$$$$0521',host = '192.168.1.6')
obj.create_database(DB_NAME='IAMDB')



select_template = """
SELECT *
FROM
IAMDB.iamdb_tbl
"""

val = obj.read_query(select_template)


for i in val.fetchall():
	print(i)
# 	break





########## Create Table ###############
# TABLES = {}
# TABLES['iamdb_tbl'] = ("""CREATE TABLE  
# 	IAMDB.iamdb_tbl (Rank varchar(5) NOT NULL,
# 	Title varchar(100) NOT NULL,
# 	Year varchar(10) NOT NULL,
# 	Rating float(2) NOT NULL) ENGINE=InnoDB
# 	""")

# for table in TABLES:
# 	table_name = TABLES[table]

# 	try:
# 		print('Creating table {}:'.format(table),end='')
# 		obj.execute_db(table_name)
# 		print('Sucess!')
# 	except Error as err:
# 		print('Error!',err)


# ########## Insert Table ###############

# table_template = ("""INSERT INTO IAMDB.iamdb_tbl(Rank,Title,Year,Rating) VALUES(%s,%s,%s,%s)""")
# input_data = (1,'TestMovie',2020,9.8)
# obj.insert_data(table_template,input_data)


###### Extract Data ########






"""
DDL (Data Definition Language): CREATE, ALTER, DROP (cannot rollback, delete including constraints), RENAME and TRUNCATE (cannot rollback, delete all data except for constraints)
ex. CREATE DATABASE / CREATE TABLE etc.

DML(Data Manipulation Language): SELCT, INSERT, UPDATE, DELETE (You can Rollback, can use where)

"""
