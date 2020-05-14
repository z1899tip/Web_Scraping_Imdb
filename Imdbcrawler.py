from bs4 import BeautifulSoup
import requests
import mysql_db_connection

#TODO: Create connection with IMDB Site

class DataCollector:

	def __init__(self,address):
		self.address = address

	def get_connection(self,url):
		connection = requests.get(url)
		return connection

	def parse_data(self):
		con = self.get_connection(self.address)
		soup = BeautifulSoup(con.content,'lxml')
		other_info_list = soup.find_all('td',class_='titleColumn')
		rating_info_list = soup.find_all('td',class_='ratingColumn imdbRating')
		needed_info_list = zip(other_info_list,rating_info_list)

		rank_list = []
		title_list = []
		date_list = []
		rating_list = []
		for info in needed_info_list:
			(rank,title,_,date,_,),(rating)=list(info)
			rank_list.append(int(rank.strip().strip('.')))
			title_list.append(title.text)
			date_list.append(date.text.strip("(").strip(")"))
			rating_list.append(float(rating.text.strip()))

		return zip(rank_list,title_list,date_list,rating_list)


address = 'https://www.imdb.com/chart/top'
obj = DataCollector(address)

cnx = mysql_db_connection.SqlConnection()
cnx.erase_table_data()
table_template = ("""INSERT INTO IAMDB.iamdb_tbl(Rank,Title,Year,Rating) VALUES(%s,%s,%s,%s)""")

for data in list(obj.parse_data()):
	cnx.insert_data(table_template,data)

