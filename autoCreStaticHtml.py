#coding=utf-8
import time
import sys
import MySQLdb

def get_db_data(country, dateTime):
	"""
+-----------------+------------------+------+-----+---------+----------------+
| Field           | Type             | Null | Key | Default | Extra          |
+-----------------+------------------+------+-----+---------+----------------+
| id              | int(11) unsigned | NO   | PRI | NULL    | auto_increment |
| app_id_apple    | int(10) unsigned | NO   | MUL | NULL    |                |
| app_type        | int(10) unsigned | NO   |     | NULL    |                |
| app_name        | varchar(255)     | NO   |     | NULL    |                |
| app_url         | text             | NO   |     | NULL    |                |
| app_itunesid    | int(10) unsigned | NO   |     | NULL    |                |
| collect_country | varchar(32)      | NO   |     | NULL    |                |
| app_logo        | varchar(255)     | NO   |     | NULL    |                |
| app_price       | double           | NO   |     | NULL    |                |
| gathering_date  | date             | NO   |     | NULL    |                |
+-----------------+------------------+------+-----+---------+----------------+
	"""
	conn = MySQLdb.connect(host="localhost", user="app_gathering", passwd="app_gathering", db="app_new_application",charset='utf8')
	cursor = conn.cursor()
	str_except = ''
	if country != 'cn':
		str_except = " and app_id_apple not in (select cn.app_id_apple from appstore_cn as cn where cn.app_type in(7,8) and cn.app_itunesid = 6014 ) "
	sql = "select app_id_apple, app_name, app_url, app_logo, app_price from appstore_%s where app_type in (7, 8) and app_itunesid = 6014 and gathering_date = '%s' %s order by app_price, app_id_apple " %(country, dateTime, str_except) 
	cursor.execute(sql)
	res = cursor.fetchall()
	cursor.close()
	conn.close()
	return res
	

def createHTML(country, currency, info, dateTime):
	filename = "/var/www/self/%s/%s_%s.html" % ( country, country, dateTime)
	file_obj = open(filename, "w+")

	head = """
<!DOCTYPE html>
<html>

<head>

<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
</head>
<body>
"""
	file_obj.write(head)

	file_obj.write("<table>\n")
	tr = "<tr><td width='65px'><a href='%s'> <img src='%s' /></a></td><td width='300px'><a href='%s' >%s</a></td><td width='65px'>%s %s</td></tr>\n"
	for row in info:
		line = tr %(row[2], row[3], row[2], row[1], row[4], currency)
		file_obj.write(line.encode('utf-8'))

	file_obj.write("</table>")

	tail = """
</body>
</HTML>
"""

	file_obj.close()

def createDailyAllFile(lstCountry, dateTime):
	filename = "/var/www/self/date/%s.html" % (dateTime)
	file_obj = open(filename, "w+")

	head = """
<!DOCTYPE html>
<html>

<head>

<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
</head>
<body>
"""
	file_obj.write(head)

	file_obj.write("<table>\n")
	tr = "<tr><td><a href='/self/%s/%s_%s.html'> %s </a></td></tr>\n" 
	for country in lstCountry:
		line = tr %(country[0], country[0], dateTime, country[1])
		file_obj.write(line)

	file_obj.write("</table>\n")

	tail = """
</body>
</HTML>
"""

	file_obj.close()
	

def job(country, currency, date):
	res = get_db_data(country, date)
	createHTML(country, currency, res, date)

if __name__ == '__main__':
	date = time.strftime('%Y-%m-%d',time.localtime(time.time() - 24*60*60) )
	if len(sys.argv) == 3 and sys.argv[1] in ('-d', '--date'):
		date = sys.argv[2]

	country = [['cn','中国大陆', 'Y'],['tw','台湾地区', '$'],
				['hk','香港地区', '$'],['us','美国','$'],
				['jp','日本','Y'],['kr','韩国','$'],['ca','加拿大','$']]
	createDailyAllFile(country, date)
	for c in country:
		job(c[0], c[2],date)
		
