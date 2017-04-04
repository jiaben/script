#coding=utf-8
import time
import urllib2
import Queue
import threading
import xml.dom.minidom
import MySQLdb

#获取数据
def get_appstop_data(url): 
	myurl = url   
	user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'  
	headers={ 'User-Agent' : user_agent }  
	req=urllib2.Request(myurl, headers = headers)  
	myResponse=urllib2.urlopen(req)  
	try:
		myResponse = urllib2.urlopen(req)
	except urllib2.HTTPError, e:
		print e.fp.read()    
	resultXml = myResponse.read()
	return resultXml

def get_attrvalue(node, attrname):
	if node:
		return node.getAttribute(attrname)
	else:
		return ''

def get_xmlnode(node,name):
	if node:
		return node.getElementsByTagName(name)
	else:
		return []

def get_nodevalue(node, index = 0):
	if node:
		return node.childNodes[index].nodeValue
	else:
		return ''

#解析XML
def get_xml_data(xml_string,table,type_,itunesid=0):
	doc = xml.dom.minidom.parseString(xml_string)
	app_list = []
	for index,node in enumerate(doc.getElementsByTagName('entry')):  
		#获取app应用标题
		node_title = get_xmlnode(node,'title')
		#获取应用名称
		node_name = get_xmlnode(node,'im:name')
		#获取更新时间
		node_updated = get_xmlnode(node,'updated')
		#获取应用ID
		node_appid = get_xmlnode(node,'id')
		#获取应用类型    
		node_app_category = get_xmlnode(node,'category')
		#获取应用金额
		node_price = get_xmlnode(node,'im:price')    
		#获取应用详情
		node_content = get_xmlnode(node,'content')
		#获取应用LOGO
		node_logo = get_xmlnode(node,'im:image')        

		#获取release时间
		node_release_time = get_xmlnode(node, 'im:releaseDate')

		#应用标题
		app_title = get_nodevalue(node_title[0]).encode('utf-8','ignore')
		#应用名称
		app_name = get_nodevalue(node_name[0]).encode('utf-8','ignore')
		#应用appStop URL
		app_url = get_nodevalue(node_appid[0]).encode('utf-8','ignore')
		#应用ID
		app_id = get_attrvalue(node_appid[0],'im:id').encode('utf-8','ignore')
		#应用类型ID
		app_category_id = get_attrvalue(node_app_category[0],'im:id').encode('utf-8','ignore')
		#应用上架时间
		app_release_time = get_nodevalue(node_release_time[0]).encode('utf-8','ignore')

		#应用更新时间
		app_update_time = get_nodevalue(node_updated[0]).encode('utf-8', 'ignore')

		#应用类型名称
		app_category_name = get_attrvalue(node_app_category[0],'label').encode('utf-8','ignore')
		#获取应用金额
		app_price = get_attrvalue(node_price[0],'amount').encode('utf-8','ignore')
		#获取应用详情
		app_content = get_nodevalue(node_content[0]).encode('utf-8','ignore')
		#获取应用LOgo
		app_logo = get_nodevalue(node_logo[0]).encode('utf-8','ignore')
		rank = index+1
		app_tmp = {}
		app_tmp['rank'], app_tmp['title'], app_tmp['name'], app_tmp['url'], app_tmp['id'], app_tmp['category_id'], app_tmp['category_name'], app_tmp['price'], app_tmp['logo'], app_tmp['table'], app_tmp['type'], app_tmp['itunesid'] = (rank, app_title, app_name, app_url, app_id, app_category_id, app_category_name, app_price, app_logo, table, type_, itunesid)
		app_list.append(app_tmp)
	return app_list

#配置抓取参数
def marge_url():
	url_list = []
	#国家
	country = ['cn','tw','hk','us','jp','kr','ca']
	#大分类
	type = [['toppaidapplications',1],['topfreeapplications',2],['topgrossingapplications',3],['toppaidipadapplications',4],['topfreeipadapplications',5],['topgrossingipadapplications',6], ['newfreeapplications',7], ['newpaidapplications',8]]
	#小分类
	genre = ['6000','6001','6002','6003','6004','6005','6006','6007','6008','6009','6010','6011','6012','6013','6014','6015','6016','6017','6018','6020','6022','6023']
	#genre = ['6014']
	for k,v in enumerate(country):
		for k1,v1 in enumerate(type):
			url = ['http://itunes.apple.com/',v,'/rss/',v1[0],'/limit=200/xml']
			urlStr = [''.join(url),v,v1[1],0]
			url_list.append(urlStr)
			for k2,v2 in enumerate(genre):
				url = ['http://itunes.apple.com/',v,'/rss/',v1[0],'/limit=200/genre=',v2,'/xml']
				urlStr = [''.join(url),v,v1[1],v2]
			#	url_list.append(urlStr)
	return url_list

#执行Sql操作
def DbSql(sql,type, param=None):
	try:
		if sql:
			conn = MySQLdb.connect(host="localhost", user="app_gathering", passwd="app_gathering", db="app_new_application",charset='utf8')
			cursor = conn.cursor()
			if type == 'select':
				cursor.execute(sql)    
				data = cursor.fetchone()
			elif type == 'add':
				cursor.execute(sql, param)    
				data = int(conn.insert_id())
			else:
				data = 'nokey'
			cursor.close()
			conn.commit()
			conn.close()
			return data
		else:
			return
	except MySQLdb.Error,e:
		errMsg = "Mysql Error %d: %s" %(e.args[0],e.args[1])
		file_object = open('PythonLogo.log', 'w+')
		file_object.write(errMsg)
		file_object.close()

def addAppRank(app_list,startTime):
	for index,item in enumerate(app_list):
		name = item.get('name').replace("\'", "\\\'").replace("\"","\\\"")
		addSql = "INSERT into appstore_"+item.get('table')+"(`app_id_apple`,`app_type`,`app_name`, `app_url`, `app_itunesid`,`collect_country`,`app_logo`,`app_price`, `gathering_date`) VALUES(%s, %s,'%s','%s','%s','%s','%s','%s', date(now())) on duplicate KEY UPDATE id=id" %(item.get('id'),item.get('type'),name,MySQLdb.escape_string(item.get('url')),item.get('category_id'),item.get('table'),item.get('logo'),item.get('price'))
		DbSql(addSql, "add")
	#for index,item in enumerate(app_list):
	#	name = item.get('name').replace("\'", "\\\'").replace("\"","\\\"")
	#	print item.get('id'), item.get('type'),name, item.get('category_id'), item.get('table'), item.get('price')
	#暂停0.2秒
	time.sleep(0.2)

#q是任务队列
#NUM是并发线程总数
#JOBS是有多少任务
q = Queue.Queue()
NUM = 2
JOBS = marge_url()
startTime = int(time.time())
threads = []
mutex = threading.Lock()

class MyThread(threading.Thread):
	def __init__(self, work_queue,startTime):
		threading.Thread.__init__(self)
		self.work_queue = work_queue

	def run(self):
		global mutex
		while True:
			if self.work_queue.qsize()>0:
				arguments = self.work_queue.get()
				#threadname = threading.currentThread().getName()
				#print threadname,arguments
				mutex.acquire()        
				#抓取网页
				app_string = get_appstop_data(arguments[0])
				#解析
				app_arr = get_xml_data(app_string,arguments[1],arguments[2],arguments[3])
				#添加数据库
				addAppRank(app_arr,startTime)
				mutex.release()
				time.sleep(1)
			else:
				break

if __name__ == '__main__':
	for work in JOBS:
		q.put(work)    
	for k in range(NUM):
		threads.append(MyThread(q,startTime))
	for t in threads:
		t.setDaemon(True)    
		t.start()
	for h in threads:
		h.join()
