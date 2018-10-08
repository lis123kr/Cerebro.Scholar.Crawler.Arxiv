from AWS_SDK import S3, DynamoDB, RDS
import pickle as pk
import datetime, time
from paper import Paper

class S3_utils:
	def __init__(self):
		self.s3 = S3()

	def get_s3(self):
		if not self.s3:
			self.s3 = S3()
		return self.s3

	def get_scplist(self, filename=None):
		resource = self.s3.get_resource()

		yesterday = time.time() - 86400
		yes_dt = datetime.datetime.fromtimestamp( yesterday ).strftime("%Y%m%d")
		
		READ_NAME = yes_dt + "scplist_paper.pkl";
		if not filename:
			filename = READ_NAME

		# get previous list to crawl...
		scp = resource.Object(self.s3.BUCKET_NAME, filename)
		scp_ = pk.loads(scp.get()['Body'].read())

		print(filename+" is loaded, len : ", len(scp_))

		return scp_

	def upload_scplist(self, filename, scplist, timeformat='%Y%m%d'):
		import io
		bucket = self.s3.get_bucket()
		cur_dt = datetime.datetime.today().strftime(timeformat)
		WRITE_NAME = filename#cur_dt + filename;
		if timeformat != '%Y%m%d':
			WRITE_NAME = cur_dt + filename
		with io.BytesIO( pk.dumps( scplist ) ) as data:
			bucket.upload_fileobj( data, WRITE_NAME, ExtraArgs={'ACL': 'public-read'})

		print(WRITE_NAME+ " is uploaded, len : ", len(scplist))
		return True

class RDS_utils:
	def __init__(self):
		self.DB = RDS()
		self.axvcolumns = ['arXiv_id', 'title', 'abstract', 'version',
            'authors', 'start_date', 'pub_year', 'publication']
	
	def insert_paper(self, paper_):
		sql, data = paper_.to_insert_sql()
		return self.DB.insert(sql, data)

	def insert_link(self, A, B, cite_text):
		sql = "insert into link (p_id_is_cited, by_p_id, cite_text) values (%s, %s, %s)"
		data = ( A, B, cite_text )
		return self.DB.insert(sql, data)

	def update_paper(self, dp_id, paper_):
		ori = self.get_paper_by_p_id(dp_id, paper_.columns)
		data = paper_.Update_paper(ori)
		sql, data =paper_.to_update_sql(dp_id, data)
		return self.DB.update(sql, data)

	def get_paper_by_p_id(self, p_id, columns=None):
		if columns == None:
			columns = self.axvcolumns

		sql = "SELECT " + ','.join( c for c in columns) + " FROM paper where p_id = %s"
		# sql = "SELECT * from paper where p_id = %s"
		data = (p_id,)
		p = self.DB.select(sql, data)
		if len(p):
			return list(p[0])
		return None

	def get_pid_from_scp_id(self, scp_id):
		sql = "SELECT p_id from paper where scp_id = %s"
		data = (str(scp_id),)
		pid = self.DB.select(sql, data)
		if len(pid):
			return pid[0][0]
		return None

	def get_pid_from_arXiv_id(self, arXiv_id):
		sql = "SELECT p_id from paper where arXiv_id = %s"
		data = (arXiv_id,)
		pid = self.DB.select(sql, data)
		if len(pid):
			return pid[0][0]
		return None

	def get_top_pid(self):
		sql = "SELECT p_id from paper"
		response = self.DB.select(sql,None)
		if len(response):
			response = sorted(response)
			return response[-1][0] + 1
		return 1

	def update_axv(self, pid, data):
		sql = "UPDATE paper SET " + ','.join( c+'=%s' for i, c in enumerate(self.axvcolumns[1:]) ) + " WHERE p_id = %s"
		return self.DB.update(sql, tuple(data[1:]))

	def insert_axv(self, data):
		sql = "INSERT INTO paper (" + ','.join( i for i in self.axvcolumns) +') VALUES (' +','.join( '%s' for _ in self.axvcolumns) + ')'
		data[1] = str( {"axv" : data[1]} )
		data[3] = str( {"axv" : data[3]} )
		data[4] = str( {"axv" : data[4]} )
		data[5] = str( {"axv" : data[5]} )
		data[6] = str( {"axv" : data[6]} )
		data[7] = str( {"axv" : data[7]} )
		return self.DB.insert(sql, tuple(data))

class Dynamo_utils:
	def __init__(self, TableNameKey):
		self.TableNameKey = TableNameKey
		self.DB = DynamoDB(TableNameKey)

	def get_DB(self, TableNameKey):
		if not self.DB:
			self.DB = DynamoDB(TableNameKey)
		return self.DB

	def insert_scplist(self, order_, data):
		item = {
			'order' : order_,
			'scp_id' : data['scp_id'],
			'A' : data['A'],
			'B' : data['B'],
			'cite_text' : data['cite_text'],
			'scp_eid': data.get('scp_eid', None)
		}
		if self.DB.insert_item(item):
			pass
			return True
		return False

	def get_scplist_by_order(self, order_):
		item =  self.DB.get_item({'order':order_})
		return item

	def batch_write(self, itemlist):
		if self.DB.batch_write(itemlist):
			print("inserted {} itmes".format(len(itemlist)))
			return True

class Duplication_check:
	DB = None

	@classmethod
	def init(cls):
		cls.DB = DynamoDB("DUPLICATION_TABLE_NAME")

	@classmethod
	def check(cls, title_year):
		if not cls.DB:
			cls.init()
		item = cls.DB.get_item({"title_pubyear" : str(title_year) })
		if item:
			return int(item['p_id'])
		else:
			return None

	@classmethod
	def insert_title_year(cls, title_year, p_id):
		if not cls.DB:
			cls.init()
		item = { 
				"title_pubyear" : title_year,
				"p_id" : p_id
				}
		if cls.DB.insert_item(item):
			return True
		else:
			return False

	@classmethod
	def get_count(cls):
		if not cls.DB:
			cls.init()
		return cls.DB.get_table().item_count
		