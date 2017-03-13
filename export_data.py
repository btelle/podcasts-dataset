import csv, sqlalchemy, io, random, cStringIO, codecs
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from config import conn_string

def page_query(q):
	""" Page through query to prevent killed queries """
	offset = 0
	while True:
		r = False
		for elem in q.limit(1000).offset(offset):
		   r = True
		   yield elem
		offset += 1000
		if not r:
			break
		
class UnicodeWriter:
	"""
	A CSV writer which will write rows to CSV file "f",
	which is encoded in the given encoding.
	"""

	def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
		# Redirect output to a queue
		self.queue = cStringIO.StringIO()
		self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
		self.stream = f
		self.encoder = codecs.getincrementalencoder(encoding)()

	def writerow(self, row):
		self.writer.writerow([s.encode("utf-8") for s in row])
		# Fetch UTF-8 output from the queue ...
		data = self.queue.getvalue()
		data = data.decode("utf-8")
		# ... and reencode it into the target encoding
		data = self.encoder.encode(data)
		# write to the target stream
		self.stream.write(data)
		# empty queue
		self.queue.truncate(0)

	def writerows(self, rows):
		for row in rows:
			self.writerow(row)

def write_row(row, writer, key_index):
	row_dict = dict(zip(row.keys(), row))
		
	row_arr = []
	for k in key_index:
		v = row_dict[k]
		if v == None:
			row_arr.append('')
		elif type(v) == datetime:
			row_arr.append(v.strftime('%Y-%m-%dT%H:%M:%S%Z'))
		elif type(v) in (int, float, long):
			row_arr.append(str(v))
		else:
			row_arr.append(v)
	
	return writer.writerow(row_arr)

engine = create_engine(conn_string)
db_conn = engine.connect()
metadata = MetaData(engine)
Session = sessionmaker(bind=engine)
session = Session()

shows_table = Table('shows', metadata, autoload=True)
shows_columns = ['id', 'feed_url', 'title', 'subtitle', 'description', 'summary', 'author', 'email', 'link', 'language', 'explicit', 'image', 'category', 'subcategory', 'created_at', 'last_build_date']

episodes_table = Table('episodes', metadata, autoload=True)
episodes_columns = ['id', 'show_id', 'title', 'link', 'guid', 'subtitle', 'description', 'summary', 'author', 'audio_url', 'audio_file_size', 'audio_mime_type', 'category', 'explicit', 'length', 'pub_date', 'keywords']

select = session.query(shows_table)
with io.open('data/shows.csv', 'wb') as fh:
	writer = UnicodeWriter(fh, encoding='utf-8')
	writer.writerow(shows_columns)
	
	for row in page_query(select):
		write_row(row, writer, shows_columns)

select = session.query(episodes_table)
with io.open('data/episodes.csv', 'wb') as fh:
	writer = UnicodeWriter(fh, encoding='utf-8')
	writer.writerow(episodes_columns)
	
	for row in page_query(select):
		write_row(row, writer, episodes_columns)
