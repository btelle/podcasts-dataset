#!/usr/bin/env python
from __future__ import print_function
import requests, glob, sys, os, datetime, json, uuid, sqlalchemy, re, hashlib
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime
from config import conn_string

dtd = '{http://www.itunes.com/DTDs/Podcast-1.0.dtd}'.lower()
fields = [
	'feed_url',
	'title',
	'subtitle',
	'description',
	'summary',
	'author',
	'email',
	'link',
	'language'
]

tmp_dir = '/tmp/podcasts/feeds/'

engine = create_engine(conn_string)
db_conn = engine.connect()
metadata = MetaData(engine)

shows = Table('shows', metadata,
	Column('id', String(36), primary_key=True),
	Column('feed_url', String(255)),
	Column('title', String(255)),
	Column('subtitle', String()),
	Column('description', String()),
	Column('summary', String()),
	Column('author', String(250)),
	Column('email', String(255)),
	Column('link', String(255)),
	Column('language', String(10)),
	Column('explicit', Integer),
	Column('image', String(500)),
	Column('category', String(200)),
	Column('subcategory', String(200)),
	Column('created_at', DateTime()),
	Column('last_build_date', DateTime())
)

def parse_xml(xml):
	parser = ET.XMLParser(encoding="utf-8")
	tree = ET.fromstring(xml, parser=parser)
	obj = {}
	
	for channel in tree.findall('channel'):
		for child in channel:
			tag = child.tag.lower().replace(dtd, '').split('}')[-1]
			
			if tag in fields:
				try:
					obj[tag] = child.attrib if (child.attrib and type(child.attrib) == str) else child.text.strip() if child.text else ''	
				except AttributeError:
					obj[tag] = ''
				
			elif tag == 'lastbuilddate':
				try:
					if re.search('[+-][0-9]+$', child.text.strip()):
						dt = datetime.datetime.strptime(child.text.strip()[0:-5].strip(), '%a, %d %b %Y %H:%M:%S')
					else:
						dt = datetime.datetime.strptime(child.text.strip().strip(), '%a, %d %b %Y %H:%M:%S')
				except (ValueError,AttributeError):
					dt = datetime.datetime.now()
				
				obj['last_build_date'] = dt.strftime('%Y-%m-%d %H:%M:%S')
			
			elif tag == 'owner':
				if child.find(dtd+'email') is not None:
					obj['email'] = child.find(dtd+'email').text
			
			elif tag == 'category':
				tag = 'category' if 'category' not in obj else 'subcategory'
				if child.attrib and 'text' in child.attrib:
					obj[tag] = child.attrib['text']
				elif type(child.text) == str:
					obj[tag] = child.text.strip()
			
			elif tag == 'explicit':
				obj['explicit'] = child.text.strip().lower() == 'yes'
			
			elif tag == 'image':
				if child.find('url') is not None:
					obj['image'] = child.find('url').text
				elif type(child.attrib) == dict and 'href' in child.attrib:
					obj['image'] = child.attrib['href']
				elif child.text and child.text.strip() != '':
					obj['image'] = child.text.strip()
	
	obj['created_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	
	if 'last_build_date' not in obj:
		obj['last_build_date'] = obj['created_at']
	
	if 'language' in obj:
		obj['language'] = obj['language'].lower()
	
	if 'email' in obj:
		obj['email'] = obj['email'][0:255]
	
	if 'image' in obj and type(obj['image']) != str:
		print(obj['image'])
		if type(obj['image']) == dict and 'href' in obj['image']:
			obj['image'] = obj['image']['href']
		else:
			obj['image'] = ''
	
	if 'link' in obj and type(obj['link']) != str:
		if type(obj['link']) == dict and 'href' in obj['link']:
			obj['link'] = obj['link']['href']
		else:
			obj['link'] = ''
	
	return obj

def save_to_db(obj):
	ins = shows.insert(obj)
	try:
		db_conn.execute(ins)
		return True
	except sqlalchemy.exc.IntegrityError:
		print('Non-unique feed, skipping')
	except sqlalchemy.exc.CompileError:
		print('Table error')
		print(ins.compile().params)
		raise
	except sqlalchemy.exc.OperationalError:
		print('Missing required field')
	except:
		print('Unexpected exception')
		print(ins.compile().params)
		raise
	return False

def save_to_file(filename, contents):
	if not os.path.exists(filename.rsplit('/', 1)[0]):
		os.mkdir(filename.rsplit('/', 1)[0])
	
	with open(filename, 'wb') as fh:
		fh.write(contents)
	
	return True
	
def process_feed(url):
	try:
		feed_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
		tmp_location = tmp_dir+feed_id+'/'+hashlib.sha256(url.lower()).hexdigest()+'.xml'

		if os.path.exists(tmp_location):
			print('cache hit')
			with open(tmp_location, 'rb') as fh:
				contents = fh.read()
		else:
			req = requests.get(url, timeout=30)
			contents = req.content
	except (requests.exceptions.ConnectionError, 
			requests.exceptions.TooManyRedirects, 
			requests.exceptions.ReadTimeout):
		print('Connection error')
		return False
	
	if os.path.exists(tmp_location) or req.status_code == 200:
		try:
			obj = parse_xml(contents)
			obj['feed_url'] = url.lower()
			obj['id'] = feed_id
			
			if save_to_file(tmp_location, contents) and save_to_db(obj):
				return obj
			return False
			
		except ET.ParseError as e:
			print('Bad XML document, parse failed.')
			print(e)
		
	return False

if __name__ == '__main__':
	with open('checkpoint.log', 'r') as chk:
		checkpoint = chk.read()
	
	for f in glob.glob('data/*.txt'):
		with open(f) as fh:
			for line in fh.read().split("\n"):
				# Failed run checkpoint 
				if checkpoint != '' and checkpoint.strip() != line.strip():
					continue
				else:
					checkpoint = ''
				
				try:
					process_feed(line)
				except KeyboardInterrupt:
					with open('checkpoint.log', 'w') as chk:
						chk.write(line)
					sys.exit(0)
				except:
					with open('checkpoint.log', 'w') as chk:
						chk.write(line)
					
					raise
