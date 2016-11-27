from __future__ import print_function
import requests, glob, sys, os, datetime, json, uuid, re, hashlib, sqlalchemy
import xml.etree.ElementTree as ET

class PodcastLib:
	show_fields = [
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
	
	episode_fields = [
		'title',
		'link',
		'guid',
		'subtitle',
		'description',
		'summary',
		'author',
		'category'
	]

	tmp_dir = '/tmp/podcasts/feeds/'
	dtd = '{http://www.itunes.com/DTDs/Podcast-1.0.dtd}'.lower()
	
	@staticmethod
	def process_episode(node):
		"""
		Parse an XML node into an episode object.
		
		Params:
			node -- An item node from parsed XML feed
		
		Returns:
			Episode object.
		"""
		
		obj = {}
		for child in node:
			tag = child.tag.lower().replace(PodcastLib.dtd, '').split('}')[-1]
			if tag in PodcastLib.episode_fields:
				try:
					obj[tag] = child.attrib if (child.attrib and type(child.attrib) == str) else child.text.strip() if child.text else ''	
				except AttributeError:
					obj[tag] = ''
			elif tag == 'enclosure':
				if 'url' in child.attrib:
					obj['audio_url'] = child.attrib['url']
				if 'length' in child.attrib:
					obj['audio_file_size'] = child.attrib['length'].replace(',', '')
				if 'type' in child.attrib:
					obj['audio_mime_type'] = child.attrib['type']
			
			elif tag == 'explicit':
				obj['explicit'] = child.text.strip().lower() == 'yes' if child.text is not None else 0
			
			elif tag == 'pubdate':
				try:
					if re.search('[+-][0-9]+$', child.text.strip()):
						dt = datetime.datetime.strptime(child.text.strip()[0:-5].strip(), '%a, %d %b %Y %H:%M:%S')
					else:
						dt = datetime.datetime.strptime(child.text.strip().strip(), '%a, %d %b %Y %H:%M:%S')
				except (ValueError,AttributeError):
					dt = None
				obj['pub_date'] = dt
		
			elif tag == 'duration': 
				if child.text and ':' in child.text:
					lengths = child.text.split(':')[::-1]
					duration = 0
			
					for i in range(0, len(lengths)):
						try:
							duration += max((i*60), 1) * int(float(lengths[i]))
						except (ValueError, TypeError):
							pass
				else:
					try:
						duration = int(child.text)
					except (ValueError, TypeError):
						duration = None

				obj['duration'] = duration
			
			if 'duration' not in obj:
				obj['duration'] = None
			
			if type(obj['duration']) is str and ':' in obj['duration']:
				obj['duration'] = None
			
			if 'description' in obj:
				obj['description'] = obj['description'][:255]
			
			if 'author' in obj:
				obj['author'] = obj['author'][:255]
			
			if 'audio_file_size' in obj:
				if type(obj['audio_file_size']) is str:
					try:
						int(obj['audio_file_size'])
					except ValueError:
						obj['audio_file_size'] = ''
				
				if obj['audio_file_size'] and int(obj['audio_file_size']) < 0:
					obj['audio_file_size'] = ''
			
		return obj
	
	@staticmethod
	def parse_xml(xml, process_episodes=False):
		"""
		Parse XML string into a show object
	
		Params:
			xml -- XML string to parse
			process_episodes -- If true, process episode items and add them to show object.
	
		Returns:
			A show object
		"""
		
		parser = ET.XMLParser(encoding="utf-8")
		tree = ET.fromstring(xml, parser=parser)
		obj = {}
	
		for channel in tree.findall('channel'):
			for child in channel:
				tag = child.tag.lower().replace(PodcastLib.dtd, '').split('}')[-1]
			
				if tag in PodcastLib.show_fields:
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
					if child.find(PodcastLib.dtd+'email') is not None:
						obj['email'] = child.find(PodcastLib.dtd+'email').text
			
				elif tag == 'category':
					tag = 'category' if 'category' not in obj else 'subcategory'
					if child.attrib and 'text' in child.attrib:
						obj[tag] = child.attrib['text']
					elif type(child.text) == str:
						obj[tag] = child.text.strip()
			
				elif tag == 'explicit':
					obj['explicit'] = child.text.strip().lower() == 'yes' if child.text is not None else 0
			
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
			obj['language'] = obj['language'].lower().split(',')[0][0:10]
	
		if 'email' in obj and obj['email']:
			obj['email'] = obj['email'][0:255]
		
		if 'category' in obj and obj['category']:
			obj['category'] = obj['category'][0:200]
	
		if 'author' in obj and obj['author']:
			obj['author'] = obj['author'][0:250]
		
		if 'title' in obj and obj['title']:
			obj['title'] = obj['title'][0:255]
		
		if 'image' in obj and obj['image']:
			obj['image'] = obj['image'][0:255]
	
		if 'image' in obj and type(obj['image']) != str:
			if type(obj['image']) == dict and 'href' in obj['image']:
				obj['image'] = obj['image']['href']
			else:
				obj['image'] = ''
	
		if 'link' in obj and type(obj['link']) != str:
			if type(obj['link']) == dict and 'href' in obj['link']:
				obj['link'] = obj['link']['href']
			else:
				obj['link'] = ''
		
		if process_episodes:
			obj['episodes'] = []

			for channel in tree.findall('channel'):
				for child in channel.findall('item'):
					try:
						obj['episodes'].append(PodcastLib.process_episode(child))
					except:
						print('could not process episode')
						raise
						
		return obj
	
	@staticmethod
	def save_to_file(filename, contents):
		"""
		Save file contents to tmp file.
		
		Params:
			filename -- Local file to write to.
			contents -- Binary file contents
		"""
		
		if not os.path.exists(filename.rsplit('/', 1)[0]):
			os.mkdir(filename.rsplit('/', 1)[0])
	
		with open(filename, 'wb') as fh:
			fh.write(contents)
	
		return True
	
	@staticmethod
	def process_feed(url, process_episodes=False):
		"""
		Download and process a feed URL.
	
		Params:
			url -- RSS Feed URL to process
			process_episodes -- Flag to determine if the parser will parse episode items. 
								Default false.
	
		Returns:
			A show object.
		"""
		
		try:
			feed_id = str(uuid.uuid3(uuid.NAMESPACE_URL, url))
			tmp_location = PodcastLib.tmp_dir+feed_id+'/'+hashlib.sha256(url.lower()).hexdigest()+'.xml'

			if os.path.exists(tmp_location):
				print('cache hit')
				with open(tmp_location, 'rb') as fh:
					contents = fh.read()
			else:
				req = requests.get(url, timeout=30)
				contents = req.content
				PodcastLib.save_to_file(tmp_location, contents)
		except (requests.exceptions.ConnectionError, 
				requests.exceptions.TooManyRedirects, 
				requests.exceptions.ReadTimeout,
				requests.exceptions.InvalidSchema,
				UnicodeDecodeError):
			print('Connection error')
			return False
	
		if os.path.exists(tmp_location) or req.status_code == 200:
			try:
				obj = PodcastLib.parse_xml(contents, process_episodes)
				obj['feed_url'] = url.lower()
				obj['id'] = feed_id
				
				if 'episodes' in obj:
					for i in range(0, len(obj['episodes'])):
						obj['episodes'][i]['show_id'] = obj['id']
						try:
							hash = hashlib.sha256(obj['episodes'][i]['audio_url'].decode('utf-8').lower() + obj['episodes'][i]['audio_file_size'].decode('UTF-8')).hexdigest()
							id = str(uuid.uuid3(uuid.NAMESPACE_URL, hash))
						except (KeyError, UnicodeEncodeError):
							id = str(uuid.uuid4())
						obj['episodes'][i]['id'] = id
				
				return obj
			
			except ET.ParseError as e:
				print('Bad XML document, parse failed.')
				print(e)
		
		return False
	
	@staticmethod
	def save_to_db(db_conn, tab, obj):
		"""
		Save an object to the database.
		
		Params:
			db_conn -- SQLAlchemy connection.
			tab -- Table definition.
			obj -- Data to insert.
		"""
		ins = tab.insert(obj)
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
			#raise
		except:
			print('Unexpected exception')
			print(ins.compile().params)
			raise
		return False
