#!/usr/bin/env python
from __future__ import print_function
import glob, sys, os, sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime
from config import conn_string
from podcast_lib import PodcastLib

engine = create_engine(conn_string)
db_conn = engine.connect()
metadata = MetaData(engine)

shows_table = Table('shows', metadata,
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
				
				if line == '':
					continue
				
				try:
					obj = PodcastLib.process_feed(line)
					if obj:
						PodcastLib.save_to_db(db_conn, shows_table, obj)
				
				except KeyboardInterrupt:
					with open('checkpoint.log', 'w') as chk:
						chk.write(line)
					sys.exit(0)
				
				except:
					with open('checkpoint.log', 'w') as chk:
						chk.write(line)
					
					raise
