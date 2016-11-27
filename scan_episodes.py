#!/usr/bin/env python
from __future__ import print_function
import requests, glob, sys, os, datetime, json, uuid, sqlalchemy, re, hashlib
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, select, MetaData, Table, Column, String, Integer, DateTime
from config import conn_string
from podcast_lib import PodcastLib

engine = create_engine(conn_string)
db_conn = engine.connect()
metadata = MetaData(engine)

shows_table = Table('shows', metadata,
	Column('id', String(36), primary_key=True),
	Column('feed_url', String(255)),
)

episodes_table = Table('episodes', metadata,
	Column('id', String(36), primary_key=True),
	Column('show_id', String(36)),
	Column('title', String(255)),
	Column('link', String(255)),
	Column('guid', String(255)),
	Column('subtitle', String()),
	Column('description', String()),
	Column('summary', String()),
	Column('author', String(255)),
	Column('audio_url', String(255)),
	Column('audio_file_size', Integer),
	Column('audio_mime_type', String(50)),
	Column('category', String(200)),
	Column('explicit', Integer),
	Column('length', Integer),
	Column('pub_date', DateTime()),
	Column('keywords', String(800))
)

s = select([shows_table])
shows = db_conn.execute(s)

for show in shows:
	try:
		obj = PodcastLib.process_feed(show[1].encode('utf-8'), process_episodes=True)
		if obj:
			for episode in obj['episodes']:
				PodcastLib.save_to_db(db_conn, episodes_table, episode)
	
	except KeyboardInterrupt:
		sys.exit(0)
