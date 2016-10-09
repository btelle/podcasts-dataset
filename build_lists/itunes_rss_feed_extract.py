#!/usr/bin/env python
from __future__ import print_function
import requests, re, time

rss_template = 'https://itunes.apple.com/us/rss/toppodcasts/limit=100/genre=%s/explicit=true/json'
category_ids = [
	1301,
	1321,
	1303,
	1304,
	1323,
	1325,
	1307,
	1305,
	1310,
	1311,
	1314,
	1315,
	1324,
	1316,
	1318,
	1309
]

def get_podcast_id(url):
	if '?' in url:
		query_params = url.split('?')[1].split('&')
		for param in query_params:
			pair = param.split('=')
			if pair[0] == 'id':
				return pair[1]

	for part in url.split('?')[0].split('/'):
		if part[0:2] == 'id':
			return part[2:]

def get_feed_url(id):
	urls = ['http://itunes.apple.com/podcast/id','http://itunes.apple.com/WebObjects/DZR.woa/wa/viewPodcast?id=']
	headers = {'User-Agent': 'iTunes/10.1 (Windows; U; Microsoft Windows XP Home Edition Service Pack 2 (Build 2600)) DPI/96'}
	
	for url in urls:
		req = requests.get(url+id, headers=headers)
		feed_url = re.search('feed-url="([^"]+)"', req.text)
		
		if feed_url:
			return feed_url.group(1)

with open('../data/itunes_top_100.txt', 'a') as fh:
	for category in category_ids:
		rss_url = rss_template % category
		req = requests.get(rss_url)
		
		for row in req.json()['feed']['entry']:
			itunes_url = row['link']['attributes']['href']
			feed_url = get_feed_url(get_podcast_id(itunes_url))
			
			if feed_url:
				fh.write(feed_url+"\n")
		
			time.sleep(1)
		time.sleep(20)
