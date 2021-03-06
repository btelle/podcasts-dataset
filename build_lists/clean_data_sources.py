#!/usr/bin/env python
from __future__ import print_function
import re, requests

def all_podcasts():
	with open('sources/allpodcasts_com_directory.html', 'r') as fh:
		contents = fh.read()

	with open('../data/allpodcasts_list.txt', 'w') as fh:
		for li in contents.split('</li><li>')[1:-1]:
			feed_url = re.search('href="(.*)"', li)
			if feed_url:
				fh.write(feed_url.group(1)+"\n")

def newtimeradio():
	with open('sources/newtimeradio_com.html', 'r') as fh:
		contents = fh.read()
	
	with open('../data/newtimeradio_list.txt', 'w') as fh:
		for match in re.findall('<a href="([^"]+)"><img src="podcast.gif" border="0"></a>', contents):
			if match and len(match) > 0:
				fh.write(match.strip()+"\n")

def publicradiofan():
	with open('sources/publicradiofan_com_podcast_directory.html', 'r') as fh:
		contents = fh.read()
	
	with open('../data/publicradiofan_list.txt', 'w') as fh:
		for match in re.findall('<A HREF="([^"]+)"><img src="/pod.gif" height=14 width=36 border=0 alt="podcast"></A>', contents):
			if match and len(match) > 0:
				fh.write(match.strip()+"\n")

def godcasts():
	base_url = 'http://www.godcast1000.com/index.php?cat=&start={0}'
	with open('../data/godcasts.txt', 'w') as fh:
		for i in range(1, 1101, 50):
			url = base_url.format(i)
			contents = requests.get(url).text
			
			for match in re.findall('Get RSS: <a href="([^"]+)">', contents):
				if match and len(match) > 0:
					fh.write(match.strip()+"\n")

all_podcasts()
newtimeradio()
publicradiofan()
godcasts()
