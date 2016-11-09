import re, requests

def get_feed_from_page(url):
	html = requests.get(url).text
	match = re.search('<a href="([^"]+)" target="_blank"  class="icon-feed-producer  producer-social" title="Feed">', html)

	if match:
		return match.group(1)

def parse_search_page(url):
	html = requests.get(url).text
	results = []
	
	for match in re.findall('<a class="item_title" href="([^"]+)">', html):
		if match and len(match) > 0:
			results.append("https://www.podcastpedia.org"+match.strip())
	return results

base_url = 'https://www.podcastpedia.org/search/advanced_search/results?numberResultsPerPage=500&searchTarget=podcasts&categId={0}&searchMode=natural&currentPage=1'
categories = [
	45,
	39,
	24,
	48,
	27,
	43,
	38,
	46,
	22,
	44,
	42,
	28,
	35,
	37,
	49,
	21,
	29,
	41,
	25,
	31,
	33,
	47
]

with open('../data/podcastpedia_list.txt', 'w') as fh:
	for cat in categories:
		search_url = base_url.format(cat)
		for url in parse_search_page(search_url):
			feed_url = get_feed_from_page(url)
			if feed_url:
				fh.write(feed_url+"\n")
