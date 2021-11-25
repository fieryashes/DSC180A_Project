import dask.dataframe as dd
import re

tweets = dd.read_json('data/out_practice.jsonl')

def find_url(string):

    # findall() has been used
    # with valid conditions for urls in string
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.findall(regex,string)
    return [x[0] for x in url]

urls = tweets.text.apply(find_url)

def check_links(link_list):
    return len(link_list) > 0


print('The percentage of tweets with links are: ', urls.apply(check_links).mean().compute())