import dask.dataframe as dd
import re

tweets = dd.read_json('data/out_practice.jsonl')


# the proportion of tweets that contain a URL


def find_url(string):
    # findall() has been used
    # with valid conditions for urls in string
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s(" \
            r")<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’])) "
    url = re.findall(regex, string)
    return [x[0] for x in url]


urls = tweets.text.apply(find_url, meta=('text', 'object'))


def check_links(link_list):
    return len(link_list) > 0


print('The percentage of tweets with links are: ', urls.apply(check_links, meta=('text', 'bool')).mean().compute())

# the number of unique users
print('There number of unique users are: ', len(tweets.author_id.unique()))
print('The total amount of tweets are: ', len(tweets))


# the proportion of the data that are retweets from text
def is_retweet(text):
    return text[:2] == 'RT'


# Get the percentage of retweets
retweets_percentage = tweets.text.apply(is_retweet, meta=('text', 'bool')).mean().compute()

# Print the percentage of retweets
print('The percentage of tweets that are retweets are: ', retweets_percentage)


# the proportion of the data that are retweets from referenced_tweet
def is_retweet_v2(lst):
    if type(lst) == list:
        return lst[0]['type'] == 'retweeted'
    return False


# Get the percentage of retweets pt2
retweets_percentage_v2 = tweets.referenced_tweets.apply(is_retweet_v2,
                                                        meta=('referenced_tweets', 'bool')).mean().compute()

# Print the percentage of retweets
print('The percentage of tweets that are retweets are: ', retweets_percentage_v2)