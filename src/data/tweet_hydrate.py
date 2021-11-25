import pandas as pd
from twarc.client2 import Twarc2
from twarc.expansions import ensure_flattened
import jsonlines

# Read the data locally
df = pd.read_csv('data/final_tweet_ids.csv')

# Create Twarc2 token object
t = Twarc2(
    bearer_token="AAAAAAAAAAAAAAAAAAAAAH7EUgEAAAAAXlhiY1qAjCATms6B4P5qED9b8nQ"
                 "%3D6mmTiPoOL87i2YV330s437ayHBqbD2bF9G0WKWfSfFDl11f06a")


# Function to rehydrate tweets
# Parameters: DataFrame: DataFrame TweetIDs, starting_tweet_index: Index to start hydrating
# Returns: Tweet Generator based on Twitter API
def rehydrate_tweets(input_df, starting_tweet_index=0):
    results = t.tweet_lookup(input_df.tweet_id[starting_tweet_index:])
    return results


# User function to rehydrate tweets and return generator
search_results = rehydrate_tweets(df)


# Write the tweets into a jsonl file
with jsonlines.open('data/out.jsonl', mode='w') as writer:
    # Get all results page by page:
    for page in search_results:
        # "Flatten" results returning 1 tweet at a time, with expansions inline:
        for tweet in ensure_flattened(page):
            # Write the tweet as a line of jsonl
            writer.write(tweet)