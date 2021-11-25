def test_tweets(num_tweets):
    tweet_num = 1
    with open("data/out.jsonl", "r") as all_tweets:
        with open("test/testdata/test_tweets.jsonl", "a") as test_tweets:
            for i in all_tweets:
                if tweet_num <= num_tweets:
                    test_tweets.write(i)
                    tweet_num += 1
                else:
                    break