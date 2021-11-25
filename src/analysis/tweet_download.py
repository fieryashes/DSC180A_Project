import gzip
import os
import shutil

import pandas as pd
import wget

month_dict = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
              'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

day_dict = {'Jan': 31, 'Feb': 29, 'Mar': 31, 'Apr': 30, 'May': 31, 'Jun': 30, 'Jul': 31, 'Aug': 31, 'Sep': 30,
            'Oct': 31, 'Nov': 30, 'Dec': 31}

# Get Tweet IDs from 2020 April to December
ids_2020 = pd.DataFrame()

for month in month_dict.keys():

    # For 2020
    if 4 <= int(month_dict[month]) <= 12:
        for day in range(1, day_dict[month]):

            if day < 10:
                day = str(0) + str(day)

            date_string = '2020-' + month_dict[month] + '-' + str(day)

            URL = 'https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/' + date_string + '/' + date_string + '-dataset.tsv.gz'

            # Downloads the dataset (compressed in a GZ format)
            wget.download(URL, out='clean-dataset.tsv.gz')

            # Unzips the dataset and gets the TSV dataset
            with gzip.open('clean-dataset.tsv.gz', 'rb') as f_in:
                with open('clean-dataset.tsv', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Deletes the compressed GZ file
            os.unlink("clean-dataset.tsv.gz")

            # Gets all possible languages from the dataset
            df = pd.read_csv('clean-dataset.tsv', sep="\t")

            # Sample as we go
            ids_2020 = ids_2020.append(df.sample(frac=.1))

# ids_2020.to_csv('2020_tweet_ids.csv')

# Get Tweet IDs from March
march_2020 = pd.DataFrame()

# Get March dates (3/22 to 3/31)
for day in range(22, 31):

    if day < 10:
        day = str(0) + str(day)

    date_string = '2020-' + '03' + '-' + str(day)

    URL = 'https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/' + date_string + '/' + date_string + '-dataset.tsv.gz'

    # Downloads the dataset (compressed in a GZ format)
    wget.download(URL, out='clean-dataset.tsv.gz')

    # Unzips the dataset and gets the TSV dataset
    with gzip.open('clean-dataset.tsv.gz', 'rb') as f_in:
        with open('clean-dataset.tsv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Deletes the compressed GZ file
    os.unlink("clean-dataset.tsv.gz")

    # Gets all possible languages from the dataset
    df = pd.read_csv('clean-dataset.tsv', sep="\t")

    # Sample as we go
    march_2020 = march_2020.append(df.sample(frac=.1))

# Concat dataframes for 2020
ids_2020 = ids_2020[['tweet_id', 'date', 'time']]

march_2020 = march_2020[['tweet_id', 'date', 'time']]

final_ids_2020 = pd.concat([march_2020, ids_2020], axis=0)

# Get Tweet IDs from 2021 January to September
ids_2021 = pd.DataFrame()

for i in month_dict.keys():

    # For 2021
    if 1 <= int(month_dict[i]) <= 9:
        for day in range(1, day_dict[i]):

            # d.strftime('X%d/X%m/%Y').replace('X0','X').replace('X','')

            if day < 10:
                day = str(0) + str(day)

            date_string = '2021-' + month_dict[i] + '-' + str(day)

            URL = 'https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/' + date_string + '/' + date_string + '-dataset.tsv.gz'

            # Downloads the dataset (compressed in a GZ format)
            wget.download(URL, out='clean-dataset.tsv.gz')

            # Unzips the dataset and gets the TSV dataset
            with gzip.open('clean-dataset.tsv.gz', 'rb') as f_in:
                with open('clean-dataset.tsv', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # Deletes the compressed GZ file
            os.unlink("clean-dataset.tsv.gz")

            # Gets all possible languages from the dataset
            df = pd.read_csv('clean-dataset.tsv', sep="\t")

            # Sample as we go
            ids_2021 = ids_2021.append(df.sample(frac=.1))


# Get Tweet IDs from March
october_2021 = pd.DataFrame()

# Get October dates (10/01 to 10/10)
for day in range(1, 10):

    if day < 10:
        day = str(0) + str(day)

    date_string = '2021-' + '10' + '-' + str(day)

    URL = 'https://github.com/thepanacealab/covid19_twitter/raw/master/dailies/' + date_string + '/' + date_string + '-dataset.tsv.gz'

    # Downloads the dataset (compressed in a GZ format)
    wget.download(URL, out='clean-dataset.tsv.gz')

    # Unzips the dataset and gets the TSV dataset
    with gzip.open('clean-dataset.tsv.gz', 'rb') as f_in:
        with open('clean-dataset.tsv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Deletes the compressed GZ file
    os.unlink("clean-dataset.tsv.gz")

    # Gets all possible languages from the dataset
    df = pd.read_csv('clean-dataset.tsv', sep="\t")

    # Sample as we go
    october_2021 = october_2021.append(df.sample(frac=.1))

# Concat dataframes for 2020
ids_2021 = ids_2021[['tweet_id', 'date', 'time']]

october_2021 = october_2021[['tweet_id', 'date', 'time']]

final_ids_2021 = pd.concat([ids_2021, october_2021], axis=0)

final_ids = pd.concat([final_ids_2020, final_ids_2021], axis=0)

final_ids = final_ids.sample(2000000, random_state=1)

final_ids.to_csv('data/final_tweet_ids.csv')

print('Finished')