import csv
import os
from postgres import DB
from dotenv import load_dotenv
from google_trends_api import GoogleTrendsApi
from yandex_wordstat_api import WordStatApiClient


def get_phrases_from_csv(filepath):
    phrases = []
    with open(filepath) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        print(next(csv_reader))
        for row in csv_reader:
            if row[2]:
                phrases.append({"phrase": row[2]})
            if row[6]:
                phrases.append({"phrase": row[6]})
            if row[10]:
                phrases.append({"phrase": row[10]})
            if row[14]:
                phrases.append({"phrase": row[14]})
            if row[18]:
                phrases.append({"phrase": row[18]})
            if row[22]:
                phrases.append({"phrase": row[22]})
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            db.insert_values_into_phrases_table(phrases)
            db.delete_duplicates_from_phrases_table()


if __name__ == '__main__':
    get_phrases_from_csv("keywords.csv")

    gt = GoogleTrendsApi()
    gt.update_phrase_statistics_multithread()

    word_stat_client = WordStatApiClient()
    word_stat_client.update_phrase_statistics()
