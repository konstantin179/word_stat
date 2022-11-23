import psycopg2
import os
import traceback
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from typing import List


class DB:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.connection = None
        self.connect()

    def connect(self):
        if not self.connection:
            try:
                self.connection = psycopg2.connect(self.connection_string)
            except (Exception, psycopg2.Error) as error:
                print("PostgreSQL error:", error)
        return self.connection

    def __enter__(self):
        return self

    def create_phrases_table(self):
        """Create table in db for requested phrases."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS phrases (
                                                     id SERIAL PRIMARY KEY,
                                                     phrase VARCHAR
                            );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def insert_values_into_phrases_table(self, data: List[dict]):
        """Inserts requested phrases into phrases table."""
        try:
            cursor = self.connection.cursor()
            execute_values(cursor,
                           "INSERT INTO phrases (phrase) VALUES %s",
                           data, template="(%(phrase)s)")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def get_phrases(self):
        """Returns list of phrases from phrases table."""
        phrases = []
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT phrase FROM phrases;"""
            cursor.execute(query)
            for row in cursor.fetchall():
                phrases.append(row[0])
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return phrases

    def delete_duplicates_from_phrases_table(self):
        """Delete duplicates from phrases table."""
        try:
            cursor = self.connection.cursor()
            delete_query = """DELETE FROM phrases 
                               WHERE ctid IN 
                                    (SELECT ctid 
                                       FROM (SELECT ctid,
                                                    row_number() OVER (PARTITION BY phrase
                                                    ORDER BY id DESC) AS row_num
                                               FROM phrases
                                            ) t
                                      WHERE t.row_num > 1
                                    );"""
            cursor.execute(delete_query)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as e:
            print("PostgreSQL error:", e)

    def create_google_trends_stat_table(self):
        """Create table in db for google trends stat data."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS google_trends_stat (
                                             id SERIAL PRIMARY KEY,
                                             year INT,
                                             week_number INT,
                                             phrase VARCHAR,
                                             shows_percent INT
                    );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def insert_values_into_google_trends_stat_table(self, data):
        """Inserts statistics data into google_trends_stat table."""
        try:
            cursor = self.connection.cursor()
            execute_values(cursor,
                           "INSERT INTO google_trends_stat (year, week_number, phrase, shows_percent) VALUES %s",
                           data)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def get_new_google_trends_phrases(self, year, week_number):
        """Returns list of new phrases to get statistics from Google trends.
        Arguments:
            year - current year,
            week - week number of previous week."""
        new_phrases = []
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT phrase
                          FROM phrases
                         WHERE phrase NOT IN (SELECT phrase
                                                FROM (SELECT phrase, MAX(week_number) max_w_n
                                                        FROM google_trends_stat
                                                       WHERE year = {year}
                                                       GROUP BY phrase) t
                                               WHERE t.max_w_n >= {week_number});"""
            cursor.execute(query)
            for row in cursor.fetchall():
                new_phrases.append(row[0])
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return new_phrases

    def delete_duplicates_from_google_trends_stat_table(self):
        """Delete duplicates from google_trends_stat table."""
        try:
            cursor = self.connection.cursor()
            delete_query = """DELETE FROM google_trends_stat 
                               WHERE ctid IN 
                                    (SELECT ctid 
                                       FROM (SELECT ctid,
                                                    row_number() OVER (PARTITION BY year, week_number, phrase
                                                    ORDER BY id DESC) AS row_num
                                               FROM google_trends_stat
                                            ) t
                                      WHERE t.row_num > 1
                                    );"""
            cursor.execute(delete_query)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as e:
            print("PostgreSQL error:", e)

    def get_google_trends_stat_plot_data(self, phrase, year, start_week, end_week):
        """Returns weeks and shows percents for given phrase, year and weeks interval."""
        weeks, shows_percents = [], []
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT week_number, shows_percent
                                  FROM google_trends_stat
                                 WHERE phrase='{phrase}' AND year={year} AND
                                       week_number BETWEEN {start_week} AND {end_week}
                                 ORDER BY week_number;"""
            cursor.execute(query)
            for week_number, shows_percent in cursor.fetchall():
                weeks.append(week_number)
                shows_percents.append(shows_percent)
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return weeks, shows_percents

    def create_yandex_word_stat_table(self):
        """Create table in db for yandex word stat data."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS yandex_word_stat (
                                             id SERIAL PRIMARY KEY,
                                             year INT,
                                             month INT,
                                             phrase VARCHAR,
                                             shows INT
                    );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def insert_values_into_yandex_word_stat_table(self, data: List[tuple]):
        """Inserts statistics data into yandex_word_stat table."""
        try:
            cursor = self.connection.cursor()
            execute_values(cursor,
                           "INSERT INTO yandex_word_stat (year, month, phrase, shows) VALUES %s",
                           data)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def get_new_yandex_word_stat_phrases(self, year, month, limit=1000):
        """Returns list of new phrases to get statistics from yandex word stat.
                Arguments:
                    year - current year,
                    month - number of previous month."""
        new_phrases = []
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT phrase
                          FROM phrases
                         WHERE phrase NOT IN (SELECT phrase
                                                FROM (SELECT phrase, MAX(month) max_month
                                                        FROM yandex_word_stat
                                                       WHERE year = {year}
                                                       GROUP BY phrase) t
                                               WHERE t.max_month >= {month})
                         LIMIT {limit};"""
            cursor.execute(query)
            for row in cursor.fetchall():
                new_phrases.append(row[0])
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return new_phrases

    def delete_duplicates_from_yandex_word_stat_table(self):
        """Delete duplicates from yandex_word_stat table."""
        try:
            cursor = self.connection.cursor()
            delete_query = """DELETE FROM yandex_word_stat 
                               WHERE ctid IN 
                                    (SELECT ctid 
                                       FROM (SELECT ctid,
                                                    row_number() OVER (PARTITION BY year, month, phrase
                                                    ORDER BY id DESC) AS row_num
                                               FROM yandex_word_stat
                                            ) t
                                      WHERE t.row_num > 1
                                    );"""
            cursor.execute(delete_query)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as e:
            print("PostgreSQL error:", e)

    def get_ya_word_stat_plot_data(self, phrase, year, start_month, end_month):
        """Returns months and shows numbers for given phrase, year and months interval."""
        months, shows = [], []
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT month, shows
                          FROM yandex_word_stat
                         WHERE phrase='{phrase}' AND year={year} AND month BETWEEN {start_month} AND {end_month}
                         ORDER BY month;"""
            cursor.execute(query)
            for month_number, shows_number in cursor.fetchall():
                months.append(month_number)
                shows.append(shows_number)
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return months, shows

    def create_influenza_stat_table(self):
        """Create table in db for influenza statistics data."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS influenza_stat (
                                     id SERIAL PRIMARY KEY,
                                     year INT,
                                     week_number INT,
                                     cases_number REAL
            );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def insert_values_into_influenza_stat_table(self, data: List[tuple]):
        """Inserts statistics data into influenza_stat table."""
        try:
            cursor = self.connection.cursor()
            execute_values(cursor,
                           "INSERT INTO influenza_stat (year, week_number, cases_number) VALUES %s",
                           data)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def get_max_week_number(self, year):
        max_week_number = None
        try:
            cursor = self.connection.cursor()
            query = f"SELECT MAX(week_number) FROM influenza_stat WHERE year={year};"
            cursor.execute(query)
            max_week_number = cursor.fetchone()[0]
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
            return "error"
        return max_week_number

    def get_influenza_stat_plot_data(self, year, start_week, end_week):
        """Returns weeks and cases numbers for given year and weeks interval."""
        weeks, cases = [], []
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT week_number, cases_number
                          FROM influenza_stat
                         WHERE year={year} AND week_number BETWEEN {start_week} AND {end_week}
                         ORDER BY week_number;"""
            cursor.execute(query)
            for week_number, cases_number in cursor.fetchall():
                weeks.append(week_number)
                cases.append(cases_number)
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return weeks, cases

    def create_last_ya_word_stat_req_table(self):
        """Create table in db for timestamp and phrases number of last request to yandex word stat API."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS last_ya_word_stat_req (
                                     id SERIAL PRIMARY KEY,
                                     request_date TIMESTAMP,
                                     phrases_number INT
            );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def insert_values_in_last_ya_word_stat_req_table(self, req_date, phrases_number):
        """Inserts last request info in db."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO last_ya_word_stat_req (request_date, phrases_number) VALUES (%s, %s)",
                           (req_date, phrases_number))
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def get_last_req_info(self):
        """Returns last request info from last_ya_word_stat_req table."""
        last_req_date = None
        last_req_phrases_number = None
        try:
            cursor = self.connection.cursor()
            query = f"""SELECT request_date, phrases_number
                          FROM last_ya_word_stat_req
                         ORDER BY request_date DESC
                         LIMIT 1;"""
            cursor.execute(query)
            last_req_date, last_req_phrases_number = cursor.fetchone()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return last_req_date, last_req_phrases_number

    def delete_old_rows_in_last_ya_word_stat_req_table(self):
        """Delete rows with old requests data."""
        try:
            cursor = self.connection.cursor()
            delete_query = """DELETE FROM last_ya_word_stat_req 
                               WHERE request_date < (SELECT MAX(request_date) FROM last_ya_word_stat_req);"""
            cursor.execute(delete_query)
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as e:
            print("PostgreSQL error:", e)

    def close(self):
        if self.connection:
            self.connection.close()

    def __exit__(self, exc_type, exc_value, tb):
        self.close()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            # return False # uncomment to pass exception through
        return True


if __name__ == "__main__":
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            db.create_phrases_table()
            db.create_influenza_stat_table()
            db.create_yandex_word_stat_table()
            db.create_google_trends_stat_table()
            db.create_last_ya_word_stat_req_table()
    else:
        print("DB connection string is not found.")
