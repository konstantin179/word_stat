import psycopg2
import os
import csv
import traceback
from psycopg2.extras import execute_values
from io import StringIO
from dotenv import load_dotenv


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
                                                     phrase VARCHAR,
                            );""")
            self.connection.commit()
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)

    def insert_values_into_phrases_table(self, data):
        """Inserts requested phrases into phrases table."""
        try:
            cursor = self.connection.cursor()
            execute_values(cursor,
                           "INSERT INTO phrases (phrase) VALUES %s",
                           data)
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
            for phrase in cursor.fetchall():
                phrases.append(phrase)
            cursor.close()
        except (Exception, psycopg2.Error) as error:
            print("PostgreSQL error:", error)
        return phrases

    def delete_duplicates_from_phrases_table(self):
        """Delete duplicates from phrases table."""
        try:
            cursor = self.connection.cursor()
            delete_query = f"""DELETE FROM phrases 
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

    def insert_values_into_yandex_word_stat_table(self, data):
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

    def delete_duplicates_from_yandex_word_stat_table(self):
        """Delete duplicates from yandex_word_stat table."""
        try:
            cursor = self.connection.cursor()
            delete_query = f"""DELETE FROM yandex_word_stat 
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
                         WHERE phrase={phrase} AND year={year} AND month BETWEEN {start_month} AND {end_month}
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

    def insert_values_into_influenza_stat_table(self, data):
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

    def close(self):
        if self.connection:
            self.connection.close()

    def __exit__(self, exc_type, exc_value, tb):
        self.close()
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            # return False # uncomment to pass exception through
        return True


# Method to inserting pandas DataFrame into db with df.to_sql()
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


if __name__ == "__main__":
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            db.create_influenza_stat_table()
    else:
        print("DB connection string is not found.")
