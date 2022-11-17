import psycopg2
import os
import csv
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

    def create_influenza_stat_table(self):
        """Create table in db for influenza statistics data."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE influenza_stat (
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
        return max_week_number

    def close(self):
        if self.connection:
            self.connection.close()

    def __exit__(self):
        self.close()


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
