import time
import os
import pandas as pd
import numpy as np
from datetime import date, datetime
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
from postgres import DB
from dotenv import load_dotenv
from matplotlib.figure import Figure
from matplotlib.ticker import (MultipleLocator, AutoMinorLocator)
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor


class GoogleTrendsApi:
    def __init__(self, date_start: str = "", date_end: str = ""):
        """date_start and date_end are date strings as "YYYY-MM-DD"."""
        if date_start:
            self.date_start = date.fromisoformat(date_start)
        else:
            self.date_start = date.today().replace(month=1, day=1)
        if date_end:
            self.date_end = date.fromisoformat(date_end)
        else:
            self.date_end = date.today().replace(month=12, day=31)

    def update_phrase_statistics(self, limit=1000):
        """Downloads new phrase search statistics."""
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        phrases = []
        today = date.today()
        year = today.year
        week = int(today.strftime("%W")) - 1
        if week < 1:
            year = year - 1
            week = 52
        if conn_string:
            with DB(conn_string) as db:
                phrases = db.get_new_google_trends_phrases(year, week, limit)
        for phrase in phrases:
            self.get_phrase_statistics(phrase)
        if conn_string:
            with DB(conn_string) as db:
                db.delete_duplicates_from_google_trends_stat_table()

    def update_phrase_statistics_multithread(self, limit=1000):
        """Multithreading version of update_phrase_statistics."""
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        phrases = []
        today = date.today()
        year = int(today.year)
        week = int(today.strftime("%W")) - 1
        if week < 1:
            year = year - 1
            week = 52
        if conn_string:
            with DB(conn_string) as db:
                phrases = db.get_new_google_trends_phrases(year, week, limit)
        with ThreadPoolExecutor() as executor:
            executor.map(self.get_phrase_statistics, phrases)
        if conn_string:
            with DB(conn_string) as db:
                db.delete_duplicates_from_google_trends_stat_table()

    def get_phrase_statistics(self, phrase: str):
        """Downloads phrases search statistics from Google trends and writes it in db."""
        df = self._get_phrase_statistics_df(phrase)
        df = df.reset_index()  # Set all column names in one line.
        if not df.empty:
            self.save_data_to_db(df)

    def _get_phrase_statistics_df(self, phrase: str):
        """Returns dataframe with phrase statistics from Google trends."""
        time_frame = self.date_start.strftime("%Y-%m-%d") + ' ' + self.date_end.strftime("%Y-%m-%d")
        phrases = [phrase]
        phrase_stat_df = pd.DataFrame()
        timer = 0
        timestep = 20
        timeout = 100
        while timer < timeout:
            try:
                pytrend = TrendReq(hl='RU', tz=3)
                pytrend.build_payload(phrases, timeframe=time_frame, geo='RU')
                phrase_stat_df = pytrend.interest_over_time()
                if not phrase_stat_df.empty:
                    return phrase_stat_df
                else:
                    print(f"Google trends has no data for phrase '{phrase}'")
                    return phrase_stat_df
            except ResponseError as e:
                print("Error: " + str(e))
            time.sleep(timestep)
            timer += timestep
        return phrase_stat_df

    def save_data_to_db(self, df):
        """Save statistics data from dataframe to google_trends_stat table in db."""
        year = self.date_start.year
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.isocalendar().week
        # Change numbers because weeks in downloaded df starts with Sunday
        if df['date'][0] == 52:
            df['date'][0] = 0
            df['date'] += 1
        phrase = df.columns[1]
        data = []
        for index, row in df.iterrows():
            data.append((year, row['date'], phrase, row[phrase]))
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                db.insert_values_into_google_trends_stat_table(data)
                print(f"Google trends stat for phrase '{phrase}' saved into db.")


def get_google_trends_plot_by_week(phrase: str, year: int = 2022, start_week: int = 1, end_week: int = 52):
    """Returns bytes buffer with graph of phrase google search statistics by week,
            starting from week number start_week to week number end_week."""
    weeks, shows_percents = [], []
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            weeks, shows_percents = db.get_google_trends_stat_plot_data(phrase, year, start_week, end_week)
    if not (weeks and shows_percents):  # DB has no requested data.
        return None
    weeks = np.array(weeks)
    shows_percents = np.array(shows_percents)
    fig = Figure(figsize=(7, 3.8), layout='constrained')
    ax = fig.subplots()
    ax.plot(weeks, shows_percents, linewidth=2.5)  # Plot some data on the axes.
    ax.set_xlabel('Недели')  # Add an x-label to the axes.
    ax.set_ylabel('Процент от максимального числа показов')  # Add a y-label to the axes.
    ax.set_title(f"Статистика показов фразы '{phrase}' в Google поиске за {year} г.")  # Add a title to the axes.
    # Make a plot with major ticks that are multiples of 5 and minor ticks that
    # are multiples of 5 if weeks number > 14, else with only major ticks that are multiples of 1.
    if len(weeks) > 14:
        maj_locator_freq = 5
    else:
        maj_locator_freq = 1
    ax.xaxis.set_major_locator(MultipleLocator(maj_locator_freq))
    if maj_locator_freq > 4:
        # For the minor ticks, use no labels.
        ax.xaxis.set_minor_locator(AutoMinorLocator(5))
    ax.grid(True)
    image_buf = BytesIO()
    fig.savefig(image_buf, format="jpeg")
    return image_buf


def get_google_trends_plot_by_month(phrase: str, year: int = 2022, start_month: int = 1, end_month: int = 12):
    """Returns bytes buffer with graph of phrase google search statistics by month,
    starting from start_month to end_month."""
    start_week = date(year, start_month, 1).strftime("%W")
    end_week = date(year, end_month, 28).strftime("%W")
    weeks, shows_percents = [], []
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            weeks, shows_percents = db.get_google_trends_stat_plot_data(phrase, year, start_week, end_week)
    if not (weeks and shows_percents):  # DB has no requested data.
        return None
    months_shows = {}
    for week_num, shows_percent in zip(weeks, shows_percents):
        if week_num == 0:
            continue
        date_string = f"{year}-W{week_num}-1"
        date_from_week = datetime.strptime(date_string, "%Y-W%W-%w")
        month = date_from_week.month
        if month not in months_shows:
            months_shows[month] = shows_percent
        else:
            months_shows[month] += shows_percent
    # Removing the month before start_month, which may have been added
    # since the first week may start in the previous month.
    if (start_month - 1) in months_shows:
        del months_shows[start_month - 1]
    months = np.array(list(months_shows.keys()))
    shows_percents = np.array(list(months_shows.values()))
    # shows_percents values normalization
    shows_percents = np.around(shows_percents / np.amax(shows_percents) * 100).astype(int)
    fig = Figure(figsize=(7, 3.8), layout='constrained')
    ax = fig.subplots()
    ax.plot(months, shows_percents, linewidth=2.5)  # Plot some data on the axes.
    ax.set_xlabel('Месяцы')  # Add an x-label to the axes.
    ax.set_ylabel('Процент от максимального числа показов')  # Add a y-label to the axes.
    ax.set_title(f"Статистика показов фразы '{phrase}' в Google поиске за {year} г.")  # Add a title to the axes.
    ax.xaxis.set_major_locator(MultipleLocator(1))
    ax.grid(True)
    image_buf = BytesIO()
    fig.savefig(image_buf, format="jpeg")
    return image_buf

