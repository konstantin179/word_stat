import requests
import re
import os
import numpy as np
import datetime
from matplotlib.figure import Figure
from matplotlib.ticker import (MultipleLocator, AutoMinorLocator)
from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from postgres import DB
from dotenv import load_dotenv


class InfluenzaStatParser:
    def __init__(self, year=2022):
        self.url = "https://www.influenza.spb.ru/system/epidemic_situation/laboratory_diagnostics/"
        self.year = str(year)

    def update_statistics_data(self):
        """Adds new statistics data into db."""
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                last_week_number = db.get_max_week_number(self.year)
        if last_week_number == "error":  # Error in database.
            return None
        week_numbers = self.get_week_numbers()
        data = []
        if not last_week_number:  # DB table has no data for this year.
            data = self.get_statistics_data_multithread(week_numbers)
        else:
            week_numbers = [week_num for week_num in week_numbers if int(week_num) > last_week_number]
            if week_numbers:  # Take data for new week numbers that are not in the database.
                data = self.get_statistics_data_multithread(week_numbers)
        if data:
            self.write_data_into_db(data)

    def get_statistics_data(self, week_numbers):
        """Returns list of tuples with year, week numbers and cases numbers."""
        data = []
        for week_number in week_numbers:
            cases_number = self.get_cases_per_week(week_number)
            data.append((int(self.year), int(week_number), cases_number))
        return data

    def get_statistics_data_multithread(self, week_numbers):
        """Multithreading version of get_statistics_data.
        Returns list of tuples with year, week numbers and cases numbers."""
        data = []
        with ThreadPoolExecutor() as executor:
            for week_and_cases_number in executor.map(self._get_cases_per_week_for_multithread, week_numbers):
                data.append((int(self.year), *week_and_cases_number))
        return data

    @staticmethod
    def write_data_into_db(data):
        """Writes statistics data to influenza_stat table in db."""
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                db.insert_values_into_influenza_stat_table(data)

    def get_week_numbers(self):
        """Returns list of available week numbers."""
        params = {"year": self.year, "week": "01"}
        week_numbers = []
        try:
            content = requests.get(self.url, params=params).text
            soup = BeautifulSoup(content, 'lxml')
            week_numbers = soup.find(id="id_week").text.split()
        except requests.exceptions.RequestException as e:
            print("Request error in get_week_numbers: " + str(e))
        return week_numbers

    def get_cases_per_week(self, week_number: str):
        """Return number of cases per 10 000 population per week."""
        params = {"year": self.year, "week": week_number}
        cases_number = None
        try:
            content = requests.get(self.url, params=params).text
            soup = BeautifulSoup(content, 'lxml')
            pattern = rf".*????.*{week_number}.*{self.year}.*?????????????? ???????????????????????????? ?????????????????? ???????? ?? ??????????????.*????????????.*"
            texts = soup.find_all(class_="bulletin__text")
            stat_text = ''
            for t in texts:
                if re.match(pattern, t.text):
                    stat_text = t.text
            # Searching for group with number of cases
            search_result = re.search(r"????????????.*?(\d+[,.]?\d*)\b\s????\s10", stat_text)
            if not search_result:
                return None
            cases_number = search_result.group(1)
            cases_number = float(cases_number.replace(',', '.'))
        except requests.exceptions.RequestException as e:
            print("Request error in get_week_numbers: " + str(e))
        return cases_number

    def _get_cases_per_week_for_multithread(self, week_number):
        cases_number = self.get_cases_per_week(week_number)
        week_number = int(week_number)
        return week_number, cases_number

    def get_plot_by_week(self, start_week: int = 1, end_week: int = 52):
        """Returns bytes buffer with graph of influenza statistics by week,
        starting from week number start_week to week number end_week."""
        weeks, cases = [], []
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                weeks, cases = db.get_influenza_stat_plot_data(self.year, start_week, end_week)
        if not (weeks and cases):  # DB has no requested data.
            return None
        weeks = np.array(weeks)
        cases = np.array(cases)
        fig = Figure(figsize=(7, 3.8), layout='constrained')
        ax = fig.subplots()
        ax.plot(weeks, cases, linewidth=2.5)  # Plot some data on the axes.
        ax.set_xlabel('????????????')  # Add an x-label to the axes.
        ax.set_ylabel('???????????????????????????? ???? 10 ??????. ??????.')  # Add a y-label to the axes.
        ax.set_title(f"???????????????? ???????????????????????????? ???????? ?? ?????????????? ???? ?????????????? ???? {self.year} ??.")  # Add a title to the axes.
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

    def get_plot_by_month(self, start_month: int = 1, end_month: int = 12):
        """Returns bytes buffer with graph of influenza statistics by month,
        starting from start_month to end_month."""
        start_week = datetime.date(int(self.year), start_month, 1).strftime("%W")
        end_week = datetime.date(int(self.year), end_month, 28).strftime("%W")
        weeks, cases = [], []
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                weeks, cases = db.get_influenza_stat_plot_data(self.year, start_week, end_week)
        if not (weeks and cases):  # DB has no requested data.
            return None
        months_cases = {}
        for week_num, cases_num in zip(weeks, cases):
            date_string = f"{self.year}-W{week_num}-1"
            date_from_week = datetime.datetime.strptime(date_string, "%Y-W%W-%w")
            month = date_from_week.month
            if month not in months_cases:
                months_cases[month] = cases_num
            else:
                months_cases[month] += cases_num
        # Removing the month before start_month, which may have been added
        # since the first week may start in the previous month.
        if (start_month - 1) in months_cases:
            del months_cases[start_month - 1]
        months = np.array(list(months_cases.keys()))
        cases = np.array(list(months_cases.values()))
        fig = Figure(figsize=(7, 3.8), layout='constrained')
        ax = fig.subplots()
        ax.plot(months, cases, linewidth=2.5)  # Plot some data on the axes.
        ax.set_xlabel('????????????')  # Add an x-label to the axes.
        ax.set_ylabel('???????????????????????????? ???? 10 ??????. ??????.')  # Add a y-label to the axes.
        ax.set_title(f"???????????????? ???????????????????????????? ???????? ?? ?????????????? ???? ?????????????? ???? {self.year} ??.")  # Add a title to the axes.
        ax.xaxis.set_major_locator(MultipleLocator(1))
        ax.grid(True)
        image_buf = BytesIO()
        fig.savefig(image_buf, format="jpeg")
        return image_buf
