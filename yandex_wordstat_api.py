import urllib.request
import json
import time
import os
import numpy as np
from matplotlib.figure import Figure
from io import BytesIO
from urllib.error import URLError
from typing import List, Tuple
from postgres import DB
from dotenv import load_dotenv
from datetime import date, datetime, timedelta


class WordStatApiClient:
    """Class represents a client to work with Yandex WordStat API."""

    def __init__(self, token):
        self.token = token
        self.url = 'https://api.direct.yandex.ru/v/json/'

    def update_phrase_statistics(self):
        """Downloads new phrase search statistics."""
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        phrases_number = 0
        limit = 1000
        last_req_date = None
        last_req_phrases_number = None
        if conn_string:
            with DB(conn_string) as db:
                last_req_date, last_req_phrases_number = db.get_last_req_info()
        # Checking the API limit of 1000 requests per day.
        if last_req_date:
            last_req_date = datetime.fromisoformat(last_req_date)
            passed_time = datetime.now() - last_req_date
            if passed_time < timedelta(days=1):
                if last_req_phrases_number is not None:
                    limit = 1000 - last_req_phrases_number
                    if limit < 1:
                        return None
                    phrases_number += last_req_phrases_number
                else:
                    return None
        today = date.today()
        year = today.year
        month = today.month - 1
        if month < 1:
            year = year - 1
            month = 12
        phrases = []
        if conn_string:
            with DB(conn_string) as db:
                phrases = db.get_new_yandex_word_stat_phrases(year, month, limit)
        self.get_phrase_statistics(phrases)
        req_date = datetime.now()
        phrases_number += len(phrases)
        if conn_string:
            with DB(conn_string) as db:
                db.insert_values_in_last_ya_word_stat_req_table(req_date, phrases_number)
                db.delete_old_rows_in_last_ya_word_stat_req_table()

    def get_phrase_statistics(self, phrases: List[str], geo_id: List[int] = None):
        """Downloads phrase search statistics from Yandex WordStat API and writes it in db.
        Args:
            phrases: sequence of phrases for which statistics are needed;
            geo_id:  a list of region IDs. Allows you to get statistics of search queries made
            only in the specified regions. Defaults to None - statistics are issued for all regions.
        """
        for ten_phrases in chunks(phrases, 10):
            # Report cannot contain more than 10 phrases
            report_id = self.request_report(ten_phrases, geo_id)
            if not report_id:
                return None
            timer = 0
            timestep = 20
            timeout = 100  # On average, generating reports takes about one minute.
            ready = False
            while timer < timeout:
                time.sleep(timestep)
                timer += timestep
                ready = self._report_ready(report_id)
                if ready:
                    break
            if not ready:
                return None
            report = self.get_report(report_id)
            if not report:
                return None
            self.add_phrase_statistics_to_db(report)
            self.delete_report(report_id)
        # Delete duplicates from yandex_word_stat table
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                db.delete_duplicates_from_yandex_word_stat_table()

    def request_report(self, phrases: Tuple[str], geo_id: List[int] = None):
        """Requests report from Yandex WordStat.
        Returns report ID."""
        data = {
            "method": "CreateNewWordstatReport",
            'token': self.token,
            'locale': 'ru',
            "param": {
                "Phrases": phrases,
            }
        }
        if geo_id:
            data["param"]["GeoId"] = geo_id
        jdata = json.dumps(data, ensure_ascii=False).encode('utf8')
        report_id = None
        try:
            with urllib.request.urlopen(self.url, jdata) as response:
                res = json.loads(response.read().decode('utf8'))
            report_id = res["data"]
        except URLError as e:
            print("Error in request_report: " + str(e))
        return report_id

    def _report_ready(self, report_id):
        """Checks if report is ready.
        Returns True if ready, False if not."""
        data = {
            "method": "GetWordstatReportList",
            'token': self.token,
            'locale': 'ru',
        }
        jdata = json.dumps(data, ensure_ascii=False).encode('utf8')
        status = None
        try:
            with urllib.request.urlopen(self.url, jdata) as response:
                res = json.loads(response.read().decode('utf8'))
            reports_list = res["data"]
            for report in reports_list:
                if report["ReportID"] == report_id:
                    status = report["StatusReport"]
        except URLError as e:
            print("Error in _report_ready: " + str(e))
        if status == "Done":
            return True
        return False

    def get_report(self, report_id):
        """Get report from Yandex WordStat."""
        data = {
            "method": "GetWordstatReport",
            "param": report_id,
            'token': self.token,
            'locale': 'ru',
        }
        jdata = json.dumps(data, ensure_ascii=False).encode('utf8')
        report = None
        try:
            with urllib.request.urlopen(self.url, jdata) as response:
                res = json.loads(response.read().decode('utf8'))
            report = res["data"]
        except URLError as e:
            print("Error in get_report: " + str(e))
        return report

    @staticmethod
    def add_phrase_statistics_to_db(report):
        """Writes phrase statistics data from report to yandex_word_stat table in db."""
        data = []
        prev_month_date = date.today().replace(day=1) - timedelta(days=2)
        year = prev_month_date.year
        month = prev_month_date.month
        for rep_info in report:
            phrase = rep_info["SearchedWith"]["Phrase"]
            shows = rep_info["SearchedWith"]["Shows"]
            data.append((year, month, phrase, shows))
        load_dotenv()
        conn_string = os.getenv("DB_CONN_STR")
        if conn_string:
            with DB(conn_string) as db:
                db.insert_values_into_yandex_word_stat_table(data)

    def delete_report(self, report_id):
        """Delete report from Yandex WordStat API. It can have max 5 reports."""
        data = {
            "method": "DeleteWordstatReport",
            "param": report_id,
            'token': self.token,
            'locale': 'ru',
        }
        jdata = json.dumps(data, ensure_ascii=False).encode('utf8')
        try:
            with urllib.request.urlopen(self.url, jdata) as response:
                res = json.loads(response.read().decode('utf8'))
            if res["data"] == 1:
                print(f"Report {report_id} deleted successfully.")
        except URLError as e:
            print("Error in get_report: " + str(e))


def get_ya_word_stat_plot(phrase: str, year: int = 2022, start_month: int = 1, end_month: int = 12):
    """Returns bytes buffer with graph of phrase yandex search statistics by month,
    starting from start_month to end_month."""
    months, shows = [], []
    load_dotenv()
    conn_string = os.getenv("DB_CONN_STR")
    if conn_string:
        with DB(conn_string) as db:
            months, shows = db.get_ya_word_stat_plot_data(phrase, year, start_month, end_month)
    if not (months and shows):  # DB has no requested data.
        return None
    months = np.array(months)
    shows = np.array(shows)
    fig = Figure(figsize=(7, 3.8), layout='constrained')
    ax = fig.subplots()
    ax.plot(months, shows, linewidth=2.5)  # Plot some data on the axes.
    ax.set_xlabel('Месяцы')  # Add an x-label to the axes.
    ax.set_ylabel('Число показов')  # Add a y-label to the axes.
    ax.set_title(f"Статистика показов фразы '{phrase}' в Яндекс поиске за {year} г.")  # Add a title to the axes.
    ax.grid(True)
    image_buf = BytesIO()
    fig.savefig(image_buf, format="jpeg")
    return image_buf


def chunks(sequence, n):
    """Yield successive n-sized chunks from sequence."""
    for i in range(0, len(sequence), n):
        yield sequence[i:i + n]
