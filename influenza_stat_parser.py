import requests
import re
import os
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
        week_numbers = self.get_week_numbers()
        if not last_week_number:
            self.get_statistics_data_multithread(week_numbers)
        else:
            week_numbers = [week_num for week_num in week_numbers if week_num > last_week_number]
            if week_numbers:
                self.get_statistics_data_multithread(week_numbers)

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
        """Writes statistics data to db."""
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
            pattern = rf".*На.*{week_number}.*{self.year}.*уровень заболеваемости населения ОРВИ и гриппом.*состав.*"
            texts = soup.find_all(class_="bulletin__text")
            stat_text = ''
            for t in texts:
                if re.match(pattern, t.text):
                    stat_text = t.text
            # Searching for group with number of cases
            search_result = re.search(r"состав.*?(\d+[,.]?\d*)\b\sна\s10", stat_text)
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


