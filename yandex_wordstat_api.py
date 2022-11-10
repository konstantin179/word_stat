import urllib.request
import json
import time
from urllib.error import URLError
from typing import List


class WordStatApiClient:
    """Class represents a client to work with Yandex WordStat API."""
    def __init__(self, token):
        self.token = token
        self.url = 'https://api.direct.yandex.ru/v/json/'

    def get_phrase_statistics(self, phrase: str, geo_id: List[int, ] = None):
        """Downloads phrase search statistics from Yandex WordStat API and writes it in db.
        Args:
            phrase: a phrase for which statistics are needed;
            geo_id:  a list of region IDs. Allows you to get statistics of search queries made
            only in the specified regions. Defaults to None - statistics are issued for all regions.
        """
        report_id = self.request_report(phrase, geo_id)
        if not report_id:
            return None
        timer = 0
        timestep = 20
        timeout = 90    # On average, generating reports takes about one minute.
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

    def request_report(self, phrase: str, geo_id: List[int, ] = None):
        """Requests report from Yandex WordStat.
        Returns report ID."""
        data = {
            "method": "CreateNewWordstatReport",
            'token': self.token,
            'locale': 'ru',
            "param": {
                "Phrases": [phrase],
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

    def add_phrase_statistics_to_db(self, report):
        """Writes phrase statistics from report to db."""
        pass

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
