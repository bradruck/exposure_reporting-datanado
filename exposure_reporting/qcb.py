import json
import urllib.request
import logging
from .exception import ConfigError


class QCBConnection(object):
    def __init__(self, url, project, listener):
        self.url = url
        self.project = project
        self.listener = listener

    def post(self, payload):
        """ Use HTTP Post Request to send QCB Payload to QCB
        Args:
            payload: JSON payload
        Returns:
            Response code
        """
        json_data = json.dumps(payload)
        data = json_data.encode("ascii")
        headers = {"Content-Type": "application/json",
                   "PROJECT": self.project,
                   "LISTENER": self.listener}
        req = urllib.request.Request(self.url, data, headers)
        try:
            response = urllib.request.urlopen(req, timeout=30)
            return response.getcode()
        except Exception as e:
            logging.log(30, "QCB Request failed.\n{}\nMoving on...".format(e))


def get_payload(issue, summaries):
    """ Constructs payload
    Args:
        issue: JIRA issue object
        summaries: List of OrderedDicts of summary values
    Returns:
        Payload
    """
    payload = {"issue": issue.key}
    all_reports = []
    for summary in summaries:
        report = {}
        report["id"] = summary["Campaign Name"]
        report["C"] = str(summary["Rows in Exposure File"])
        report["F"] = str(summary["Impressions in File"])
        report["G"] = str(summary["Customer IDs in File"])
        report["H"] = str(summary["Exposed Unique Customer IDs in File"])
        all_reports.append(report)

    payload["metrics"] = all_reports
    return payload
