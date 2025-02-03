from jira import JIRA
import re
import logging
import pandas as pd
import io
import os
from datetime import datetime, timedelta
from exception import InputError, ConfigError


class Jira(object):
    def __init__(self, url, username, password):
        self._url = url
        self._username = username
        self._password = password
        self.conn = None

    def connect(self):
        """Sets connection to JIRA"""
        try:
            self.conn = JIRA(self._url, basic_auth=(self._username, self._password))
        except Exception as e:
            logging.log(40, "Unable to connect to JIRA -- please check URL, username, and password\n{}".format(e))
            raise ConfigError("JIRA authentication failure")

    def get_attachment(self, issue, keyword, extension='.xlsx'):
        """ Gets attachment containing keyword from ticket
        Args:
            issue: JIRA issue object
            keyword: String to find in attachment name
        Returns:
            attachment object
        """
        issue = self.conn.issue(issue.key)
        attachments = [(a.id, a.created) for a in issue.fields.attachment
                       if keyword.lower() in a.filename.lower()
                       and extension in a.filename]
        if attachments:
            latest = max(attachments, key=lambda x: x[1])
            return self.conn.attachment(latest[0]).get()
        else:
            self.conn.add_comment(issue, "Input-ADD Error: No attachment with '{}' in filename".format(keyword))
            raise InputError("No ADD attachment")

    def transition(self, issue, name):
        """ Transitions an issue, based on its name
        Args:
            issue (object): JIRA issue object
            name (string): transition name
        """
        transitions = {t['name']:t['id'] for t in self.conn.transitions(issue)}
        if name in transitions:
            self.conn.transition_issue(issue, transitions[name])
        else:
            logging.log(40, "Transition '{}' unavailable from current status".format(name))

    def add_comment(self, issue, comment):
        """Adds comment to JIRA"""
        self.conn.add_comment(issue, comment)


def add_label(issue, label):
    """Adds label to ticket"""
    issue.fields.labels.append(label)
    issue.update(fields={"labels": issue.fields.labels})


def remove_label(issue, label):
    """Removes label from ticket"""
    try:
        issue.fields.labels.remove(label)
        issue.update(fields={"labels": issue.fields.labels})
    except:
        logging.log(30, "Cannot remove label {} from ticket".format(label))


def get_summary(issue):
    """ Gets summary field """
    summary = str(issue.fields.summary)
    regexed = re.sub("[-#|,.!@&$%*()'_\"/\\\]", "", summary)
    campaign_name = '_'.join(regexed.split())
    return campaign_name


def get_campaign_name(issue):
    """ Gets campaign name, as defined by file naming conventions """
    summary = str(issue.fields.summary)
    try:
        m = re.search('(?<=for).*(?=NO)|(?<=for).*(?=\#)|(?<=for).*', summary)
        summary = m.group(0)
    except:
        pass
    s = re.sub("[-#|,.!@&$%*()'_\"/\\\]", "", summary)
    return '_'.join(s.split())


def get_file_info(issue):
    """ Gets file info fields """
    file_info = str(issue.fields.customfield_12147).replace(' ', '')
    if file_info is None:
        return None
    lines = file_info.split('\n')
    if len(lines) < 2:
        return None
    line = lines[1]
    fields = line.split('|')
    if not fields[0]:
        return None
    while len(fields) < 5:
        fields.append('null')
    return fields
    

def get_start_date(issue, fmt):
    """ Gets start date from ticket
        Common fmts: "%Y-%m-%d", "%Y%m%d"
    """
    start_date = issue.fields.customfield_10431
    try:
        return datetime.strptime(start_date, "%Y-%m-%d").strftime(fmt)
    except:
        msg = "Input-JIRA Error: Start Date: {}.".format(start_date)
        logging.log(40, msg)
        return None


def get_end_date(issue, fmt):
    """ Gets end date from ticket 
        Common fmts: "%Y-%m-%d", "%Y%m%d"
    """
    end_date = issue.fields.customfield_10418
    try:
        return datetime.strptime(end_date, "%Y-%m-%d").strftime(fmt)
    except:
        msg = "Input-JIRA Error: End Date: {}.".format(end_date)
        logging.log(40, msg)
        return None


def get_impression_source(issue):
    """ Gets impression source field """
    impression_source = str(issue.fields.customfield_12414).strip()
    expected = ["Managed Services", "Pixel"]
    if impression_source not in expected:
        msg = "Input-JIRA Error: Impression Source: {}. Expected: {}.".format(impression_source, expected)
        logging.log(40, msg)
        return None
    return impression_source


def get_ioid(issue):
    """ Gets IOID field """
    ioid = issue.fields.customfield_10447
    if ioid:
        return str(ioid).strip()
    return None

   
def get_receiver(issue):
    """ Gets receiver field """
    receiver = issue.fields.customfield_14612
    if receiver:
        return re.sub("[-#|,.!@&$%*()'_\"/\\\ ]", "", str(receiver))
    return None


def get_media_partner(issue):
    """ Gets media partner field """
    media_partner = issue.fields.customfield_13177
    if media_partner:
        return str(media_partner).strip()
    return None


def get_collection_method(issue):
    """ Gets collection method from ticket """
    collection_method = str(issue.fields.customfield_12414).strip()
    expected = ['Pixel', 'Other']
    if collection_method not in expected:
        msg = "Input-JIRA Error: Invalid Collection Method: {}. Expected: {}.".format(collection_method, expected)
        logging.log(40, msg)
        return None
    return collection_method


def get_output_type(issue):
    """ Gets output type field
    Args:
        issue: JIRA issue object
    Returns:
        Output type or None
    """
    output_type = str(issue.fields.customfield_15513).strip()
    expected = ["All", "Exposed", "Unexposed"]
    if output_type not in expected:
        msg = "Input-JIRA Error: Output Type: {}. Expected: {}.".format(output_type, expected)
        logging.log(40, msg)
        return None
    return output_type


def get_report_type(issue):
    """ Gets report type field
    Args:
        issue: JIRA issue object
    Returns:
        Report type, or None
    """
    report_type = str(issue.fields.customfield_15512).strip()
    expected = ["Household", "Individual"]
    if report_type not in expected:
        msg = "Input-JIRA Error: Report Type: {}. Expected: {}.".format(report_type, expected)
        logging.log(40, msg)
        return None
    return report_type


def get_scorecard_approver(issue):
    """ Gets the scorecard approver field
    Args:
        issue: JIRA issue object
    Returns:
        Scorecard approver (firstname.lastname)
    """
    approver = issue.fields.customfield_11248
    if approver:
        return str(issue.fields.customfield_11248).strip().lower()
    return None
