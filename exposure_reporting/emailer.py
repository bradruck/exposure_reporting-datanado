# emailer.py

import io
import os
import shutil
from email.message import EmailMessage


def create_email(subject, frm, to, cc, text):
    """Creates email object with to/from/cc parameters
    Args:
        subject: Email subject
        frm: Sender
        to: Receiver
        cc: cc recipient
        text: Email text
    """
    email = EmailMessage()
    email['Subject'] = subject
    email['From'] = frm
    email['To'] = to
    email['Cc'] = cc
    email.set_content(text)
    msg_txt = bytes(email).decode('utf-8')

    io_object = io.StringIO()
    io_object.write(msg_txt)
    return io_object


def write_email(email, outpath, filename):
    """Writes email text to file
    Args:
        email (str): Email email
        outpath (str): Where to write the file
        filename (str): What to call the file
    """
    file = os.path.join(outpath, filename)
    with open(file, 'w') as outfile:
        email.seek(0)
        shutil.copyfileobj(email, outfile)


def get_subject(campaign_name):
    """ Sets subject name for email
    Args:
        campaign_name: Base campaign name for all deliverables
    """
    return '{} exposure files are ready'.format(campaign_name)


def get_text(campaign_name, file_info, summaries):
    """Creates Exposure Report email text
    Args:
        campaign_name (str): Base name for all files in email
        file_info (list): Onramp file info
        summaries (list(OrderedDict)): List of counts for each report
    Returns:
        text (str): Full email text
    """
    opener = get_opener(campaign_name, file_info)
    file_summaries = [get_summary(summary) for summary in summaries]
    sendoff = get_sendoff()

    text = opener + '\n'.join(file_summaries) + sendoff
    return text


def get_opener(campaign_name, file_info):
    """Gets email opener text"""
    return """
Hello,

The {campaign_name} files are ready and located on the client site.  I have included the necessary client information below.

Format: CUST_ID|TIMESTAMP|ATTRIBUTE1|ATTRIBUTE2|ATTRIBUTE3|ATTRIBUTE4|CREATIVE|PLACEMENT

Total Columns: 8
CUST_ID = {custid}
Attribute1 = {a1}
Attribute2 = {a2}
Attribute3 = {a3}
Attribute4 = {a4}
Creative = Dynamic variable
Placement = Dynamic variable
Delimiter: Pipe
    """.format(campaign_name=campaign_name,
               custid=file_info[0],
               a1=file_info[1],
               a2=file_info[2],
               a3=file_info[3],
               a4=file_info[4])


def get_summary(summary):
    """Gets email text for the counts of one file"""
    return """
File Name: {filename}.txt.gz
Record Count: {record_count}
Total CUST_IDs in File: {custids}
Exposed Unique CUST_IDs: {unique_exposed_custids}
Impressions in File: {impressions}
    """.format(filename=summary['Campaign Name'],
               record_count=summary['Rows in Exposure File'],
               custids=summary['Customer IDs in File'],
               unique_exposed_custids=summary['Exposed Unique Customer IDs in File'],
               impressions=summary['Impressions in File'])


def get_sendoff():
    """Gets email text for sendoff"""
    return '\n\nSincerely,\nOracle Data Cloud\n'
