
################################################################################
#
#    Filename: main.py
#    Authors: Sam Garfield
#    Date Last Updated: 04/03/2023
#
#    Description: Python executable that runs the Exposure Reporting automation
#
#    Usage: python main.py -- Runs all tickets in queue
#           python main.py [ISSUE] -- Runs one ticket
#
################################################################################

from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime, timedelta
import os
import logging
import sys
import exception
from cfg import CFG
from jira_util import Jira
from exposure_report import ExposureReport


def main(args):
    """Executes exposure reports
    Args:
        args: Command-line arguments
            --rerun (bool): Flag to overwrite queries or not
            --ticket (str): JIRA ticket key (e.g. CAM-123456)
    """
    configfile = ConfigParser()
    configfile.read('config.ini')

    config = CFG(configfile)
    set_logger(config)

    jira = Jira(config.get_field('jira', 'url'),
                config.get_field('jira', 'username'),
                os.environ['JIRA_USER']
                )
    jira.connect()

    # Case 1: Check manual reruns
    if args.ticket:
        try:
            issue = jira.conn.issue(args.ticket)
        except:
            print('Invalid ticket: {}'.format(args.ticket))
            return 1
        er = ExposureReport(config, issue)
        er.run(args.rerun)
        return 0

    # Case 2: Check actively-processing tickets
    today_minus_two = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    active_jql = config.get_field('jql', 'active').format(today_minus_two=today_minus_two)
    active_issues = jira.conn.search_issues(active_jql)
    if active_issues:
        logging.info("Active issues: {}\nExiting...".format([issue.key for issue in active_issues]))
        return 1

    # Case 3: Check tickets to process
    today_minus_two = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    process_jql = config.get_field('jql', 'jql').format(today_minus_two=today_minus_two)
    issues = jira.conn.search_issues(process_jql)
    logging.info("Issues: {}".format([issue.key for issue in issues]))
    for issue in issues:
        print(issue.key)
        er = ExposureReport(config, issue)
        er.run(args.rerun)

    logging.info("Exiting successfully")
    #return 0


def set_logger(config):
    """Sets logfile"""
    project_name = config.get_field('project', 'name')
    log_file_path = config.get_field('logfile', 'path')
    today_date = (datetime.now()-timedelta(hours=6)).strftime('%Y-%m-%d_%H%M%S')
    logfile_name = "{log_file_path}/{project_name}_{today_date}.log".format(log_file_path=log_file_path,
                                                                            project_name=project_name,
                                                                            today_date=today_date)
    logging.basicConfig(filename=logfile_name,
                        level=logging.INFO,
                        format='%(asctime)s: %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logging.log(20, "Starting execution of {} automation".format(project_name))


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--rerun', '-r', choices=[True, False], nargs='?', default=True, const=True, type=bool)
    parser.add_argument('--ticket', '-t', type=str)
    args = parser.parse_args()
    main(args)
    main(args)
