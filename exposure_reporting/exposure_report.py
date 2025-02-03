# exposure_report.py

from datetime import datetime, timedelta, date
import os
import logging

import headers
import zfs
import queries
import add
#import qcb
import emailer
#import qubole
import jira_util
from aws import AWS
from jira_util import Jira
#from qcb import QCBConnection
from datanado import DatanadoClient
from report import Report
from s3 import S3Tools


class ExposureReport(object):
    def __init__(self, config, issue):
        """ Sets the config and issue for an ExposureReport instance
        Args:
            config: CFG class instance
            issue: JIRA issue object
        """
        self.config = config
        self.issue = issue
        #self.s3_bucket = self.config.get_field('aws', 's3_bucket')
        self.logger = logging.log

    def run(self, rerun=False):
        """ Run an Exposure Report ticket
        Args:
            rerun (bool): Flag to overwrite queries or not
        """
        # Set necessary connections
        self.logger(20, "Running ticket: {}".format(self.issue.key))
        jira_util.add_label(self.issue, 'OM.Processing')
        
        jira = Jira(self.config.get_field('jira', 'url'),
                    self.config.get_field('jira', 'username'),
                    os.environ['JIRA_USER'])
        jira.connect()

        # Vault Client Object - no longer being used, replaced by k8s secrets
        '''VC_Obj = VaultClient("prod")
        aws_key = VC_Obj.VaultSecret('aws', 'prod_user_key_id')
        aws_secret_key = VC_Obj.VaultSecret('aws', 'prod_user_secret_key')'''

        aws_key = os.environ['AWS_ACCESS_KEY']
        aws_secret_key = os.environ['AWS_SECRET']

        aws_conn = AWS(aws_key,
                       aws_secret_key,
                       self.config.get_field('aws', 'bucket'))
        aws_conn.check_keys()

        #qubole.configure(self.config.get_field('qubole', 'token'))
        # This is replaced below with a Datanado client in execute_queries() method
        
        # Collect and validate inputs
        jira_args = self.get_jira_args()
        if not self.validate(jira_args, transition=jira.transition, comment=jira.add_comment, msg='JIRA-Input Error: Missing required field'):
            return

        config_args = self.get_config_args(jira_args)
        if not self.validate(config_args, transition=None, comment=None, msg='Config Error: Missing required field'):
            return
        
        attachment = jira.get_attachment(self.issue, keyword='ADD', extension='.xlsx')
        add_args = self.get_add_args(attachment, jira_args, config_args, aws_conn)
        if not self.validate_add(add_args, transition=jira.transition, comment=jira.add_comment):
            return

        reports = self.get_reports(jira_args, config_args, add_args)
        for report in reports:
            report.validate()

        # create a sql file name and Datanado payload
        hive_query_file = "{}_{}.sql".format(str(date.today()), self.issue)
        datanado_payload_object = {
            "job-internal-name": "PA_EXPOSURE_REPORTING",
            "parameters": {
                "hive-arg-1-script-location": "s3://dlx-prod-analytics/analytics-platform/gold/query/exposure_reporting/{}".format(hive_query_file),
                "hive-arg-1-command-name": "Exposure_Report_{}".format(self.issue)
                }
        }
        print("s3://dlx-prod-analytics/analytics-platform/gold/query/exposure_reporting/{}".format(hive_query_file))
        # Run report queries, or skip if specified (skipping no longer used)
        if rerun is False:
            self.logger(20, "Skipping queries -- all S3 directories populated")
        else:
            self.logger(20, "Reports to run: {}".format(len(reports)))
            queries_succeeded = self.execute_queries(reports, datanado_payload_object, hive_query_file)

        if not queries_succeeded:
            jira.transition(self.issue, "Processing Failure")
            jira_util.remove_label(self.issue, 'OM.Processing')
            return
        
        self.logger(20, "Queries succeeded")
        
        # Start downloading files
        #zfs_path = "{}{}".format(config_args['zfs_volume'], config_args['zfs_path'])
        zfs_path = config_args['zfs_path']
        zfs.stage_path(zfs_path)

        # Create files per report
        summaries, duplicates = [], []
        summary_srcs, duplicate_srcs = [], []
        files = [('EXPOSURE', 'txt'), ('SUMMARY', 'csv'), ('WEEKLY', 'csv'), ('DUPLICATES', 'csv')]
        for report in reports:
            # Download files -- onramp, weekly
            src = ['{prefix}/{report_name}/{filename}'.format(prefix=config_args['output_prefix'], report_name=report.campaign_name, filename=file[0]) for file in files]
            dst = ['{path}/{report_name}_{filename}.{extension}'.format(path=config_args['zfs_path'], report_name=report.campaign_name, filename=file[0], extension=file[1]) for file in files]
            exposure_file = '{path}/{report_name}.txt'.format(path=config_args['zfs_path'], report_name=report.campaign_name)
            
            self.logger(20, "Downloading files for report: {}, files: {}".format(report.campaign_name, src))
            
            aws_conn.download(src[0], exposure_file)
            self.logger(20, "Exposure File successfully transferred to ZFS directory")
            aws_conn.download_csv(src[2], dst[2], delimiter=',', headers=headers.get_weekly_headers())

            # Sort onramp by timestamp, then by cust id
            zfs.sort(exposure_file, '|', 2, config_args['zfs_path'])
            zfs.sort(exposure_file, '|', 1, config_args['zfs_path'])

            # Gzip onramp
            zipfile = zfs.zip(exposure_file)

            # Collect summary and duplicates prefixes
            summary_srcs.append(src[1])
            duplicate_srcs.append(src[3])

        # Create overall summary file
        self.logger(20, "Downloading summary file")
        summary_dst = '{0}/{1}_SUMMARY.csv'.format(config_args['zfs_path'], jira_args['Campaign Name'])
        aws_conn.download_csv(summary_srcs, summary_dst, delimiter=',', headers=headers.get_summary_headers())

        # Create overall duplicates file
        self.logger(20, "Downloading duplicates file")
        duplicate_dst = '{0}/{1}_DUPLICATES.csv'.format(config_args['zfs_path'], jira_args['Campaign Name'])
        aws_conn.download_csv(duplicate_srcs, duplicate_dst, delimiter=',', headers=headers.get_duplicate_headers())

        # Get fields from summary and duplicates file
        summaries = zfs.get_fields(summary_dst, headers.get_summary_headers(), skip_header=True)
        duplicates = zfs.get_fields(duplicate_dst, headers.get_duplicate_headers(), skip_header=True)

        # Create summary and duplicate comments
        self.logger(20, "Sending summary & duplicate comments to JIRA")
        summary_comment = self.get_summary_comment(summaries, headers.get_summary_headers())
        duplicate_comment = self.get_summary_comment(duplicates, headers.get_duplicate_headers())

        # Post comments to JIRA
        jira.add_comment(self.issue, summary_comment)
        jira.add_comment(self.issue, duplicate_comment)

        # Create and post QC Brains payload - No longer supported
        """self.logger(20, "Invoking QC Brains")
        qcb_connection = QCBConnection(config_args['qcb_url'],
                                       config_args['qcb_project'],
                                       config_args['qcb_listener'])
        payload = qcb.get_payload(self.issue, summaries)
        qcb_connection.post(payload)"""

        # Create email file
        self.logger(20, "Creating email")
        subject = emailer.get_subject(jira_args['Campaign Name'])
        frm = config_args['email_from']
        to = '{approver}@oracle.com'.format(approver='.'.join(jira_args['Scorecard Approver'].split()))
        cc = config_args['email_cc']
        text = emailer.get_text(jira_args['Campaign Name'],
                                jira_args['Incoming File Information'],
                                summaries)
        email_file = emailer.create_email(subject, frm, to, cc, text)
        emailer.write_email(email_file, config_args['zfs_path'], config_args['email_filename'])

        # Transition ticket to QC
        jira.transition(self.issue, 'Submit for Approval')
        jira_util.remove_label(self.issue, 'OM.Processing')

    def get_jira_args(self):
        """ Gets all necessary JIRA variables """
        jira_args = {
            'Campaign Name': jira_util.get_campaign_name(self.issue),
            'Impression Source': jira_util.get_impression_source(self.issue),
            'Output Type': jira_util.get_output_type(self.issue),
            'Report Type': jira_util.get_report_type(self.issue),
            'Start Date': jira_util.get_start_date(self.issue, "%Y%m%d"),
            'End Date': jira_util.get_end_date(self.issue, "%Y%m%d"),
            'Start Dash': jira_util.get_start_date(self.issue, "%Y-%m-%d"),
            'End Dash': jira_util.get_end_date(self.issue, "%Y-%m-%d"),
            'Incoming File Information': jira_util.get_file_info(self.issue),
            'Scorecard Approver': jira_util.get_scorecard_approver(self.issue)
        }
        jira_args['input_folder'] = 'AttributeFileInput' if jira_args['Report Type'] == 'Household' else 'IndividualAttributeFileInput'
        self.logger(20, jira_args)
        return jira_args

    def get_config_args(self, jira_args):
        """Gets variables from the config file"""
        config_args = {
            'bucket': self.config.get_field('aws', 'bucket'),
            'input_prefix': self.config.get_field('aws', 'input_prefix'),
            'output_prefix': self.config.get_field('aws', 'output_prefix').format(report_type=jira_args['Report Type']),
            'zfs_path': self.config.get_field('zfs', 'path').format(issuekey=self.issue.key),
            'zfs_volume': self.config.get_field('zfs', 'volume'),
            #'cluster': self.config.get_field('qubole', 'cluster'),
            #'qcb_url': self.config.get_field('qcb', 'url'),
            #'qcb_project': self.config.get_field('qcb', 'project'),
            #'qcb_listener': self.config.get_field('qcb', 'listener'),
            'email_from': self.config.get_field('email', 'from'),
            'email_cc': self.config.get_field('email', 'cc'),
            'email_filename': self.config.get_field('email', 'filename')
        }
        self.logger(20, config_args)
        return config_args

    def get_add_args(self, attachment, jira_args, config_args, aws_conn):
        """Gets variables from the ADD attachment file"""
        add_object = add.parse(attachment)
        rows = add.get_rows(add_object)
        add_args = {
            'rows': rows,
            'audience_file': [add.get_audience_file(add_object, i, jira_args['input_folder'], config_args['input_prefix'], aws_conn) for i in range(rows)],
            'pixel_id': [add.get_pixel_id(add_object, row=i) for i in range(rows)],
            'profile_ids': [add.get_profile_ids(add_object, row=i) for i in range(rows)],
            'targeted': [add.get_targeted_flag(add_object, row=i) for i in range(rows)]
        }
        self.logger(20, add_args)
        return add_args

    def get_reports(self, jira_args, config_args, add_args):
        """Creates reports"""
        reports = []
        for i in range(add_args['rows']):
            start_date = jira_args['Start Date']
            end_date = jira_args['End Date']
            start_dash = jira_args['Start Dash']
            end_dash = jira_args['End Dash']
            impression_source = jira_args['Impression Source']
            output_type = jira_args['Output Type']
            report_type = jira_args['Report Type']
            audience_file = add_args['audience_file'][i]
            pixel_id = add_args['pixel_id'][i]
            profile_ids = add_args['profile_ids'][i]
            targeted = add_args['targeted'][i]
            report_number = i+1
            bucket = config_args['bucket']
            output_prefix = config_args['output_prefix']
            campaign_name = '{}_{}_{}_Exposure'.format(jira_args['Campaign Name'], pixel_id, i+1)

            report = Report(campaign_name, start_date, end_date, start_dash, end_dash,
                            impression_source, output_type, report_type, audience_file,
                            pixel_id, profile_ids, targeted, report_number, bucket, output_prefix)
            reports.append(report)
        return reports

    def can_skip_queries(self, reports, aws_conn):
        """Determines if queries have already been run successfully for this ticket"""
        s3_locations = []
        for report in reports:
            s3_locations.extend(['{}/{}/EXPOSURE'.format(report.output_prefix, report.campaign_name),
                                 '{}/{}/SUMMARY'.format(report.output_prefix, report.campaign_name),
                                 '{}/{}/WEEKLY'.format(report.output_prefix, report.campaign_name),
                                 '{}/{}/DUPLICATES'.format(report.output_prefix, report.campaign_name)])
        is_empty = [aws_conn.is_empty(s3_location) for s3_location in s3_locations]
        if True in is_empty:
            return False
        else:
            return True

    def execute_queries(self, reports, payload_object, query_file_name):
        """ Runs queries from reports via Hive execution
        Args:
            reports: list
            query_file_name: str
            payload_object: dict
        Returns:
            True or False
        """
        parallel_queries = []
        for report in reports:
            query = queries.get_queries(report.campaign_name,
                                        report.start_date,
                                        report.end_date,
                                        report.start_dash,
                                        report.end_dash,
                                        report.impression_source,
                                        report.output_type,
                                        report.report_type,
                                        report.audience_file,
                                        report.pixel_id,
                                        report.profile_ids,
                                        report.targeted,
                                        report.report_number,
                                        report.bucket,
                                        report.output_prefix)
            parallel_queries.append(query)

            # Method call to create sql file and upload to S3 location for Datanado job
            self.upload_query_file(query_file_name, query)

        #job_ids = qubole.run_queries_parallel(parallel_queries, label, 'ER {}'.format(self.issue.key))
        #failed = [job_id for job_id in job_ids if not qubole.is_success(job_id)]

        # Create Datanado client
        datanado_client = DatanadoClient(payload_object=payload_object)
        # Launch Datanado API job
        job_instance_id = datanado_client.execute_api_request()
        print(job_instance_id)

        # Run DN Method to keep track of job status
        if datanado_client.watch_datanado_job(job_instance_id):
            self.logger(20, "Moving on to Post-Processing")
            return True
        else:
            self.logger(20, "Qubole query failed")
            return False

        #if failed:
            #self.logger(30, "Parallel queries failed. Qubole Job IDs: {}".format(failed))
            #return False
        #return True

    def upload_query_file(self, s3_file_name, s3_query):
        # Create S3 client
        s3_client = S3Tools(self.config)
        # Upload local file to S3 location
        s3_client.upload_sql_file(s3_file_name, s3_query)

    def get_summary_comment(self, summaries, headers):
        """ Constructs summary comment to post to JIRA
        Args:
            summaries: List of lists of values
            headers: List of header names
        Returns:
            comment
        """
        comment_list = ['||{}||'.format('||'.join(headers))]
        for summary in summaries:
            values = list(summary.values())
            comment_list.append('|{}|'.format('|'.join(values)))
        return '\n'.join(comment_list)

    def purge_files(self, purge_dir, purge_days, min_size):
        """ Removes files older than a given age and under a given size
        Args:
            purge_dir: Directory to purge files
            purge_days: Time constraint after which a file must be deleted
            min_size: Size constraint under which a file must be deleted
        """
        now = datetime.now()
        for file in os.listdir(purge_dir):
            file_path = os.path.join(purge_dir, file)
            file_date = datetime.fromtimestamp(os.path.getmtime(file_path))
            file_size = os.path.getsize(file_path)
            if file_date < now - timedelta(days=purge_days):
                self.logger(20, "Purging file [{file_path}] with timestamp [{timestamp}]".format(file_path=file_path, timestamp=file_date))
                os.remove(file_path)
            elif file_size < min_size:
                self.logger(20, "Purging file [{file_path}] with filesize [{filesize}]".format(file_path=file_path, filesize=file_size))
                os.remove(file_path)

    def validate(self, args, transition=None, comment=None, msg=None):
        """Validates inputs by running through key, value pairs"""
        errors = []
        for name, value in args.items():
            if value is None:
                errors.append('{}: {}'.format(msg, name))
        if errors:
            message = '\n'.join(errors)
            self.logger(40, message)
            if comment:
                comment(self.issue, message)
            if transition:
                transition(self.issue, "Processing Failure")
            # if exception:
            #     raise exception(message)
            jira_util.remove_label(self.issue, 'OM.Processing')
            return False
        return True

    def validate_add(self, args, transition, comment, msg=None):
        """Checks errors and comments in JIRA"""
        errors = []
        for name, values in args.items():
            if name == 'rows':
                continue
            for value in values:
                if value is not None and value.startswith('Input-ADD Error'):
                    errors.append(value)
        if errors:
            message = '\n'.join(errors)
            comment(self.issue, message)
            transition(self.issue, "Processing Failure")
            jira_util.remove_label(self.issue, 'OM.Processing')
            return False
            # raise exception(message)
        return True
