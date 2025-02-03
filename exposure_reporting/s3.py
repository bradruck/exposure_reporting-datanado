"""This module creates an AWS S3 Tools Class object to provide various S3
functionality for the application

Exported Classes
S3Tools
"""

import boto3
import os
import logging


class S3Tools:
    """
    A class used to create an S3Tools object

    Parameters
    ----------
   config: dict

    Attributes
    ----------
    s3_bucket: s3 bucket name
    s3_prefix: s3 file location prefix
    s3_session: instance
        Instance of AWS S3 Session object
    s3_client: instance
        Instance of AWS S3 Client object
    local_prefix: local file location prefix

    Methods
    -------
    upload_sql_file()
        Uploads file to s3 location
    _create_folder(path)
        Creates a specified folder if it doesn't exist
    _create_s3_session()
        Returns AWS S3 Session object
    _upload_file(s3_bucket, s3_prefix, local_directory, file_name
        Uploads local file to S3 location
    _delete_file(s3_bucket, s3_prefix, file_name)
        Deletes file in S3 location
    """

    def __init__(self, config):
        self.config = config
        self.s3_bucket = self.config.get_field('s3', 's3_bucket')
        self.s3_prefix = self.config.get_field('s3', 's3_output_prefix')
        self.s3_access_key = os.environ['S3_ACCESS_KEY']
        self.s3_secret_key = os.environ['S3_SECRET']
        self.s3_session = self._create_s3_session()
        self.s3_client = self.s3_session.resource('s3')
        self.local_prefix = self.config.get_field('project', 'data_directory')

    def upload_sql_file(self, file_name, file_string):
        """Creates and uploads local sql file to S3 location"""
        #self._create_folder()
        with open("{}/{}".format(self.local_prefix, file_name), 'w') as f:
            f.write(file_string)

        self._upload_file(self.s3_bucket, '{}/{}'.format(self.local_prefix, file_name), '{}/{}'.format(self.s3_prefix, file_name))

    def _upload_file(self, bucket, source_object, target_object):
        """Uploads local file to S3 location"""
        try:
            self.s3_client.Bucket(bucket).upload_file(source_object, target_object)
        except Exception as e:
            logging.log(40, '_upload_file error')

    @staticmethod
    def _create_folder(path):
        """Creates a specified folder if it doesn't exist"""
        does_exist = os.path.exists(path)

        if not does_exist:
            os.makedirs(path)

    def _create_s3_session(self):
        """Returns AWS S3 Session object"""
        s3_session = boto3.Session(
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
        )

        logging.log(20, 's3_session object ({0}) created'.format(s3_session))

        return s3_session

    def _delete_file(self, s3_bucket, s3_prefix, file_name):
        """Deletes file in S3 location"""
        target_object = '{0}/{1}'.format(s3_prefix, file_name)

        try:
            self.s3_client.delete_object(Bucket=s3_bucket, Key=target_object)
        except Exception as e:
            logging.log(40, '_delete_file', e)
