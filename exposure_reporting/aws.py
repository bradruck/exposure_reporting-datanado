import io
import logging
import boto3
import pandas as pd
from exception import ConfigError, FileError


class AWS(object):
    def __init__(self, key, secret_key, bucket):
        self._key = key
        self._secret_key = secret_key
        self._bucket = bucket

    def check_keys(self):
        """Checks validity of class """
        try:
            session = boto3.Session(self._key, self._secret_key)
        except Exception as e:
            raise ConfigError("Config Error: Check AWS credentials")

    def download(self, src, dst):
        """ Downloads a file from S3
        Args:
            self: AWS object
            src: S3 prefix
            dst: zfs_path + file
        """
        session = boto3.Session(self._key, self._secret_key)
        bucket = session.resource("s3").Bucket(self._bucket)
        try:
            with open(dst, 'wb') as outfile:
                for key in bucket.objects.filter(Prefix=src):
                    outfile.write(key.get()["Body"].read())
        except Exception as e:
            raise FileError("File download from {} to {} failed.\n{}".format(src, dst, e))

    def download_csv(self, src, dst, delimiter=',', headers=None):
        """ Downloads a CSV file from S3
        Args:
            src (str or list(str)): Single S3 prefix or list of S3 prefixes
            dst (str): zfs_path + filename
            delimiter (str): File delimiter
            headers (list(str)): List of file headers (optional)
        """
        if type(src) == str:
            keys = self.get_keys(src)
        elif type(src) == list:
            keys = []
            for s in src:
                k = self.get_keys(s)
                keys.extend(k)

        files = []
        for key in keys:
            file = self.get_file_from_key(key, delimiter)
            files.append(file)

        df = pd.DataFrame()
        if files:
            df = pd.concat(files)
            if headers:
                df.columns = headers
            try:
                df.to_csv(dst, sep=delimiter, index=False)
            except Exception as e:
                raise FileError("File download failed: {}\n{}".format(dst, e))
        else:
            raise FileError("Files to download are empty: {}".format(src))

    def upload(self, src, dst):
        """ Uploads file from filesystem to S3
        Args:
            src: zfs_path + filename
            dst: S3 prefix + filename
        """
        s3 = boto3.client("s3", aws_access_key_id=self._key, aws_secret_access_key=self._secret_key)
        fin = open(src, "rb")
        mpu = s3.create_multipart_upload(Bucket=self._bucket, Key=dst)
        chunk_size = 10485760
        parts = []
        part_number = 0
        chunk_read, chunk_write = "", ""

        while True:
            chunk_read = fin.read(chunk_size)
            if chunk_write:
                part_number += 1
                chunk_to_load = chunk_write
                part = s3.upload_part(Body=chunk_to_load, Bucket=self._bucket,
                                      Key=dst, PartNumber=part_number,
                                      UploadId=mpu["UploadId"])
                parts.append({"ETag": part["ETag"], "PartNumber": part_number})
            if len(chunk_read) == 0:
                fin.close()
                break
            chunk_write = chunk_read
        resp = s3.complete_multipart_upload(Bucket=self._bucket, Key=dst,
                                            MultipartUpload={"Parts": parts},
                                            UploadId=mpu["UploadId"])

    def get_keys(self, prefix):
        """ Gets keys in a specified prefix
        Args:
            prefix: S3 prefix
        """
        session = boto3.Session(
            aws_access_key_id=self._key,
            aws_secret_access_key=self._secret_key,
        )
        s3 = session.resource('s3')
        #s3 = boto3.resource('s3')
        bucket = s3.Bucket(self._bucket)
        keys = list(bucket.objects.filter(Prefix=prefix))
        return keys

    def is_empty(self, prefix):
        """ Determines if a directory is empty or not
        Args:
            prefix: S3 prefix
        """
        client = boto3.client("s3", aws_access_key_id=self._key, aws_secret_access_key=self._secret_key)
        response = client.list_objects_v2(
            Bucket=self._bucket,
            Prefix=prefix
        )
        obj_list = response.get('Contents', [])
        if not obj_list:
            return True
        else:
            return False

    def get_file_from_key(self, key, delimiter):
        """ Gets a file object from an S3 key object
        Args:
            key: S3 key object
            delimiter: expected file delimiter
        """
        if key.size != 0:
            contents = key.get()['Body'].read().decode('utf-8')
            file_obj = io.StringIO(contents)
            file = pd.read_csv(file_obj, sep=delimiter, header=None)
            return file
