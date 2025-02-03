"""This module creates a DatanadoClient Python object. The DatanadoClient object allows for request authentication
using 3AMP API Security (https://confluence.oracledatacloud.com/display/SAuth/3AMP+-+Design+-+API+Security). It
also manages the POST request to the Datanado API
(https://confluence.oracledatacloud.com/pages/viewpage.action?spaceKey=DKB&title=Execute+or+Run+Datanado+Job).

Exported Classes
DatanadoClient
"""

import datetime
import hashlib
import hmac
import requests
import email.utils
import base64
import json
import os
import time
import logging


class DatanadoClient:
    """
    A class used to create a DatanadoClient Python object

    Parameters
    ----------
    payload_object: dict
        Datanado job parameters to be sent with API request

    Attributes
    ----------
    client_id: str
        Job service client ID for API authentication (accessed as an environment variable using Vault)
    secret: str
        Secret associated with the client ID for API authentication (accessed as an environment variable using Vault)
    path: str
        API endpoint target path
    host: str
        API host address
    method: str
        HTTP request method to be used
    current_datetime: datetime
        Current datetime to be used for authentication purposes - created at object instantiation
    current_datestring: str
        String formatted current_datetime
    content_type: str
        Header for use to indicate the media type of the resource being sent through HTTP request
    payload_object: dict
        Parameters to be sent with API request

    Methods
    -------
    execute_api_request()
        Orchestration method to execute the HTTP POST request to Datanado API
    watch_datanado_job()
        Returns 'True' for successful job completion, else returns 'False'
    _get_json_payload()
        Returns jsonified payload
    _get_x_content_sha256(json_payload)
        Returns encoded payload and content length
    _encode_payload(json_payload)
        Returns utf-8 encoded payload
    _create_256_digest(encoded_payload)
        Returns hashed and digested payload
     _encode_decode_base64(digested_payload)
        Returns string representation of base64 conversion of payload
    _get_endpoint()
        Returns URL endpoint for API
    _get_encoded_header_string(content_length, x_content_sha256)
        Returns utf-i encoded string for use in header for authorization signature
    _get_request_headers(signature, x_content_sha256, content_length)
        Returns the request headers to be used when making HTTP post request to API
    _get_auth_header(self, signature):
        Returns the formatted 3AMP Authorization header for use in API request
    """

    def __init__(self, payload_object):
        self.client_id = os.environ['DATANADO_JOB_SERVICE_CLIENT_ID']
        self.secret = os.environ['DATANADO_JOB_SERVICE_CLIENT_SECRET'].encode('utf-8')

        self.path = '/api/v1/jobs/launch'
        self.host = 'datanado-job-service.valkyrie.net'
        self.method = 'POST'

        self.current_datetime = datetime.datetime.now()
        self.current_datestring = email.utils.format_datetime(self.current_datetime)

        self.content_type = 'application/json'
        self.payload_object = payload_object

    def execute_api_request(self):
        """Orchestration method to execute the HTTP POST request to Datanado API"""
        url = self._get_endpoint()
        json_payload = self._get_json_payload()
        x_content_sha256, content_length = self._get_x_content_sha256(json_payload=json_payload)
        encoded_header_string = self._get_encoded_header_string(content_length=content_length, x_content_sha256=x_content_sha256)
        signature = self._get_signature(encoded_header_string=encoded_header_string)
        headers = self._get_request_headers(signature=signature, x_content_sha256=x_content_sha256, content_length=content_length)

        try:
            response = requests.post(url=url, headers=headers, data=json_payload)
        except Exception as e:
            logging.log(40, e)
        else:
            logging.log(20, response.text)
            return json.loads(response.text).get("job-instance").get("id")

    def watch_datanado_job(self, job_instance_id):
        """Returns 'True' for successful job completion, else returns 'False'"""
        job_status = "IN_PROGRESS"
        while job_status == "IN_PROGRESS":
            time.sleep(60)
            response = requests.get(
                'http://datanado-job-status-service-prod.prd-use1-eks-b.k8s.oracledatacloud.com/api/v1/orchestrationStatus/{}'
                .format(job_instance_id),
            )
            logging.log(20, json.loads(response.text).get("job-status"))
            job_status = json.loads(response.text).get("job-status")

        if job_status == "SUCCESS":
            logging.log(20, "Datanado job {} completed".format(job_instance_id))
            return True
        else:
            logging.log(40, "Datanado job {} failed.".format(job_instance_id))
            return False

    def _get_json_payload(self):
        """Returns jsonified payload"""
        json_payload = json.dumps(self.payload_object)

        return json_payload

    def _get_x_content_sha256(self, json_payload):
        """Returns encoded payload and content length"""
        encoded_payload, content_length = self._encode_payload(json_payload=json_payload)

        digested_payload = self._create_256_digest(encoded_payload=encoded_payload)
        x_content_sha256 = self._encode_decode_base64(digested_payload=digested_payload)

        return x_content_sha256, content_length

    def _encode_payload(self, json_payload):
        """Returns utf-8 encoded payload"""
        encoded_payload = json_payload.encode('utf-8')
        content_length = str(len(encoded_payload))

        return encoded_payload, content_length

    def _create_256_digest(self, encoded_payload):
        """Returns hashed and digested payload"""
        m = hashlib.sha256()
        m.update(encoded_payload)
        digested_payload = m.digest()

        return digested_payload

    def _encode_decode_base64(self, digested_payload):
        """Returns string representation of base64 conversion of payload"""
        decoded_payload = base64.b64encode(digested_payload).decode('utf-8')

        return decoded_payload

    def _get_endpoint(self):
        """Returns URL endpoint for API"""
        url_endpoint = 'http://{0}{1}'.format(self.host, self.path)

        return url_endpoint

    def _get_encoded_header_string(self, content_length, x_content_sha256):
        """Returns utf-i encoded string for use in header for authorization signature"""
        header_string = (self.method + self.path + self.host + self.current_datestring + x_content_sha256 + self.content_type + content_length).lower()
        encoded_header_string = header_string.encode('utf-8')

        return encoded_header_string

    def _get_signature(self, encoded_header_string):
        """Returns string representation of base64 conversion of header_string"""
        m = hmac.new(self.secret, digestmod=hashlib.sha256)
        m.update(encoded_header_string)
        signature = base64.b64encode(m.digest()).decode('utf-8')

        return signature

    def _get_request_headers(self, signature, x_content_sha256, content_length):
        """Returns the request headers to be used when making HTTP post request to API"""
        headers = {
            'Date': self.current_datestring,
            'Authorization': self._get_auth_header(signature=signature),
            'x-content-sha256': x_content_sha256,
            'content-length': content_length,
            'content-type': self.content_type
        }

        return headers

    def _get_auth_header(self, signature):
        """Returns the formatted 3AMP Authorization header for use in API request"""
        auth_header = '3AMP; version="1",keyId="{0}",algorithm="hmac-sha256",headers="(resource-target) host date ' \
                      'x-content-sha256 content-type content-length", signature="{1}"'.format(self.client_id, signature)

        return auth_header
