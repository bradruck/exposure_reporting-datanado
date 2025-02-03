from qds_sdk.commands import HiveCommand
from qds_sdk.qubole import Qubole
import logging
import time
from .exception import ConfigError, QuboleError


def configure(api_token):
    """Configures Qubole connection"""
    try:
        Qubole.configure(api_token=api_token, poll_interval=300)
    except Exception as e:
        raise ConfigError("Qubole configuration failed\n{}".format(e))


def find_command(job_id):
    """ Finds command in Qubole
    Args:
        job_id (int): Qubole job ID
    Returns:
        HiveCommand object
    """
    hcmd = HiveCommand.find(job_id)
    return hcmd


def is_success(job_id):
    """ Gets status of command in Qubole
    Args:
        job_id (int): Qubole job ID
    Returns:
        HiveCommand status
    """
    hcmd = HiveCommand.find(job_id)
    status = hcmd.status
    return HiveCommand.is_success(status)


def run_queries_parallel(queries, label, name):
    """ Runs Qubole queries in parallel
    Args:
        queries (list(string)): List of query strings
        label (string): Hive cluster
        name (string): Query name
    Returns:
        job_ids (list(int)): List of all Qubole Job IDs launched
    """
    qidlist, job_ids = [], []
    errors = {}
    for i, query in enumerate(queries):
        hcmd = HiveCommand.create(query=query, label=label, name='{0} {1}/{2}'.format(name, i+1, len(queries)))
        qidlist.append([i, hcmd.id])
        job_ids.append(hcmd.id)
    while qidlist:
        time.sleep(Qubole.poll_interval)
        for entry in qidlist:
            i = entry[0]
            job_id = entry[1]
            hcmd = HiveCommand.find(job_id)
            if HiveCommand.is_success(hcmd.status):
                qidlist.remove([i, job_id])
            elif HiveCommand.is_done(hcmd.status):
                errors[i] = [job_id, hcmd.status]
                qidlist.remove([i, job_id])
    if errors:
        logging.log(40, "Parallel queries errors: {}".format(errors))
    return job_ids


def run_query(query, label, name, retries=0):
    """ Runs query in Qubole
    Args:
        query (string): Query
        label (string): Hive cluster
        name (string): Query name
        retries (int): Number of retries (optional)
    Returns:
        Job ID (int)
    """
    is_success = False
    attempt = 1
    max_attempts = 1 + retries
    while not is_success and attempt <= max_attempts:
        hcmd = HiveCommand.create(query=query, label=label, name=name)
        status = watch_status(hcmd.id)
        is_success = HiveCommand.is_success(status)
        attempt += 1
    if is_success is False:
        logging.log(30, "Qubole job failed {} time(s)".format(max_attempts))
    return hcmd.id


def watch_status(job_id):
    """ Monitors Qubole query status as it runs
    Args:
        job_id (int): Qubole job ID
    Returns:
        status (string): Success or failure status
    """
    cmd = HiveCommand.find(job_id)
    while not HiveCommand.is_done(cmd.status):
        time.sleep(Qubole.poll_interval)
        cmd = HiveCommand.find(job_id)
    status = cmd.status
    return status
