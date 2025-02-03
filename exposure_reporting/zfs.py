#import logging
import os
import shutil
import gzip
from collections import OrderedDict


def stage_path(path):
    """ Creates path """
    if not os.path.exists(path):
        os.makedirs(path)


def get_count(file):
    """ Gets line count """
    if is_empty(file):
        return 0
    try:
        with open(file) as f:
            for i, line in enumerate(f):
                pass
        return i+1
    except Exception as e:
        #logging.log(40, "Unable to get line count for {}\n{}".format(file, e))
        print("Unable to get line count for {}\n{}".format(file, e))
        return -1


def sort(file, delimiter, column, path):
    """ Sorts file
    Args:
        file (string): File to sort
        delimiter (string): File delimiter
        column (int): Which column to sort
        path (string): Path of temporary sorting directory
    """
    command = "sort -t'{delimiter}' -k{column} {file} -o {file} --temporary-directory={path}".format(delimiter=delimiter,
                                                                                                     column=column,
                                                                                                     file=file,
                                                                                                     path=path)
    try:
        os.system(command)
    except Exception as e:
        #logging.log(40, "File sort failed.\n{}".format(e))
        print( "File sort failed.\n{}".format(e))


def zip(file):
    """ Zips a file """
    zipfile = "{file}.gz".format(file=file)
    try:
        with open(file, 'rb') as f_in:
            with gzip.open(zipfile, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return zipfile
    except:
        #logging.log(40, "Unable to zip file {}".format(filename))
        print("Unable to zip file {}".format(file))
        return None


def delete(file):
    """ Deletes a file """
    try:
        os.remove(file)
    except Exception as e:
        #logging.log(40, "Unable to delete file {}\n{}".format(filename, e))
        print("Unable to delete file {}\n{}".format(file, e))


def get_size(file):
    """ Gets filesize in bytes """
    return os.stat(file).st_size


def is_empty(file):
    """ Checks for empty file """
    size = get_size(file)
    return size == 0


def get_fields(file, headers, delimiter=',', skip_header=False):
    """ Gets list of OrderedDicts mapping headers to metrics from a file
    Args:
        file: file to read
        delimiter: how the file columns are split
        headers: what we should call each metric
    Returns:
        Single OrderedDict for a single-line file, otherwise list of OrderedDicts
    """
    all_fields = []
    try:
        with open(file, 'r') as readfile:
            if skip_header:
                next(readfile)
            for line in readfile:
                metrics = [metric.strip() for metric in line.split(delimiter)]
                fields = OrderedDict((headers[i], metrics[i]) for i, j in enumerate(headers))
                all_fields.append(fields)
    except Exception as e:
        #logging.log(40, "Unable to get fields: {}".format(file))
        print("Unable to get fields: {}".format(file))
        return []
    
    return all_fields
