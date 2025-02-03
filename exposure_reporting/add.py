# add.py

import logging
import pandas as pd
import io
from exception import InputError, ParseError


def parse(attachment, sheetname=None):
    """Returns a Pandas DataFrame of the given JIRA attachment
    Args:
        attachment: JIRA attachment object (jira.resources.Attachment)
        sheetname (str): Specific sheetname to parse (optional)
    Returns:
        add: Pandas DataFrame
    """
    if attachment:
        excelfile = pd.ExcelFile(io.BytesIO(attachment))
        if sheetname:
            if sheetname in excelfile.sheet_names():
                add = excelfile.parse(sheetname)
            else:
                logging.log(40, "Invalid sheetname: {}. In file: {}".format(sheetname, excelfile.sheet_names))
                return None
        else:
            add = excelfile.parse()
        add.columns = list(range(0, len(add.loc[0])))
        return add
    return None


def get_rows(add):
    """Returns number of rows in file"""
    rows = len(add[0])
    return rows


def get_audience_file(add, row, input_folder, input_prefix, aws):
    """Gets audience file field"""
    try:
        audience_file = str(add[1][row]).strip()
    except Exception as e:
        raise ParseError("Unable to parse audience file: (B{})\n{}".format(row+1, e))

    prefix = input_prefix.format(audience_file=audience_file, input_folder=input_folder)

    if aws.is_empty(prefix):
        # Audience file isn't in S3
        msg = "Input-ADD Error: Invalid audience file: {}. Please revise ADD (Cell B{}).".format(audience_file, row+1)
        logging.log(40, msg)
        return msg

    return audience_file


def get_pixel_id(add, row):
    """Gets Pixel ID field"""
    try:
        pixel_id = str(add[2][row]).strip().replace('.0', '')
    except Exception as e:
        raise ParseError("Unable to parse Pixel ID: (C{})\n{}".format(row+1, e))

    if not pixel_id.isdigit():
        msg = "Input-ADD Error: Invalid Pixel/LineItem ID: {}. (Cell C{}). Expected only digits.".format(pixel_id, row+1)
        logging.log(40, msg)
        return msg

    return pixel_id


def get_profile_ids(add, row):
    """Gets Profile IDs field"""
    try:
        profile_ids = str(add[3][row]).replace(' ', '').replace('.0', '')
    except Exception as e:
        raise ParseError("Unable to parse Profile IDs: (D{})\n{}".format(row+1, e))
    
    if profile_ids in [None, 'nan']:
        return None
    
    plist = profile_ids.split(',')
    for profile in plist:
        if not profile.isdigit():
            msg = "Input-ADD Error: Invalid Profile IDs: {}. (Cell D{}). Expected only digits.".format(profile_ids, row+1)
            logging.log(40, msg)
            return msg

    return profile_ids


def get_targeted_flag(add, row):
    """Gets targeted flag field"""
    try:
        targeted = str(add[4][row]).strip().upper()
    except Exception as e:
        raise ParseError("Unable to parse targeted flag: (E{})\n{}".format(row+1, e))

    expected = ['Y', 'N']
    if targeted not in expected:
        msg = "Input-ADD Error: Invalid targeted flag: {}. (Cell E{}). Expected: {}.".format(targeted, row+1, expected)
        logging.log(40, msg)
        return msg

    return targeted
