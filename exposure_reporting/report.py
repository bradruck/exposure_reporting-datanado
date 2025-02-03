import re
import logging
from exception import InputError

class Report(object):
    def __init__(self, campaign_name, start_date, end_date, start_dash, end_dash,
                 impression_source, output_type, report_type, audience_file,
                 pixel_id, profile_ids, targeted, report_number, bucket, output_prefix):
        self.campaign_name = campaign_name
        self.start_date = start_date
        self.end_date = end_date
        self.start_dash = start_dash
        self.end_dash = end_dash
        self.impression_source = impression_source
        self.output_type = output_type
        self.report_type = report_type
        self.audience_file = audience_file
        self.pixel_id = pixel_id
        self.profile_ids = profile_ids
        self.targeted = targeted
        self.report_number = report_number
        self.bucket = bucket
        self.output_prefix = output_prefix


    def validate(self):
        """ Raises exceptions occurring report to report
        Conditions:
            1. If Report Type = Individual, Targeted cannot be Y
            2. If Targeted = Y, Profile IDs are required
            3. If Targeted = N, Profile IDs must be blank
        """
        if self.report_type == 'Individual' and self.targeted == 'Y':
            raise InputError("Input-ADD Error: Individual level report cannot be targeted. (Cell E{row}).".format(row=self.report_number))
        elif self.targeted == 'Y' and self.profile_ids is None:
            raise InputError("Input-ADD Error: Targeted campaign must include Profile IDs. (Cells D{row}, E{row}).".format(row=self.report_number))
        elif self.targeted == 'N' and self.profile_ids:
            raise InputError("Input-ADD Error: Untargeted campaign must not include Profile IDs. (Cells D{row}, E{row}).".format(row=self.report_number))