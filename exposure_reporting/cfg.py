# cfg.py

import logging


class CFG(object):
    def __init__(self, configfile):
        self.configfile = configfile

    def get_field(self, section, option, return_type=str):
        """ Gets a field in the config file
        Args:
            config: ConfigParser object
            section: Section in config file
            option: Option in section in config file
            return_type: Desired return type
        """
        if self.configfile.has_section(section):
            if self.configfile.has_option(section, option):
                try:
                    return return_type(self.configfile.get(section, option))
                except:
                    logging.log(40, "Config section {}, option {} cannot be cast as {}".format(section, option, return_type))
                    return None
            else:
                logging.log(40, "Config section {} has no option {}".format(section, option))
                return None
        else:
            logging.log(40, "Config has no section {}".format(section))
            return None
