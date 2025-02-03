import logging


class Error(Exception):
    """A general error derived from Exception"""
    def __init__(self, msg):
        Exception.__init__(self, msg)
        logging.log(40, msg)


class InputError(Error):
    """Error that occurs on JIRA or ADD input"""
    pass


class ParseError(Error):
    """Error on parsing an attachment file"""
    pass


class ConfigError(Error):
    """Error that occurs on config files"""
    pass


class QuboleError(Error):
    """Error that occurs on a Qubole failure"""
    pass


class FileError(Error):
    """Error that occurs on some file"""
    pass
