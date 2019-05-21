import logging

import six
from abc import ABCMeta, abstractmethod


class InvalidValidator(Exception):
    """
    Raised when expected a valid validator but something else given
    """
    pass


@six.add_metaclass(ABCMeta)
class BaseValidator(object):
    """
    Validator Base class, each validator should inherit from this one
    """

    def __init__(self):
        logging.basicConfig()
        self.log = logging.getLogger(type(self).__name__)

    @abstractmethod
    def validate(self, input_string):
        """

        This function should be implemented in the validators

        :param input_string: Input string from command line (provided by the user)
        :return: True in case validation success / False otherwise
        """
        pass
