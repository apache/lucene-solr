from re import match

from consolemenu.validators.base import BaseValidator


class RegexValidator(BaseValidator):

    def __init__(self, pattern):
        super(RegexValidator, self).__init__()
        self.__pattern = pattern

    @property
    def pattern(self):
        return self.__pattern

    def validate(self, input_string):
        """
        Validate input_string against a regex pattern

        :return: True if match / False otherwise
        """
        validation_result = False
        try:
            validation_result = bool(match(pattern=self.pattern, string=input_string))
        except TypeError as e:
            self.log.error(
                'Exception while validating Regex, pattern={}, input_string={} - exception: {}'.format(self.pattern,
                                                                                                       input_string,
                                                                                                       e))
        return validation_result
