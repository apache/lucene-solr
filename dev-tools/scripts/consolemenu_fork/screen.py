from __future__ import print_function

import platform
import subprocess
import sys
import textwrap


class Screen(object):
    """
    Class representing a console screen.
    """

    def __init__(self):
        self.__tw = textwrap.TextWrapper()
        # TODO get actual screen size
        self.__height = 40
        self.__width = 80

    @property
    def screen_height(self):
        """
        int: The screen height in rows.
        """
        return self.__height

    @property
    def screen_width(self):
        """
        int: The screen width in columns.
        """
        return self.__width

    @staticmethod
    def clear():
        """
        Clear the screen.
        """
        if platform.system() == 'Windows':
            subprocess.check_call('cls', shell=True)
        else:
            print(subprocess.check_output('clear').decode())

    @staticmethod
    def flush():
        """
        Flush any buffered standard output to screen.
        """
        sys.stdout.flush()

    def input(self, prompt=''):
        """
        Prompt the end user for input.

        Args:
            prompt (:obj:`str`, optional): The message to display as the prompt.

        Returns:
            The input provided by the user.
        """
        if sys.version[0] == '2':
            return raw_input(prompt)
        else:
            return input(prompt)

    @staticmethod
    def printf(*args):
        """
        Print the specified arguments to the screen.

        Args:
            *args: Variable length argument list.
        """
        print(*args, end='')

    @staticmethod
    def println(*args):
        """
        Print the specified arguments to the screen, including an appended newline character.

        Args:
            *args: Variable length argument list.
        """
        print(*args)
