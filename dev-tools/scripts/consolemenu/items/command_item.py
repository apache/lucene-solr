import subprocess

from consolemenu.items import ExternalItem


class CommandItem(ExternalItem):
    """
    A menu item to execute a console command
    """

    def __init__(self, text, command, arguments=None, menu=None, should_exit=False):
        """
        :ivar str command: The console command to be executed
        :ivar list[str] arguments: An optional list of string arguments to be passed to the command
        :ivar int exit_status: the exit status of the command, None if it hasn't been run yet
        """
        super(CommandItem, self).__init__(text=text, menu=menu, should_exit=should_exit)
        self.command = command

        if arguments:
            self.arguments = arguments
        else:
            self.arguments = []

        self.exit_status = None

    def action(self):
        """
        This class overrides this method
        """
        commandline = "{0} {1}".format(self.command, " ".join(self.arguments))
        try:
            completed_process = subprocess.run(commandline, shell=True)
            self.exit_status = completed_process.returncode
        except AttributeError:
            self.exit_status = subprocess.call(commandline, shell=True)

    def get_return(self):
        """
        :return: the exit status of the command
        :rtype: int
        """
        return self.exit_status
