from consolemenu import ConsoleMenu
from consolemenu.items import SelectionItem


class SelectionMenu(ConsoleMenu):
    """
    A menu that simplifies item creation, just give it a list of strings and it builds the menu for you

    Args:
        strings (:obj:`list` of :obj:`str`):  The list of strings this menu should be built from.
        title (str): The title of the menu.
        subtitle (str): The subtitle of the menu.
        screen (:obj:`consolemenu.screen.Screen`): The screen object associated with this menu.
        formatter (:obj:`MenuFormatBuilder`): The MenuFormatBuilder instance used to format this menu.
        prologue_text (str): Text to include in the "prologue" section of the menu.
        epilogue_text (str): Text to include in the "epilogue" section of the menu.
        show_exit_option (bool): Specifies whether this menu should show an exit item by default. Defaults to True.
            Can be overridden when the menu is started.
        exit_option_text (str): Text for the Exit menu item. Defaults to 'Exit'.
    """

    def __init__(self, strings, title=None, subtitle=None, screen=None, formatter=None,
                 prologue_text=None, epilogue_text=None, show_exit_option=True, exit_option_text='Exit'):
        super(SelectionMenu, self).__init__(title, subtitle, screen=screen, formatter=formatter,
                                            prologue_text=prologue_text, epilogue_text=epilogue_text,
                                            show_exit_option=show_exit_option, exit_option_text=exit_option_text)
        for index, item in enumerate(strings):
            self.append_item(SelectionItem(item, index, self))

    @classmethod
    def get_selection(cls, strings, title="Select an option", subtitle=None, show_exit_option=True, _menu=None):
        """
        Single-method way of getting a selection out of a list of strings.

        Args:
            strings (:obj:`list` of :obj:`str`):  The list of strings this menu should be built from.
            title (str): The title of the menu.
            subtitle (str): The subtitle of the menu.
            show_exit_option (bool): Specifies whether this menu should show an exit item by default. Defaults to True.
            _menu: Should probably only be used for testing, pass in a list and the created menu used internally by
                the method will be appended to it

        Returns:
            int: The index of the selected option.

        """
        menu = cls(strings, title, subtitle, show_exit_option=show_exit_option)
        if _menu is not None:
            _menu.append(menu)
        menu.show()
        menu.join()
        return menu.selected_option

    def append_string(self, string):
        self.append_item(SelectionItem(string))
