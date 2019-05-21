from __future__ import print_function

import platform
import threading

import os

from consolemenu.menu_formatter import MenuFormatBuilder
from consolemenu.screen import Screen


def callable_wrapper(o):
    """
    Checks if object is a function and returns the result, else returns plain string.
    This makes it possible to pass a method reference to a menu or item instead of a string,
    thus the menu can update itself dynamically as values change.
    """
    if callable(o):
        return o()
    else:
        return o


class ConsoleMenu(object):
    """
    A class that displays a menu and allows the user to select an option.

    Args:
        title (str): The title of the menu, or a method reference that returns a string.
        subtitle (str): The subtitle of the menu, or a method reference that returns a string.
        screen (:obj:`consolemenu.screen.Screen`): The screen object associated with this menu.
        formatter (:obj:`MenuFormatBuilder`): The MenuFormatBuilder instance used to format this menu.
        prologue_text (str): Text or method reference to include in the "prologue" section of the menu.
        epilogue_text (str): Text or method reference to include in the "epilogue" section of the menu.
        show_exit_option (bool): Specifies whether this menu should show an exit item by default. Defaults to True.
            Can be overridden when the menu is started.
        exit_option_text (str): Text for the Exit menu item. Defaults to 'Exit'.

    Attributes:
        cls.currently_active_menu (:obj:`ConsoleMenu`): Class variable that holds the currently active menu or None
            if no menu is currently active (e.g. when switching between menus)
        items (:obj:`list` of :obj:`MenuItem`): The list of MenuItems that the menu will display
        parent (:obj:`ConsoleMenu`): The parent of this menu
        previous_active_menu (:obj:`ConsoleMenu`): the previously active menu to be restored into the class's
            currently active menu
        current_option (int): The currently highlighted menu option
        selected_option (int): The option that the user has most recently selected
    """

    currently_active_menu = None

    def __init__(self, title=None, subtitle=None, screen=None, formatter=None,
                 prologue_text=None, epilogue_text=None,
                 show_exit_option=True, exit_option_text='Exit'):
        if screen is None:
            screen = Screen()
        self.screen = screen

        if formatter is None:
            formatter = MenuFormatBuilder()
        self.formatter = formatter

        self.title = title
        self.subtitle = subtitle
        self.prologue_text = prologue_text
        self.epilogue_text = epilogue_text

        self.highlight = None
        self.normal = None

        self.show_exit_option = show_exit_option

        self.items = list()

        self.parent = None

        self.exit_item = ExitItem(menu=self, text=exit_option_text)

        self.current_option = 0
        self.selected_option = -1

        self.returned_value = None

        self.should_exit = False

        self.previous_active_menu = None

        self._main_thread = None

        self._running = threading.Event()

    def __repr__(self):
        return "%s: %s. %d items" % (callable_wrapper(self.title), callable_wrapper(self.subtitle), len(self.items))

    @property
    def current_item(self):
        """
        :obj:`consolemenu.items.MenuItem`: The item corresponding to the menu option that is currently highlighted,
            or None.
        """
        if self.items:
            return self.items[self.current_option]
        else:
            return None

    @property
    def selected_item(self):
        """
        :obj:`consolemenu.items.MenuItem`:  The item in :attr:`items` that the user most recently selected, or None.
        """
        if self.items and self.selected_option != -1:
            return self.items[self.current_option]
        else:
            return None

    def append_item(self, item):
        """
        Add an item to the end of the menu before the exit item.

        Args:
            item (MenuItem): The item to be added.

        """
        did_remove = self.remove_exit()
        item.menu = self
        self.items.append(item)
        if did_remove:
            self.add_exit()

    def remove_item(self, item):
        """
        Remove the specified item from the menu.

        Args:
            item (MenuItem): the item to be removed.

        Returns:
            bool: True if the item was removed; False otherwise.
        """
        for idx, _item in enumerate(self.items):
            if item == _item:
                del self.items[idx]
                return True
        return False

    def add_exit(self):
        """
        Add the exit item if necessary. Used to make sure there aren't multiple exit items.

        Returns:
            bool: True if item needed to be added, False otherwise.
        """
        if not self.items or self.items[-1] is not self.exit_item:
            self.items.append(self.exit_item)
            return True
        return False

    def remove_exit(self):
        """
        Remove the exit item if necessary. Used to make sure we only remove the exit item, not something else.

        Returns:
            bool: True if item needed to be removed, False otherwise.
        """
        if self.items:
            if self.items[-1] is self.exit_item:
                del self.items[-1]
                return True
        return False

    def is_selected_item_exit(self):
        """
        Checks to determine if the currently selected item is the Exit Menu item.

        Returns:
            bool: True if the currently selected item is the Exit Menu item; False otherwise.
        """
        return self.selected_item and self.selected_item is self.exit_item

    def _wrap_start(self):
        self._main_loop()
        ConsoleMenu.currently_active_menu = None
        self.clear_screen()
        ConsoleMenu.currently_active_menu = self.previous_active_menu

    def start(self, show_exit_option=None):
        """
        Start the menu in a new thread and allow the user to interact with it.
        The thread is a daemon, so :meth:`join()<consolemenu.ConsoleMenu.join>` should be called if there's a
        possibility that the main thread will exit before the menu is done

        Args:
            show_exit_option (bool): Specify whether the exit item should be shown, defaults to the value
                set in the constructor

        """
        self.previous_active_menu = ConsoleMenu.currently_active_menu
        ConsoleMenu.currently_active_menu = None

        self.should_exit = False

        if show_exit_option is None:
            show_exit_option = self.show_exit_option

        if show_exit_option:
            self.add_exit()
        else:
            self.remove_exit()

        try:
            self._main_thread = threading.Thread(target=self._wrap_start, daemon=True)
        except TypeError:
            self._main_thread = threading.Thread(target=self._wrap_start)
            self._main_thread.daemon = True

        self._main_thread.start()

    def show(self, show_exit_option=None):
        """
        Calls start and then immediately joins.

        Args:
            show_exit_option (bool):  Specify whether the exit item should be shown, defaults to the value set
                in the constructor

        """
        self.start(show_exit_option)
        self.join()

    def _main_loop(self):
        self._set_up_colors()
        ConsoleMenu.currently_active_menu = self
        self._running.set()

        while self._running.wait() is not False and not self.should_exit:
            self.screen.clear()
            self.draw()
            self.process_user_input()

    def draw(self):
        """
        Refresh the screen and redraw the menu. Should be called whenever something changes that needs to be redrawn.
        """
        self.screen.printf(self.formatter.format(title=callable_wrapper(self.title),
                                                 subtitle=callable_wrapper(self.subtitle),
                                                 items=self.items,
                                                 prologue_text=callable_wrapper(self.prologue_text),
                                                 epilogue_text=callable_wrapper(self.epilogue_text)))

    def is_running(self):
        """
        Check if the menu has been started and is not paused.

        Returns:
            bool: True if the menu is started and hasn't been paused; False otherwise.
        """
        return self._running.is_set()

    def wait_for_start(self, timeout=None):
        """
        Block until the menu is started.

        Args:
            timeout:  How long to wait before timing out.

        Returns:
            bool: False if timeout is given and operation times out, True otherwise. None before Python 2.7.
        """
        return self._running.wait(timeout)

    def is_alive(self):
        """
        Check whether the thread is stil alive.

        Returns:
            bool: True if the thread is still alive; False otherwise.
        """
        return self._main_thread.is_alive()

    def pause(self):
        """
        Temporarily pause the menu until resume is called.
        """
        self._running.clear()

    def resume(self):
        """
        Sets the currently active menu to this one and resumes it.
        """
        ConsoleMenu.currently_active_menu = self
        self._running.set()

    def join(self, timeout=None):
        """
        Should be called at some point after :meth:`start()<consolemenu.ConsoleMenu.start>` to block until
        the menu exits.

        Args:
            timeout (Number): How long to wait before timing out.

        """
        self._main_thread.join(timeout=timeout)

    def get_input(self):
        """
        Can be overridden to change the input method.
        Called in :meth:`process_user_input()<consolemenu.ConsoleMenu.process_user_input>`

        :return: the ordinal value of a single character
        :rtype: int
        """
        return self.screen.input()

    def process_user_input(self):
        """
        Gets the next single character and decides what to do with it
        """
        user_input = self.get_input()

        try:
            num = int(user_input)
        except Exception:
            return
        if 0 < num < len(self.items) + 1:
            self.current_option = num - 1
            self.select()

        return user_input

    def go_to(self, option):
        """
        Go to the option entered by the user as a number

        :param option: the option to go to
        :type option: int
        """
        self.current_option = option
        self.draw()

    def go_down(self):
        """
        Go down one, wrap to beginning if necessary
        """
        if self.current_option < len(self.items) - 1:
            self.current_option += 1
        else:
            self.current_option = 0
        self.draw()

    def go_up(self):
        """
        Go up one, wrap to end if necessary
        """
        if self.current_option > 0:
            self.current_option += -1
        else:
            self.current_option = len(self.items) - 1
        self.draw()

    def select(self):
        """
        Select the current item and run it
        """
        self.selected_option = self.current_option
        self.selected_item.set_up()
        self.selected_item.action()
        self.selected_item.clean_up()
        self.returned_value = self.selected_item.get_return()
        self.should_exit = self.selected_item.should_exit

    def exit(self):
        """
        Signal the menu to exit, then block until it's done cleaning up
        """
        self.should_exit = True
        self.join()

    def _set_up_colors(self):
        # TODO add color support
        # curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        # self.highlight = curses.color_pair(1)
        # self.normal = curses.A_NORMAL
        pass

    def clear_screen(self):
        """
        Clear the screen belonging to this menu
        """
        self.screen.clear()


class MenuItem(object):
    """
    A generic menu item
    """

    def __init__(self, text, menu=None, should_exit=False):
        """
        :ivar str text: The text shown for this menu item
        :ivar ConsoleMenu menu: The menu to which this item belongs
        :ivar bool should_exit: Whether the menu should exit once this item's action is done
        """
        self.text = text
        self.menu = menu
        self.should_exit = should_exit

    def __str__(self):
        return "%s %s" % (callable_wrapper(self.menu.title), callable_wrapper(self.text))

    def show(self, index):
        """
        How this item should be displayed in the menu. Can be overridden, but should keep the same signature.

        Default is:

            1 - Item 1

            2 - Another Item

        :param int index: The index of the item in the items list of the menu
        :return: The representation of the item to be shown in a menu
        :rtype: str
        """
        return "%2d - %s" % (index + 1, callable_wrapper(self.text))

    def set_up(self):
        """
        Override to add any setup actions necessary for the item
        """
        pass

    def action(self):
        """
        Override to carry out the main action for this item.
        """
        pass

    def clean_up(self):
        """
        Override to add any cleanup actions necessary for the item
        """
        pass

    def get_return(self):
        """
        Override to change what the item returns.
        Otherwise just returns the same value the last selected item did.
        """
        return self.menu.returned_value

    def __eq__(self, o):
        return self.text == o.text and self.menu == o.menu and self.should_exit == o.should_exit


class ExitItem(MenuItem):
    """
    Used to exit the current menu. Handled by :class:`consolemenu.ConsoleMenu`
    """

    def __init__(self, text="Exit", menu=None):
        super(ExitItem, self).__init__(text=text, menu=menu, should_exit=True)

    def show(self, index):
        """
        ExitItem overrides this method to display appropriate Exit or Return text.
        """
        # If we have a parent menu, and no overriding exit text was specified,
        # change Exit text to "Return to {Parent Menu Title}"
        if self.menu and self.menu.parent and self.text == 'Exit':
            self.text = "Return to %s" % callable_wrapper(self.menu.parent.title)
        return super(ExitItem, self).show(index)


def clear_terminal():
    """
    Call the platform specific function to clear the terminal: cls on windows, reset otherwise
    """
    if platform.system().lower() == "windows":
        os.system('cls')
    else:
        os.system('reset')
