class MenuPadding(object):
    """
    Class for menu padding. Padding is the area between the menu border and the content of the menu.

    Args:
        top (int): The top padding.
        left (int):  The left padding.
        bottom (int): The bottom padding.
        right (int): The right padding.
    """

    def __init__(self, top=1, left=2, bottom=1, right=2):
        self.__left = left
        self.__right = right
        self.__top = top
        self.__bottom = bottom

    @property
    def left(self):
        """
        The left padding.

        Returns:
            int: The left padding.
        """
        return self.__left

    @left.setter
    def left(self, left):
        self.__left = left

    @property
    def right(self):
        """
        The right padding.

        Returns:
            int: The right padding.
        """
        return self.__right

    @right.setter
    def right(self, right):
        self.__right = right

    @property
    def top(self):
        """
        The top padding.

        Returns:
            int: The top padding.
        """
        return self.__top

    @top.setter
    def top(self, top):
        self.__top = top

    @property
    def bottom(self):
        """
        The bottom padding.

        Returns:
            int: The bottom padding.
        """
        return self.__bottom

    @bottom.setter
    def bottom(self, bottom):
        self.__bottom = bottom
