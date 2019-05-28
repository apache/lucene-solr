class MenuMargins(object):
    """
    Class for menu margins. A margin is the area between the maximum specified dimensions (which is usually
    the width and height of the screen) and the menu border.

    Args:
        top (int): The top margin.
        left (int):  The left margin.
        bottom (int): The bottom margin.
        right (int): The right margin.
    """

    def __init__(self, top=1, left=2, bottom=0, right=2):
        self.__left = left
        self.__right = right
        self.__top = top
        self.__bottom = bottom

    @property
    def left(self):
        """
        The left margin.

        Returns:
            int: The left margin.

        """
        return self.__left

    @left.setter
    def left(self, left):
        self.__left = left

    @property
    def right(self):
        """
        The right margin.

        Returns:
            int: The right margin.
        """
        return self.__right

    @right.setter
    def right(self, right):
        self.__right = right

    @property
    def top(self):
        """
        The top margin.

        Returns:
            int: The top margin.
        """
        return self.__top

    @top.setter
    def top(self, top):
        self.__top = top

    @property
    def bottom(self):
        """
        The bottom margin.

        Returns:
            int: The bottom margin.
        """
        return self.__bottom

    @bottom.setter
    def bottom(self, bottom):
        self.__bottom = bottom
