from consolemenu.items import MenuItem


class SelectionItem(MenuItem):
    """
    The item type used in :class:`consolemenu.SelectionMenu`
    """

    def __init__(self, text, index, menu=None):
        """
        :ivar int index: The index of this item in the list used to initialize the :class:`consolemenu.SelectionMenu`
        """
        super(SelectionItem, self).__init__(text=text, menu=menu, should_exit=True)
        self.index = index

    def get_return(self):
        """
        :return: The index of this item in the list of strings
        :rtype: int
        """
        return self.index
