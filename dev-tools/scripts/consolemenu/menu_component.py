import textwrap

from consolemenu.format import MenuStyle


class Dimension(object):
    """
    The Dimension class encapsulates the height and width of a component.

    Args:
        width (int): the width of the Dimension, in columns.
        height (int): the height of the Dimension, in rows.
        dimension (Dimension, optional): an existing Dimension from which to duplicate the height and width.
    """

    def __init__(self, width=0, height=0, dimension=None):
        self.width = width
        self.height = height
        if dimension is not None:
            self.width = dimension.width
            self.height = dimension.height


class MenuComponent(object):
    """
    Base class for a menu component.

    Args:
        menu_style (:obj:`MenuStyle`): the style for this component.
        max_dimension (:obj:`Dimension`): the maximum Dimension (width x height) for the menu. Defaults to width=80
            and height=40 if not specified.

    Raises:
        TypeError: if menu_style is not a :obj:`MenuStyle`.
    """

    def __init__(self, menu_style, max_dimension=None):
        if not isinstance(menu_style, MenuStyle):
            raise TypeError('menu_style must be of type MenuStyle')
        if max_dimension is None:
            max_dimension = Dimension(width=80, height=40)
        self.__max_dimension = max_dimension
        self.__style = menu_style

    @property
    def max_dimension(self):
        """
        :obj:`Dimension`: The maximum dimension for the menu.
        """
        return self.__max_dimension

    @property
    def style(self):
        """
        :obj:`consolemenu.format.MenuStyle`: The style for this component.
        """
        return self.__style

    @property
    def margins(self):
        """
        :obj:`consolemenu.format.MenuMargins`: The margins for this component.
        """
        return self.__style.margins

    @property
    def padding(self):
        """
        :obj:`consolemenu.format.MenuPadding`: The padding for this component.
        """
        return self.__style.padding

    @property
    def border_style(self):
        """
        :obj:`consolemenu.format.MenuBorderStyle`: The border style for this component.
        """
        return self.__style.border_style

    def calculate_border_width(self):
        """
        Calculate the width of the menu border. This will be the width of the maximum allowable
        dimensions (usually the screen size), minus the left and right margins and the newline character.
        For example, given a maximum width of 80 characters, with left and right margins both
        set to 1, the border width would be 77 (80 - 1 - 1 - 1 = 77).

        Returns:
            int: the menu border width in columns.
        """
        return self.max_dimension.width - self.margins.left - self.margins.right - 1  # 1=newline

    def calculate_content_width(self):
        """
        Calculate the width of inner content of the border.  This will be the width of the menu borders,
        minus the left and right padding, and minus the two vertical border characters.
        For example, given a border width of 77, with left and right margins each set to 2, the content
        width would be 71 (77 - 2 - 2 - 2 = 71).

        Returns:
            int: the inner content width in columns.
        """
        return self.calculate_border_width() - self.padding.left - self.padding.right - 2

    def generate(self):
        """
        Generate this component.

        Yields:
            str: The next string of characters for drawing this component.
        """
        raise NotImplemented()

    def inner_horizontals(self):
        """
        The string of inner horizontal border characters of the required length for this component (not including
        the menu margins or verticals).

        Returns:
            str: The inner horizontal characters.
        """
        return u"{0}".format(self.border_style.inner_horizontal * (self.calculate_border_width() - 2))

    def inner_horizontal_border(self):
        """
        The complete inner horizontal border section, including the left and right border verticals.

        Returns:
            str: The complete inner horizontal border.
        """
        return u"{lm}{lv}{hz}{rv}".format(lm=' ' * self.margins.left,
                                          lv=self.border_style.outer_vertical_inner_right,
                                          rv=self.border_style.outer_vertical_inner_left,
                                          hz=self.inner_horizontals())

    def outer_horizontals(self):
        """
        The string of outer horizontal border characters of the required length for this component (not including
        the menu margins or verticals).

        Returns:
            str: The outer horizontal characters.
        """
        return u"{0}".format(self.border_style.outer_horizontal * (self.calculate_border_width() - 2))

    def outer_horizontal_border_bottom(self):
        """
        The complete outer bottom horizontal border section, including left and right margins.

        Returns:
            str: The bottom menu border.
        """
        return u"{lm}{lv}{hz}{rv}".format(lm=' ' * self.margins.left,
                                          lv=self.border_style.bottom_left_corner,
                                          rv=self.border_style.bottom_right_corner,
                                          hz=self.outer_horizontals())

    def outer_horizontal_border_top(self):
        """
        The complete outer top horizontal border section, including left and right margins.

        Returns:
            str: The top menu border.
        """
        return u"{lm}{lv}{hz}{rv}".format(lm=' ' * self.margins.left,
                                          lv=self.border_style.top_left_corner,
                                          rv=self.border_style.top_right_corner,
                                          hz=self.outer_horizontals())

    def row(self, content='', align='left'):
        """
        A row of the menu, which comprises the left and right verticals plus the given content.

        Returns:
            str: A row of this menu component with the specified content.
        """
        return u"{lm}{vert}{cont}{vert}".format(lm=' ' * self.margins.left,
                                                vert=self.border_style.outer_vertical,
                                                cont=self._format_content(content, align))

    @staticmethod
    def _alignment_char(align):
        if str(align).strip() == 'center':
            return '^'
        elif str(align).strip() == 'right':
            return '>'
        else:
            return '<'

    def _format_content(self, content='', align='left'):
        return '{lp}{text:{al}{width}}{rp}'.format(lp=' ' * self.padding.left,
                                                   rp=' ' * self.padding.right,
                                                   text=content, al=self._alignment_char(align),
                                                   width=(self.calculate_border_width() - self.padding.left -
                                                          self.padding.right - 2))


class MenuHeader(MenuComponent):
    """
    The menu header section.
    The menu header contains the top margin, menu top, title/subtitle verticals, bottom padding verticals,
    and optionally a bottom border to separate the header from the next section.
    """

    def __init__(self, menu_style, max_dimension=None, title=None, title_align='left',
                 subtitle=None, subtitle_align='left', show_bottom_border=False):
        super(MenuHeader, self).__init__(menu_style, max_dimension)
        self.title = title
        self.title_align = title_align
        self.subtitle = subtitle
        self.subtitle_align = subtitle_align
        self.show_bottom_border = show_bottom_border

    def generate(self):
        for x in range(0, self.margins.top):
            yield ''
        yield self.outer_horizontal_border_top()
        for x in range(0, self.padding.top):
            yield self.row()
        if self.title is not None and self.title != '':
            yield self.row(content=self.title, align=self.title_align)
        if self.subtitle is not None and self.subtitle != '':
            yield self.row()
            yield self.row(content=self.subtitle, align=self.subtitle_align)
        for x in range(0, self.padding.bottom):
            yield self.row()
        if self.show_bottom_border:
            yield self.inner_horizontal_border()


class MenuTextSection(MenuComponent):
    """
    The menu text block section.
    A text block section can be used for displaying text to the user above or below the main items section.
    """

    def __init__(self, menu_style, max_dimension=None, text=None, text_align='left',
                 show_top_border=False, show_bottom_border=False):
        super(MenuTextSection, self).__init__(menu_style, max_dimension)
        self.text = text
        self.text_align = text_align
        self.show_top_border = show_top_border
        self.show_bottom_border = show_bottom_border

    def generate(self):
        if self.show_top_border:
            yield self.inner_horizontal_border()
        for x in range(0, self.padding.top):
            yield self.row()
        if self.text is not None and self.text != '':
            for line in textwrap.wrap(self.text, width=self.calculate_content_width()):
                yield self.row(content=line, align=self.text_align)
        for x in range(0, self.padding.bottom):
            yield self.row()
        if self.show_bottom_border:
            yield self.inner_horizontal_border()


class MenuItemsSection(MenuComponent):
    """
    The menu section for displaying the menu items.
    """

    def __init__(self, menu_style, max_dimension=None, items=None, items_align='left'):
        super(MenuItemsSection, self).__init__(menu_style, max_dimension)
        if items is not None:
            self.__items = items
        else:
            self.__items = list()
        self.items_align = items_align
        self.__top_border_dict = dict()
        self.__bottom_border_dict = dict()

    @property
    def items(self):
        return self.__items

    @items.setter
    def items(self, items):
        self.__items = items

    @property
    def items_with_bottom_border(self):
        """
        Return a list of the names (the item text property) of all items that should show a bottom border.
        :return: a list of item names that should show a bottom border.
        """
        return self.__bottom_border_dict.keys()

    @property
    def items_with_top_border(self):
        """
        Return a list of the names (the item text property) of all items that should show a top border.
        :return: a list of item names that should show a top border.
        """
        return self.__top_border_dict.keys()

    def show_item_bottom_border(self, item_text, flag):
        """
        Sets a flag that will show a bottom border for an item with the specified text.
        :param item_text: the text property of the item
        :param flag: boolean specifying if the border should be shown.
        """
        if flag:
            self.__bottom_border_dict[item_text] = True
        else:
            self.__bottom_border_dict.pop(item_text, None)

    def show_item_top_border(self, item_text, flag):
        """
        Sets a flag that will show a top border for an item with the specified text.
        :param item_text: the text property of the item
        :param flag: boolean specifying if the border should be shown.
        """
        if flag:
            self.__top_border_dict[item_text] = True
        else:
            self.__top_border_dict.pop(item_text, None)

    def generate(self):
        for x in range(0, self.padding.top):
            yield self.row()
        for index, item in enumerate(self.items):
            if item.text in self.items_with_top_border:
                yield self.inner_horizontal_border()
            yield self.row(content=item.show(index), align=self.items_align)
            if item.text in self.items_with_bottom_border:
                yield self.inner_horizontal_border()
        for x in range(0, self.padding.bottom):
            yield self.row()


class MenuFooter(MenuComponent):
    """
    The menu footer section.
    The menu footer contains the menu bottom, bottom padding verticals, and bottom margin.
    """

    def generate(self):
        for x in range(0, self.padding.top):
            yield self.row()
        yield self.outer_horizontal_border_bottom()
        for x in range(0, self.margins.bottom):
            yield ''


class MenuPrompt(MenuComponent):
    """
    A string representing the menu prompt for user input.
    """

    def __init__(self, menu_style, max_dimension=None, prompt_string=">>"):
        super(MenuPrompt, self).__init__(menu_style, max_dimension)
        self.__prompt = prompt_string

    @property
    def prompt(self):
        return self.__prompt

    @prompt.setter
    def prompt(self, prompt):
        self.__prompt = prompt

    def generate(self):
        for x in range(0, self.padding.top):
            yield ''
        for line in self.prompt.split():
            yield u"{lm}{line} ".format(lm=' ' * self.margins.left, line=line)
