package de.lanlab.larm.util;

/**
 * Title:        LARM Lanlab Retrieval Machine
 * Description:
 * Copyright:    Copyright (c)
 * Company:
 * @author
 * @version 1.0
 */

import java.util.Collection;

public interface Queue
{
    public Object remove();
    public void insert(Object o);
    public void insertMultiple(Collection c);
    public int size();
}