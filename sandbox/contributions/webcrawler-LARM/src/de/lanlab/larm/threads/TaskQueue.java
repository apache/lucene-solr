package de.lanlab.larm.threads;
import de.lanlab.larm.util.Queue;
import java.util.Collection;

/**
 * Title:        LARM Lanlab Retrieval Machine
 * Description:
 * Copyright:    Copyright (c)
 * Company:
 * @author
 * @version 1.0
 */

import java.util.LinkedList;
import java.util.Iterator;

public class TaskQueue implements Queue
{
    LinkedList queue = new LinkedList();

    /**
     *
     */
    public TaskQueue()
    {

    }


    public void insertMultiple(Collection c)
    {
      throw new UnsupportedOperationException();
    }

    /**
     * push a task to the start of the queue
     * @param i the task
     */
    public void insert(Object i)
    {
        queue.addFirst(i);
    }

    /**
     * get the last element out of the queue
     * The element will be removed from the queue
     * @return the task
     */
    public Object remove()
    {
       return queue.isEmpty() ? null : (InterruptableTask)queue.removeLast();
    }

    /**
     *
     */
    public Iterator iterator()
    {
        return queue.iterator();
    }

    /**
     *
     */
    public void clear()
    {
        queue.clear();
    }

    public boolean isEmpty()
    {
        return queue.isEmpty();
    }

    public int size()
    {
        return queue.size();
    }
}

