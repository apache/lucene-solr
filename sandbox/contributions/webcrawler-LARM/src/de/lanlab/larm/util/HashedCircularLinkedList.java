package de.lanlab.larm.util;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.util.*;

/**
 * simple hashed linked list. It allows for inserting and removing elements like
 * in a hash table (in fact, it uses a HashMap), while still being able to easily
 * traverse the collection like a list. In addition, the iterator is circular. It
 * always returns a next element as long as there are elements in the list. In
 * contrast to the iterator of Sun's collection classes, this class can cope with
 * inserts and removals while traversing the list.<p>
 * Elements are always added to the end of the list, that is, always at the same place<br>
 * All operations should work in near constant time as the list grows. Only the
 * trade-off costs of a hash (memory versus speed) have to be considered.
 * The List doesn't accept null elements
 * @todo put the traversal function into an Iterator
 * @todo implement the class as a derivate from a Hash
 */
public class HashedCircularLinkedList
{


    /**
     * Entry class.
     */
    private static class Entry
    {
        Object key;
        Object element;
        Entry next;
        Entry previous;

        Entry(Object element, Entry next, Entry previous, Object key)
        {
            this.element = element;
            this.next = next;
            this.previous = previous;
            this.key = key;
        }
    }

    public Object getCurrentKey()
    {

        return current != null ?  current.key : null;

    }

    /**
     * the list. contains objects
     */
    private transient Entry header = new Entry(null, null, null, null);

    /**
     * the hash. maps keys to entries, which by themselves map to objects
     */
    HashMap keys;

    private transient int size = 0;

    /** the current entry in the traversal */
    Entry current = null;

    /**
     * Constructs an empty list.
     */
    public HashedCircularLinkedList(int initialCapacity, float loadFactor)
    {
        header.next = header.previous = header;
        keys = new HashMap(initialCapacity, loadFactor);
    }

    /**
     * Returns the number of elements in this list.
     *
     * @return the number of elements in this list.
     */
    public int size()
    {
        return size;
    }

    /**
     * Removes the first occurrence of the specified element in this list.  If
     * the list does not contain the element, it is unchanged.  More formally,
     * removes the element with the lowest index <tt>i</tt> such that
     * <tt>(o==null ? get(i)==null : o.equals(get(i)))</tt> (if such an
     * element exists).
     *
     * @param o element to be removed from this list, if present.
     * @return <tt>true</tt> if the list contained the specified element.
     */
    public boolean removeByKey(Object o)
    {
        // assert(o != null)
        Entry e = (Entry)keys.get(o);
        if(e != null)
        {
            if(e == current)
            {
                if(size > 1)
                {
                    current = previousEntry(current);
                }
                else
                {
                    current = null;
                }
            }
            this.removeEntryFromList(e);
            keys.remove(o);
            size--;
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Removes all of the elements from this list.
     */
    public void clear()
    {
        // list
        header.next = header.previous = header;

        // hash
        keys.clear();

        size = 0;
        current = null;
    }


    private Entry addEntryBefore(Object key, Object o, Entry e)
    {
        Entry newEntry = new Entry(o, e, e.previous, key);
        newEntry.previous.next = newEntry;
        newEntry.next.previous = newEntry;
        return newEntry;
    }

    private void removeEntryFromList(Entry e)
    {
        if(e != null)
        {
            if (e == header)
            {
                throw new NoSuchElementException();
            }

            e.previous.next = e.next;
            e.next.previous = e.previous;
        }
    }


    /**
     * (method description here)
     * defined in java.util.Map
     * @param p0 (parameter description here)
     * @param p1 (parameter description here)
     * @return (return value description here)
     */
    public boolean put(Object key, Object value)
    {
        if(key != null && !keys.containsKey(key))
        {
            Entry e = addEntryBefore(key, value, header);  // add it as the last element
            keys.put(key, e);                    // link key to entry
            size++;
            return true;
        }
        else
        {
            return false;
        }
    }


    public boolean hasNext()
    {
        return (size > 0);
    }

    private Entry nextEntry(Entry e)
    {
        // assert(e != null)
        if(size > 1)
        {
            if(e == null)
            {
                e = header;
            }
            Entry next = e.next;
            if(next == header)
            {
                next = next.next;
            }
            return next;
        }
        else if(size == 1)
        {
            return header.next;
        }
        else
        {
            return null;
        }
    }



    private Entry previousEntry(Entry e)
    {
        // assert(e != null)
        if(size > 1)
        {
            if(e == null)
            {
                e = header;
            }
            Entry previous = e.previous;
            if(previous == header)
            {
                previous = previous.previous;
            }
            return previous;
        }
        else if(size == 1)
        {
            return header.previous;
        }
        else
        {
            return null;
        }
    }

    public Object next()
    {
        current = nextEntry(current);
        if(current != null)
        {
            return current.element;
        }
        else
        {
            return null;
        }
    }

    public void removeCurrent()
    {
        keys.remove(current.key);
        removeEntryFromList(current);
    }


    public Object get(Object key)
    {
        Entry e = ((Entry)keys.get(key));
        if(e != null)
        {
            return e.element;
        }
        else
        {
            return null;
        }
    }

    /**
     * testing
     */
    public static void main(String[] args)
    {
        HashedCircularLinkedList h = new HashedCircularLinkedList(20, 0.75f);
        h.put("1", "a");
        h.put("2", "b");
        h.put("3", "c");
        String t;
        System.out.println("size [3]: " + h.size());
        t = (String)h.next();
        System.out.println("2nd element via get [b]: " + h.get("2"));

        System.out.println("next element [a]: " + t);
        t = (String)h.next();
        System.out.println("next element [b]: " + t);
        t = (String)h.next();
        System.out.println("next element [c]: " + t);
        t = (String)h.next();
        System.out.println("1st element after circular traversal [a]: " + t);
        h.removeByKey("1");
        System.out.println("1st element after remove [null]: " + h.get("1"));
        System.out.println("size after removal [2]: " + h.size());
        t = (String)h.next();
        System.out.println("next element [b]: " + t);
        t = (String)h.next();
        System.out.println("next element [c]: " + t);
        t = (String)h.next();
        System.out.println("next element [b]: " + t);
        h.removeCurrent();
        t = (String)h.next();
        System.out.println("next element after 1 removal [c]: " + t);
        t = (String)h.next();
        System.out.println("next element: [c]: " + t);
        h.removeByKey("3");
        System.out.println("size after 3 removals [0]: " + h.size());
        t = (String)h.next();
        System.out.println("next element [null]: " + t);
    }
}


