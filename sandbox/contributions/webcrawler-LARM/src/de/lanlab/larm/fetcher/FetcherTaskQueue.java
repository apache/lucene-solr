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

 package de.lanlab.larm.fetcher;

import de.lanlab.larm.threads.*;
import de.lanlab.larm.util.*;
import java.util.*;
import java.net.URL;

/**
 * this special kind of task queue reorders the incoming tasks so that every subsequent
 * task is for a different host.
 * This is done by a "HashedCircularLinkedList" which allows random adding while
 * a differnet thread iterates through the collection circularly.
 *
 * @author    Clemens Marschner
 * @created   23. November 2001
 * @version $Id$
 */
public class FetcherTaskQueue extends TaskQueue
{
    /**
     * this is a hash that contains an entry for each server, which by itself is a
     * CachingQueue that stores all tasks for this server
     * @TODO probably link this to the host info structure
     */
    HashedCircularLinkedList servers = new HashedCircularLinkedList(100, 0.75f);
    int size = 0;


    /**
     * Constructor for the FetcherTaskQueue object. Does nothing
     */
    public FetcherTaskQueue() { }


    /**
     * true if no task is queued
     *
     * @return   The empty value
     */
    public boolean isEmpty()
    {
        return (size == 0);
    }


    /**
     * clear the queue. not synchronized.
     */
    public void clear()
    {
        servers.clear();
    }


    /**
     * puts task into Queue.
     * Warning: not synchronized
     *
     * @param t  the task to be added. must be a FetcherTask
     */
    public void insert(Object t)
    {
        // assert (t != null && t.getURL() != null)

        URLMessage um = ((FetcherTask)t).getActURLMessage();
        URL act = um.getUrl();
        String host = act.getHost();
        Queue q;
        q = ((Queue) servers.get(host));
        if (q == null)
        {
            // add a new host to the queue
            //String host2 = host.replace(':', '_').replace('/', '_').replace('\\', '_');
            // make it file system ready
            q = new CachingQueue(host, 100);
            servers.put(host, q);
        }
        // assert((q != null) && (q instanceof FetcherTaskQueue));
        q.insert(t);
        size++;
    }


    /**
     * the size of the queue. make sure that insert() and size() calls are synchronized
     * if the exact number matters.
     *
     * @return   Description of the Return Value
     */
    public int size()
    {
        return size;
    }

    /**
     * the number of different hosts queued at the moment
     */
    public int getNumHosts()
    {
        return servers.size();
    }

    /**
     * get the next task. warning: not synchronized
     *
     * @return   Description of the Return Value
     */
    public Object remove()
    {
        FetcherTask t = null;
        if (servers.size() > 0)
        {
            Queue q = (Queue) servers.next();
            // assert(q != null && q.size() > 0)
            t = (FetcherTask)q.remove();
            if (q.size() == 0)
            {
                servers.removeCurrent();
                q = null;
            }
            size--;
        }
        return t;
    }


    /**
     * tests
     *
     * @param args  Description of the Parameter
     */
    public static void main(String args[])
    {
        FetcherTaskQueue q = new FetcherTaskQueue();
        de.lanlab.larm.net.HostManager hm = new de.lanlab.larm.net.HostManager(10);
        System.out.println("Test 1. put in 4 yahoos and 3 lmus. pull out LMU/Yahoo/LMU/Yahoo/LMU/Yahoo/Yahoo");
        try
        {
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.lmu.de/1"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.lmu.de/2"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/1"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/2"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/3"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/4"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.lmu.de/3"), null, false, null, hm)));
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }

        System.out.println(((FetcherTask) q.remove()).getInfo());
        System.out.println(((FetcherTask) q.remove()).getInfo());
        System.out.println(((FetcherTask) q.remove()).getInfo());
        System.out.println(((FetcherTask) q.remove()).getInfo());
        System.out.println(((FetcherTask) q.remove()).getInfo());
        System.out.println(((FetcherTask) q.remove()).getInfo());
        System.out.println(((FetcherTask) q.remove()).getInfo());

        System.out.println("Test 2. new Queue");
        q = new FetcherTaskQueue();
        System.out.println("size [0]:");
        System.out.println(q.size());
        try
        {
            System.out.println("put 3 lmus.");
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.lmu.de/1"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.lmu.de/2"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.lmu.de/3"), null, false, null, hm)));
            System.out.print("pull out 1st element [lmu/1]: ");
            System.out.println(((FetcherTask) q.remove()).getInfo());
            System.out.println("size now [2]: " + q.size());
            System.out.print("pull out 2nd element [lmu/2]: ");
            System.out.println(((FetcherTask) q.remove()).getInfo());
            System.out.println("size now [1]: " + q.size());
            System.out.println("put in 3 yahoos");
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/1"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/2"), null, false, null, hm)));
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/3"), null, false, null, hm)));
            System.out.println("remove [?]: " + ((FetcherTask) q.remove()).getInfo());
            System.out.println("Size now [3]: " + q.size());
            System.out.println("remove [?]: " + ((FetcherTask) q.remove()).getInfo());
            System.out.println("Size now [2]: " + q.size());
            System.out.println("remove [?]: " + ((FetcherTask) q.remove()).getInfo());
            System.out.println("Size now [1]: " + q.size());
            System.out.println("put in another Yahoo");
            q.insert(new FetcherTask(new URLMessage(new URL("http://www.yahoo.de/4"), null, false, null, hm)));
            System.out.println("remove [?]: " + ((FetcherTask) q.remove()).getInfo());
            System.out.println("Size now [1]: " + q.size());
            System.out.println("remove [?]: " + ((FetcherTask) q.remove()).getInfo());
            System.out.println("Size now [0]: " + q.size());
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }

    }

}
