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

import de.lanlab.larm.threads.ThreadPool;
import de.lanlab.larm.threads.ThreadPoolObserver;
import de.lanlab.larm.threads.InterruptableTask;
import de.lanlab.larm.storage.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;

import de.lanlab.larm.fetcher.FetcherTask;
import de.lanlab.larm.net.*;

/**
 * filter class; the Fetcher is the main class which keeps the ThreadPool that
 * gets the documents. It should be placed at the very end of the MessageQueue,
 * so that all filtering can be made beforehand.
 *
 * @author    Clemens Marschner
 * @version $Id$
 */

public class Fetcher implements MessageListener
{
    /**
     * holds the threads
     */
    ThreadPool fetcherPool;

    /**
     * total number of docs read
     */
    int docsRead = 0;

    /**
     * the storage where the docs are saved to
     */
    DocumentStorage storage;

    /**
     * the host manager keeps track of host information
     */
    HostManager hostManager;


    /**
     * initializes the fetcher with the given number of threads in the thread
     * pool and a document storage.
     *
     * @param maxThreads   the number of threads in the ThreadPool
     * @param storage      the storage where all documents are stored
     * @param hostManager  the host manager
     */
    public Fetcher(int maxThreads, DocumentStorage docStorage, LinkStorage linkStorage, HostManager hostManager)
    {
        this.storage = storage;
        FetcherTask.setDocStorage(docStorage);
        FetcherTask.setLinkStorage(linkStorage);
        fetcherPool = new ThreadPool(maxThreads, new FetcherThreadFactory(hostManager));
        fetcherPool.setQueue(new FetcherTaskQueue());
        docsRead = 0;
        this.hostManager = hostManager;
    }


    /**
     * initializes the pool with default values (5 threads, NullStorage)
     */
    public void init()
    {
        fetcherPool.init();
    }


    /**
     * initializes the pool with a NullStorage and the given number of threads
     *
     * @param maxThreads  the number of threads in the thread pool
     */
    public void init(int maxThreads)
    {
        fetcherPool.init();
        docsRead = 0;
    }


    /**
     * this function will be called by the message handler each time a URL
     * passes all filters and gets to the fetcher. From here, it will be
     * distributed to the FetcherPool, a thread pool which carries out the task,
     * that is to fetch the document from the web.
     *
     * @param message  the message, which should actually be a URLMessage
     * @return         Description of the Return Value
     */
    public Message handleRequest(Message message)
    {
        URLMessage urlMessage = (URLMessage) message;

        fetcherPool.doTask(new FetcherTask(urlMessage), "");
        docsRead++;

        // eat the message
        return null;
    }


    /**
     * called by the message handler when this object is added to it
     *
     * @param handler  the message handler
     */
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
        this.messageHandler = handler;
        FetcherTask.setMessageHandler(handler);
    }


    MessageHandler messageHandler;


    /**
     * the thread pool observer will be called each time a thread changes its
     * state, i.e. from IDLE to RUNNING, and each time the number of thread
     * queue entries change.
     * this just wraps the thread pool method
     *
     * @param t  the class that implements the ThreadPoolObserver interface
     */
    public void addThreadPoolObserver(ThreadPoolObserver t)
    {
        fetcherPool.addThreadPoolObserver(t);
    }


    /**
     * returns the number of tasks queued. Should return 0 if there are any idle
     * threads. this method just wraps the ThreadPool method
     *
     * @return   The queueSize value
     */
    public int getQueueSize()
    {
        return fetcherPool.getQueueSize();
    }


    /**
     * get the total number of threads.
     * this method just wraps the ThreadPool method
     *
     * @return   The workingThreadsCount value
     */
    public int getWorkingThreadsCount()
    {
        return fetcherPool.getIdleThreadsCount() + fetcherPool.getBusyThreadsCount();
    }


    /**
     * get the number of threads that are currently idle.
     * this method just wraps the ThreadPool method
     *
     * @return   The idleThreadsCount value
     */
    public int getIdleThreadsCount()
    {
        return fetcherPool.getIdleThreadsCount();
    }


    /**
     * get the number of threads that are currently busy.
     * this method just wraps the ThreadPool method
     *
     * @return   The busyThreadsCount value
     */
    public int getBusyThreadsCount()
    {
        return fetcherPool.getBusyThreadsCount();
    }


    /**
     * Gets the threadPool attribute of the Fetcher object
     * beware: the original object is returned
     *
     * @TODO remove this / make it private if possible
     * @return   The threadPool value
     */
    public ThreadPool getThreadPool()
    {
        return fetcherPool;
    }


    /**
     * Gets the total number of docs read
     *
     * @return   number of docs read
     */
    public int getDocsRead()
    {
        return docsRead;
    }


    /**
     * returns the (original) task queue
     * @TODO remove this if possible
     * @return   The taskQueue value
     */
    public FetcherTaskQueue getTaskQueue()
    {
        return (FetcherTaskQueue) this.fetcherPool.getTaskQueue();
    }
}
