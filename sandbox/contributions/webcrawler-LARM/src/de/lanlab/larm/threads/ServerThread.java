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

package de.lanlab.larm.threads;

import java.util.Vector;
import java.util.Iterator;
import java.io.*;
import java.util.*;
import de.lanlab.larm.util.*;

/**
 * This thread class acts like a server. It's running idle within
 * a thread pool until "runTask" is called. The given task will then
 * be executed asynchronously
 */
public class ServerThread extends Thread
{
    /**
     * the task that is to be executed. null in idle-mode
     */
    protected InterruptableTask task = null;

    private boolean busy = false;

    private ArrayList listeners = new ArrayList();
    private boolean isInterrupted = false;
    private int threadNumber;

    SimpleLogger log;
    SimpleLogger errorLog;

    public ServerThread(int threadNumber, String name, ThreadGroup threadGroup)
    {
        super(threadGroup, name);
        init(threadNumber);
    }


    public ServerThread(int threadNumber, String name)
    {
        super(name);
        init(threadNumber);
    }

    void init(int threadNumber)
    {
        this.threadNumber = threadNumber;
        File logDir = new File("logs");
        logDir.mkdir();
        log = new SimpleLogger("thread" + threadNumber);
        errorLog = new SimpleLogger("thread" + threadNumber + "_errors");

    }

    /**
     * constructor
     * @param threadNumber assigns an arbitrary number to this thread
     *        used by ServerThreadFactory
     */
    public ServerThread(int threadNumber)
    {
        init(threadNumber);
    }

    /**
     * the run method runs asynchronously. It waits until runTask() is
     * called
     */
    public void run()
    {
        try
        {

            while(!isInterrupted)
            {
                synchronized(this)
                {
                    while(task == null)
                    {
                        wait();
                    }
                }
                task.run(this);
                taskReady();
            }
        }
        catch(InterruptedException e)
        {
            System.out.println("ServerThread " + threadNumber + " interrupted");
            log.log("** Thread Interrupted **");
        }
    }


    /**
     * this is the main method that will invoke a task to run.
     */
    public synchronized void runTask(InterruptableTask t)
    {
        busy = true;
        task = t;
        notify();
    }

    /**
     * it should be possible to interrupt a task with this function.
     * therefore, the task has to check its interrupted()-state
     */
    public void interruptTask()
    {
        if(task != null)
        {
            task.interrupt();
        }
    }

    /**
     * the server thread can either be in idle or busy mode
     */
    public boolean isBusy()
    {
        return busy;
    }

    public void addTaskReadyListener(TaskReadyListener l)
    {
        listeners.add(l);
    }

    public void removeTaskReadyListener(TaskReadyListener l)
    {
        listeners.remove(l);
    }

    public void interrupt()
    {
        super.interrupt();
        isInterrupted = true;
    }

    public int getThreadNumber()
    {
        return this.threadNumber;
    }

    public InterruptableTask getTask()
    {
        return task;
    }

    /**
     * this method will be called when the task ends. It notifies all
     * of its observers about its changed state
     */
    protected void taskReady()
    {
        task = null;
        busy = false;
        Iterator Ie = listeners.iterator();
        while(Ie.hasNext())
        {
            ((TaskReadyListener)Ie.next()).taskReady(this);
        }
    }

    public SimpleLogger getLog()
    {
        return log;
    }

    public SimpleLogger getErrorLog()
    {
        return errorLog;
    }
}
