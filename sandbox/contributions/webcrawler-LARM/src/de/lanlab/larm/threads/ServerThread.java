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
