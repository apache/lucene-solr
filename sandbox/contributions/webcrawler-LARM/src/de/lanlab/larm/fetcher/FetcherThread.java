
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.fetcher;

import de.lanlab.larm.threads.ServerThread;
import de.lanlab.larm.util.State;

/**
 * a server thread for the thread pool that records the number
 * of bytes read and the number of tasks run
 * mainly for statistical purposes and to keep most of the information a task needs
 * static
 */
public class FetcherThread extends ServerThread
{

    long totalBytesRead = 0;
    long totalTasksRun  = 0;

    HostManager hostManager;

    byte[] documentBuffer = new byte[Constants.FETCHERTASK_READSIZE];

    public HostManager getHostManager()
    {
        return hostManager;
    }

    public FetcherThread(int threadNumber, ThreadGroup threadGroup, HostManager hostManager)
    {
        super(threadNumber,"FetcherThread " + threadNumber, threadGroup);
        this.hostManager = hostManager;
    }

    public static String STATE_IDLE = "Idle";

    State idleState = new State(STATE_IDLE); // only set if task is finished

    protected void taskReady()
    {
        totalBytesRead += ((FetcherTask)task).getBytesRead();
        totalTasksRun++;
        super.taskReady();
        idleState.setState(STATE_IDLE);

    }


    public long getTotalBytesRead()
    {
        if(task != null)
        {
            return totalBytesRead + ((FetcherTask)task).getBytesRead();
        }
        else
        {
            return totalBytesRead;
        }
    }

    public long getTotalTasksRun()
    {
        return totalTasksRun;
    }

    public byte[] getDocumentBuffer()
    {
        return documentBuffer;
    }

    public State getTaskState()
    {
        if(task != null)
        {
            // task could be null here
            return ((FetcherTask)task).getTaskState();
        }
        else
        {
            return idleState.cloneState();
        }
    }

}
