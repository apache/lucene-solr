
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.fetcher;
import de.lanlab.larm.threads.*;

/**
 * this factory simply creates fetcher threads. It's passed
 * to the ThreadPool because the pool is creating the threads on its own
 */
public class FetcherThreadFactory extends ThreadFactory
{

    //static int count = 0;

    ThreadGroup threadGroup = new ThreadGroup("FetcherThreads");

    HostManager hostManager;

    public FetcherThreadFactory(HostManager hostManager)
    {
        this.hostManager = hostManager;
    }


    public  ServerThread createServerThread(int count)
    {
        ServerThread newThread = new FetcherThread(count, threadGroup, hostManager);
        newThread.setPriority(4);
        return newThread;
    }
}