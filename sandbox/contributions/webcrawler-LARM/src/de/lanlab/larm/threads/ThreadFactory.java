
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.threads;

public class ThreadFactory
{
    // static int count = 0;

    public  ServerThread createServerThread(int count)
    {
        return new ServerThread(count);
    }
}