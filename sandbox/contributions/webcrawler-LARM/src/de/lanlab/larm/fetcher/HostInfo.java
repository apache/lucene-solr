package de.lanlab.larm.fetcher;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author Clemens Marschner
 * @version   1.0
 */

import java.util.HashMap;
import java.net.*;
import de.lanlab.larm.util.CachingQueue;
import de.lanlab.larm.util.Queue;

/**
 * contains information about a host. If a host doesn't respond too often, it's
 * excluded from the crawl.
 * This class is used by the HostManager
 *
 * @author    Clemens Marschner
 * @created   16. Februar 2002
 */
public class HostInfo
{
    static final String[] emptyKeepOutDirectories = new String[0];

    int id;
    int healthyCount = 5;   // five strikes, and you're out
    boolean isReachable = true;
    boolean robotTxtChecked = false;
    String[] disallows;    // robot exclusion
    boolean isLoadingRobotsTxt = false;
    Queue queuedRequests = null; // robot exclusion
    String hostName;

    public HostInfo(String hostName, int id)
    {
        this.id = id;
        this.disallows = HostInfo.emptyKeepOutDirectories;
        this.hostName = hostName;
    }

    /**
     * is this host reachable and responding?
     */
    public boolean isHealthy()
    {
        return (healthyCount > 0) && isReachable;
    }

    /**
     * signals that the host returned with a bad request of whatever type
     */
    public void badRequest()
    {
        healthyCount--;
    }

    public void setReachable(boolean reachable)
    {
        isReachable = reachable;
    }

    public boolean isReachable()
    {
        return isReachable;
    }

    public boolean isRobotTxtChecked()
    {
        return robotTxtChecked;
    }

    /**
     * must be synchronized externally
     */
    public boolean isLoadingRobotsTxt()
    {
        return this.isLoadingRobotsTxt;
    }

    public void setLoadingRobotsTxt(boolean isLoading)
    {
        this.isLoadingRobotsTxt = isLoading;
        if(isLoading)
        {
            this.queuedRequests = new CachingQueue("HostInfo_" + id + "_QueuedRequests", 100);
        }

    }

    public void setRobotsChecked(boolean isChecked, String[] disallows)
    {
        this.robotTxtChecked = isChecked;
        if(disallows != null)
        {
            this.disallows = disallows;
        }
        else
        {
            this.disallows = emptyKeepOutDirectories;
        }

    }

    public synchronized boolean isAllowed(String path)
    {
        // assume keepOutDirectories is pretty short
        // assert disallows != null
        int length = disallows.length;
        for(int i=0; i<length; i++)
        {
            if(path.startsWith(disallows[i]))
            {
                return false;
            }
        }
        return true;
    }
}
