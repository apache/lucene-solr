package de.lanlab.larm.fetcher;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @version   1.0
 */

import java.util.HashMap;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   16. Februar 2002
 */
public class HostManager
{
    HashMap hosts;
    static int hostCount = 0;


    /**
     * Constructor for the HostInfo object
     *
     * @param initialSize  Description of the Parameter
     */
    public HostManager(int initialCapacity)
    {
        hosts = new HashMap(initialCapacity);
    }


    /**
     * Description of the Method
     *
     * @param hostName  Description of the Parameter
     * @return          Description of the Return Value
     */
    public HostInfo put(String hostName)
    {
        if (!hosts.containsKey(hostName))
        {
            int hostID;
            synchronized (this)
            {
                hostID = hostCount++;
            }
            HostInfo hi = new HostInfo(hostName,hostID);
            hosts.put(hostName, hi);
            return hi;
        }
        return (HostInfo)hosts.get(hostName);
        /*else
        {
            hostID = hosts.get()
        }
        // assert hostID != -1;
        return hostID;*/

    }


    /**
     * Gets the hostID attribute of the HostInfo object
     *
     * @param hostName  Description of the Parameter
     * @return          The hostID value
     */
    public HostInfo getHostInfo(String hostName)
    {
        HostInfo hi = (HostInfo)hosts.get(hostName);
        if(hi == null)
        {
            return put(hostName);
        }
        return hi;
    }

    public int getSize()
    {
       return hosts.size();
    }
}
