package de.lanlab.larm.storage;

import de.lanlab.larm.storage.LinkStorage;
import de.lanlab.larm.util.SimpleLogger;
import de.lanlab.larm.fetcher.URLMessage;

import java.util.Collection;
import java.util.Iterator;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   1. Juni 2002
 */
public class LinkLogStorage implements LinkStorage
{

    SimpleLogger log;


    /**
     * Constructor for the LinkLogStorage object
     *
     * @param logFile  Description of the Parameter
     */
    public LinkLogStorage(SimpleLogger logFile)
    {
        this.log = logFile;
    }


    /**
     * empty
     */
    public void openLinkStorage()
    {
    }


    /**
     * Description of the Method
     *
     * @param c  Description of the Parameter
     * @return   Description of the Return Value
     */
    public Collection storeLinks(Collection c)
    {
        synchronized (log)
        {
            for (Iterator it = c.iterator(); it.hasNext(); )
            {
                log.log(((URLMessage) it.next()).getInfo());
            }
        }
        return c;
    }

}
