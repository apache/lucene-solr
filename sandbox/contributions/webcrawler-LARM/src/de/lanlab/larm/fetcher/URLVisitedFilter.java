package de.lanlab.larm.fetcher;

import java.net.URL;
import java.util.*;

import de.lanlab.larm.util.SimpleLogger;

/**
 * contains a HashMap of all URLs already passed. Adds each URL to that list, or
 * consumes it if it is already present
 *
 * @todo find ways to reduce memory consumption here. the approach is somewhat naive
 *
 * @author    Clemens Marschner
 * @created   3. Januar 2002
 */
class URLVisitedFilter extends Filter implements MessageListener
{

    /**
     * Description of the Method
     *
     * @param handler  Description of the Parameter
     */
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
        this.messageHandler = handler;
    }


    MessageHandler messageHandler;

    SimpleLogger log;

    HashSet urlHash;

    static Boolean dummy = new Boolean(true);



    /**
     * Constructor for the URLVisitedFilter object
     *
     * @param initialHashCapacity  Description of the Parameter
     */
    public URLVisitedFilter(int initialHashCapacity, SimpleLogger log)
    {
        urlHash = new HashSet(initialHashCapacity);
        this.log = log;
        //urlVector = new Vector(initialHashCapacity);
    }


    /**
     * clears everything
     */
    public void clearHashtable()
    {
        urlHash.clear();
        // urlVector.clear();
    }



    /**
     * @param message  Description of the Parameter
     * @return         Description of the Return Value
     */
    public Message handleRequest(Message message)
    {
        if (message instanceof URLMessage)
        {
            URLMessage urlMessage = ((URLMessage) message);
            URL url = urlMessage.getUrl();
            String urlString = urlMessage.getURLString();
            if (urlHash.contains(urlString))
            {
                //System.out.println("URLVisitedFilter: " + urlString + " already present.");
                filtered++;
                if(log != null)
                {
                    log.logThreadSafe(urlMessage.getInfo());
                }
                return null;
            }
            else
            {
                // System.out.println("URLVisitedFilter: " + urlString + " not present yet.");
                urlHash.add(urlString);
                stringSize += urlString.length(); // see below
                //urlVector.add(urlString);
            }
        }
        return message;
    }


    private int stringSize = 0;

    /**
     * just a method to get a rough number of characters contained in the array
     * with that you see that the total memory  is mostly used by this class
     */
    public int getStringSize()
    {
        return stringSize;
    }

    public int size()
    {
        return urlHash.size();
    }

}
