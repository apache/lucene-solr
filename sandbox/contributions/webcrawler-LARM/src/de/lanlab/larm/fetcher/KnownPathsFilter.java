package de.lanlab.larm.fetcher;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @created   17. Februar 2002
 * @version   1.0
 */
import java.net.*;

/**
 * this can be considered a hack
 * @TODO implement this as a fast way to filter out different URL endings or beginnings
 */
public class KnownPathsFilter extends Filter implements MessageListener
{

    MessageHandler messageHandler;

    String[] pathsToFilter =
    {
        "/robots.txt"
    };

    String[] hostFilter =
    {
        "www.nm.informatik.uni-muenchen.de",
        "cgi.cip.informatik.uni-muenchen.de"
    };

    String[] filesToFilter =
    {
            // exclude Apache directory files
            "/?D=D",
            "/?S=D",
            "/?M=D",
            "/?N=D",
            "/?D=A",
            "/?S=A",
            "/?M=A",
            "/?N=A",
    };

    int pathLength;
    int fileLength;
    int hostLength;


    /**
     * Constructor for the KnownPathsFilter object
     */
    public KnownPathsFilter()
    {
        pathLength = pathsToFilter.length;
        fileLength = filesToFilter.length;
        hostLength = hostFilter.length;
    }


    /**
     * Description of the Method
     *
     * @param message  Description of the Parameter
     * @return         Description of the Return Value
     */
    public Message handleRequest(Message message)
    {
        URL url = ((URLMessage)message).getUrl();
        String file = url.getFile();
        String host = url.getHost();
        int i;
        for (i = 0; i < pathLength; i++)
        {
            if (file.startsWith(pathsToFilter[i]))
            {
                filtered++;
                return null;
            }
        }
        for (i = 0; i < fileLength; i++)
        {
            if (file.endsWith(filesToFilter[i]))
            {
                filtered++;
                return null;
            }
        }
        for (i = 0; i<hostLength; i++)
        {
            if(hostFilter[i].equals(host))
            {
                filtered++;
                return null;
            }
        }
        return message;
    }


    /**
     * will be called as soon as the Listener is added to the Message Queue
     *
     * @param handler  the Message Handler
     */
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
        this.messageHandler = messageHandler;
    }
}
