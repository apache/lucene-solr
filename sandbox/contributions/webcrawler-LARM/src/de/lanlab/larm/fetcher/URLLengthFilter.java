package de.lanlab.larm.fetcher;

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * @author
 * @created   28. Januar 2002
 * @version   1.0
 */

/**
 * Title: LARM Lanlab Retrieval Machine Description: Copyright: Copyright (c)
 * Company:
 *
 * kills URLs longer than X characters. Used to prevent endless loops where
 * the page contains the current URL + some extension
 *
 * @author Clemens Marschner
 * @created   28. Januar 2002
 */

public class URLLengthFilter extends Filter implements MessageListener
{
    /**
     * called by the message handler
     *
     * @param handler  the handler
     */
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
        this.messageHandler = handler;
    }


    MessageHandler messageHandler;

    int maxLength;


    /**
     * Constructor for the URLLengthFilter object
     *
     * @param maxLength  max length of the _total_ URL (protocol+host+port+path)
     */
    public URLLengthFilter(int maxLength)
    {
        this.maxLength = maxLength;
    }


    /**
     * handles the message
     *
     * @param message  Description of the Parameter
     * @return         the original message or NULL if the URL was too long
     */
    public Message handleRequest(Message message)
    {
        URLMessage m = (URLMessage) message;
        String file = m.getUrl().getFile();
        if (file != null && file.length() > maxLength) // path + query
        {
            filtered++;
            return null;
        }
        return message;
    }
}
