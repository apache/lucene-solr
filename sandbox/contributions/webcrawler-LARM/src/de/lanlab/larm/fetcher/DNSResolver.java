
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c)<p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.fetcher;

import java.util.*;
import java.net.*;

/**
 * filter class; gets IP Adresses from host names and forwards them to
 * the other parts of the application
 * since URLs cache their IP addresses themselves, and HTTP 1.1 needs the
 * host names to be sent to the server, this class is not used anymore
 */
public class DNSResolver implements MessageListener
{

    HashMap ipCache = new HashMap();


    public DNSResolver()
    {
    }

    public void notifyAddedToMessageHandler(MessageHandler m)
    {
        this.messageHandler = m;
    }

    MessageHandler messageHandler;

    public Message handleRequest(Message message)
    {
        if(message instanceof URLMessage)
        {
            URL url = ((URLMessage)message).getUrl();
            String host = url.getHost();
            InetAddress ip;
            /*InetAddress ip = (InetAddress)ipCache.get(host);

            if(ip == null)
            {
                */

                try
                {
                     ip = InetAddress.getByName(host);
                    /*
                    ipCache.put(host, ip);
                    //System.out.println("DNSResolver: new Cache Entry \"" + host + "\" = \"" + ip.getHostAddress() + "\"");*/
                }
                catch(UnknownHostException e)
                {
                    ip = null;
                    return null;
                    //System.out.println("DNSResolver: unknown host \"" + host + "\"");
                }
            /*}
            else
            {
               //System.out.println("DNSResolver: Cache hit: " +  ip.getHostAddress());
            }*/
            //((URLMessage)message).setIpAddress(ip);
        }
        return message;
    }
}