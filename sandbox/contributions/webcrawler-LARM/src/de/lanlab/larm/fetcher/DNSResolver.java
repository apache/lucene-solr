/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

package de.lanlab.larm.fetcher;

import java.util.*;
import java.net.*;

/**
 * filter class; gets IP Adresses from host names and forwards them to
 * the other parts of the application
 * since URLs cache their IP addresses themselves, and HTTP 1.1 needs the
 * host names to be sent to the server, this class is not used anymore
 * @version $Id$
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