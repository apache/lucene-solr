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
 * @version $Id$
 */
public class URLVisitedFilter extends Filter implements MessageListener
{

    /**
     * Description of the Method
     *
     * @param handler  Description of the Parameter
     */
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
    }


    //SimpleLogger log;

    HashSet urlHash;

    static Boolean dummy = new Boolean(true);



    /**
     * Constructor for the URLVisitedFilter object
     *
     * @param initialHashCapacity  Description of the Parameter
     */
    public URLVisitedFilter(int initialHashCapacity)
    {
        urlHash = new HashSet(initialHashCapacity);
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
            String urlString = urlMessage.getNormalizedURLString();
            if (urlHash.contains(urlString))
            {
                //System.out.println("URLVisitedFilter: " + urlString + " already present.");
                filtered++;
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
