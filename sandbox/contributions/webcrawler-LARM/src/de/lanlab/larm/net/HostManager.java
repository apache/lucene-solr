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

package de.lanlab.larm.net;

import java.util.HashMap;

/**
 * Description of the Class
 *
 * @author    Administrator
 * @created   16. Februar 2002
 * @version $Id$
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
            //System.out.println("hostManager: + " + hostName);
            if(!hostName.equals(hostName.toLowerCase()))
            {
                try
                {
                    throw new Exception();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
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

    public HostInfo addSynonym(String hostName, String synonym)
    {
        HostInfo info = getHostInfo(hostName);
        hosts.put(synonym, info);
        return info;
    }


}
