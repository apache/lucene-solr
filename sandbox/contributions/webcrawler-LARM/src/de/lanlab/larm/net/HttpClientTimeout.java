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

// whatever package you want
import sun.net.www.http.HttpClient;
import sun.net.www.MessageHeader;
import sun.net.ProgressEntry;

import java.net.*;
import java.io.*;


/**
 *  Description of the Class
 *
 *@author     cmarschn
 *@created    2. Mai 2001
 */
public class HttpClientTimeout extends HttpClient {
    private int timeout = -1;


    /**
     *  Constructor for the HttpClientTimeout object
     *
     *@param  url              Description of Parameter
     *@param  proxy            Description of Parameter
     *@param  proxyPort        Description of Parameter
     *@exception  IOException  Description of Exception
     */
    public HttpClientTimeout(URL url, String proxy, int proxyPort) throws IOException {
        super(url, proxy, proxyPort);
    }


    /**
     *  Constructor for the HttpClientTimeout object
     *
     *@param  url              Description of Parameter
     *@exception  IOException  Description of Exception
     */
    public HttpClientTimeout(URL url) throws IOException {
        super(url, null, -1);
    }


    /**
     *  Sets the Timeout attribute of the HttpClientTimeout object
     *
     *@param  i                    The new Timeout value
     *@exception  SocketException  Description of Exception
     */
    public void setTimeout(int i) throws SocketException {
        this.timeout = -1;
        serverSocket.setSoTimeout(i);
    }


    /**
     *  Gets the Socket attribute of the HttpClientTimeout object
     *
     *@return    The Socket value
     */
    public Socket getSocket() {
        return serverSocket;
    }


    /**
     *  Description of the Method
     *
     *@param  header                   Description of Parameter
     *@param  entry                    Description of Parameter
     *@return                          Description of the Returned Value
     *@exception  java.io.IOException  Description of Exception
     */
    public boolean parseHTTP(MessageHeader header, ProgressEntry entry) throws java.io.IOException {
        if (this.timeout != -1) {
            try {
                serverSocket.setSoTimeout(this.timeout);
            }
            catch (SocketException e) {
                throw new java.io.IOException("unable to set socket timeout!");
            }
        }
        return super.parseHTTP(header, entry);
    }


    /**
     *  Description of the Method
     *
     *@exception  IOException  Description of Exception
     */
    public void close() throws IOException {
        serverSocket.close();
    }


    /*
     * public void SetTimeout(int i) throws SocketException {
     * serverSocket.setSoTimeout(i);
     * }
     */
    /*
     * This class has no public constructor for HTTP.  This method is used to
     * get an HttpClient to the specifed URL.  If there's currently an
     * active HttpClient to that server/port, you'll get that one.
     *
     * no longer syncrhonized -- it slows things down too much
     * synchronize at a higher level
     */
    /**
     *  Gets the New attribute of the HttpClientTimeout class
     *
     *@param  url              Description of Parameter
     *@return                  The New value
     *@exception  IOException  Description of Exception
     */
    public static HttpClientTimeout getNew(URL url) throws IOException {
        /*
         * see if one's already around
         */
        HttpClientTimeout ret = (HttpClientTimeout) kac.get(url);
        if (ret == null) {
            ret = new HttpClientTimeout(url);
            // CTOR called openServer()
        }
        else {
            ret.url = url;
        }
        // don't know if we're keeping alive until we parse the headers
        // for now, keepingAlive is false
        return ret;
    }
}

