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

import java.net.*;
import java.io.*;
import sun.net.www.http.HttpClient;

/**
 *  Description of the Class
 *
 *@author     cmarschn
 *@created    2. Mai 2001
 */
public class HttpURLConnectionTimeout extends sun.net.www.protocol.http.HttpURLConnection {
    int fiTimeoutVal;
    HttpTimeoutHandler fHandler;
    HttpClientTimeout fClient;


    /**
     *  Constructor for the HttpURLConnectionTimeout object
     *
     *@param  u                Description of Parameter
     *@param  handler          Description of Parameter
     *@param  iTimeout         Description of Parameter
     *@exception  IOException  Description of Exception
     */
    public HttpURLConnectionTimeout(URL u, HttpTimeoutHandler handler, int iTimeout) throws IOException {
        super(u, handler);
        fHandler = handler;
        fiTimeoutVal = iTimeout;
    }


    /**
     *  Constructor for the HttpURLConnectionTimeout object
     *
     *@param  u                Description of Parameter
     *@param  host             Description of Parameter
     *@param  port             Description of Parameter
     *@exception  IOException  Description of Exception
     */
    public HttpURLConnectionTimeout(URL u, String host, int port) throws IOException {
        super(u, host, port);
    }


    /**
     *  Description of the Method
     *
     *@exception  IOException  Description of Exception
     */
    public void connect() throws IOException {
        if (connected) {
            return;
        }
        try {
            if ("http".equals(url.getProtocol())
            /*
             * && !failedOnce <- PRIVATE
             */
                    ) {
                // for safety's sake, as reported by KLGroup
                synchronized (url) {
                    http = HttpClientTimeout.getNew(url);
                }
                fClient = (HttpClientTimeout) http;
                ((HttpClientTimeout) http).setTimeout(fiTimeoutVal);
            }
            else {
                // make sure to construct new connection if first
                // attempt failed
                http = new HttpClientTimeout(url, fHandler.getProxy(), fHandler.getProxyPort());
            }
            ps = (PrintStream) http.getOutputStream();
        }
        catch (IOException e) {
            throw e;
        }
        // this was missing from the original version
        connected = true;
    }


    /**
     *  Create a new HttpClient object, bypassing the cache of HTTP client
     *  objects/connections.
     *
     *@param  url              the URL being accessed
     *@return                  The NewClient value
     *@exception  IOException  Description of Exception
     */
    protected HttpClient getNewClient(URL url)
             throws IOException {
        HttpClientTimeout client = new HttpClientTimeout(url, (String) null, -1);
        try {
            client.setTimeout(fiTimeoutVal);
        }
        catch (Exception e) {
            System.out.println("Unable to set timeout value");
        }
        return (HttpClient) client;
    }


    /**
     *  Gets the Socket attribute of the HttpURLConnectionTimeout object
     *
     *@return    The Socket value
     */
    Socket getSocket() {
        return fClient.getSocket();
    }


    /**
     *  Description of the Method
     *
     *@exception  Exception  Description of Exception
     */
    void close() throws Exception {
        fClient.close();
    }


    /**
     *  opens a stream allowing redirects only to the same host.
     *
     *@param  c                Description of Parameter
     *@return                  Description of the Returned Value
     *@exception  IOException  Description of Exception
     */
    public static InputStream openConnectionCheckRedirects(URLConnection c)
             throws IOException {
        boolean redir;
        int redirects = 0;
        InputStream in = null;

        do {
            if (c instanceof HttpURLConnectionTimeout) {
                ((HttpURLConnectionTimeout) c).setInstanceFollowRedirects(false);
            }

            // We want to open the input stream before
            // getting headers, because getHeaderField()
            // et al swallow IOExceptions.
            in = c.getInputStream();
            redir = false;

            if (c instanceof HttpURLConnectionTimeout) {
                HttpURLConnectionTimeout http = (HttpURLConnectionTimeout) c;
                int stat = http.getResponseCode();
                if (stat >= 300 && stat <= 305 &&
                        stat != HttpURLConnection.HTTP_NOT_MODIFIED) {
                    URL base = http.getURL();
                    String loc = http.getHeaderField("Location");
                    URL target = null;
                    if (loc != null) {
                        target = new URL(base, loc);
                    }
                    http.disconnect();
                    if (target == null
                             || !base.getProtocol().equals(target.getProtocol())
                             || base.getPort() != target.getPort()
                             || !HostsEquals(base, target)
                             || redirects >= 5) {
                        throw new SecurityException("illegal URL redirect");
                    }
                    redir = true;
                    c = target.openConnection();
                    redirects++;
                }
            }
        } while (redir);
        return in;
    }


    // Same as java.net.URL.hostsEqual

    /**
     *  Description of the Method
     *
     *@param  u1  Description of Parameter
     *@param  u2  Description of Parameter
     *@return     Description of the Returned Value
     */
    static boolean HostsEquals(URL u1, URL u2) {
        final String h1 = u1.getHost();
        final String h2 = u2.getHost();

        if (h1 == null) {
            return h2 == null;
        }
        else if (h2 == null) {
            return false;
        }
        else if (h1.equalsIgnoreCase(h2)) {
            return true;
        }
        // Have to resolve addresses before comparing, otherwise
        // names like tachyon and tachyon.eng would compare different
        final boolean result[] = {false};

        java.security.AccessController.doPrivileged(
            new java.security.PrivilegedAction() {
                /**
                 *  Main processing method for the HttpURLConnectionTimeout object
                 *
                 *@return    Description of the Returned Value
                 */
                public Object run() {
                    try {
                        InetAddress a1 = InetAddress.getByName(h1);
                        InetAddress a2 = InetAddress.getByName(h2);
                        result[0] = a1.equals(a2);
                    }
                    catch (UnknownHostException e) {
                    }
                    catch (SecurityException e) {
                    }
                    return null;
                }
            });
        return result[0];
    }
}
