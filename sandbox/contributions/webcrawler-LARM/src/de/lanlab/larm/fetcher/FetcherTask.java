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
import de.lanlab.larm.threads.*;
import de.lanlab.larm.util.InputStreamObserver;
import de.lanlab.larm.util.ObservableInputStream;
import de.lanlab.larm.util.WebDocument;
import de.lanlab.larm.util.SimpleCharArrayReader;
import de.lanlab.larm.storage.DocumentStorage;
import de.lanlab.larm.storage.LinkStorage;

import de.lanlab.larm.util.State;
import de.lanlab.larm.util.SimpleLogger;
import de.lanlab.larm.net.HttpTimeoutFactory;
import HTTPClient.*;
import java.net.*;
import java.io.*;
import java.util.*;
import java.text.*;
import de.lanlab.larm.parser.Tokenizer;
import de.lanlab.larm.parser.LinkHandler;

/**
 * this class gets the documents from the web. It connects to the server given
 * by the IP address in the URLMessage, gets the document, and forwards it to
 * the storage. If it's an HTML document, it will be parsed and all links will
 * be put into the message handler again.
 *
 * @author    Clemens Marschner
 * @version $Id$
 */
public class FetcherTask
         implements InterruptableTask, LinkHandler, Serializable
{

    protected volatile boolean isInterrupted = false;

    /**
     * each task has its own number. the class variable counts up if an instance
     * of a fetcher task is created
     */
    static volatile int taskIdentity = 0;

    /**
     * the number of this object
     */
    int taskNr;

    /**
     * the BASE Href (defaults to contextUrl, may be changed with a <base> tag
     * only valid within a doTask call
     */
    private volatile URL base;

    /**
     * the URL of the docuzment
     * only valid within a doTask call
     */
    private volatile URL contextUrl;

    /**
     * the message handler the URL message comes from; same for all tasks
     */
    protected static volatile MessageHandler messageHandler;

    /**
     * actual number of bytes read
     * only valid within a doTask call
     */
    private volatile long bytesRead = 0;

    /**
     * the docStorage this task will put the document to
     */
    private static volatile DocumentStorage docStorage;

    /**
     * the docStorage this task will put the links to
     */
    private static volatile LinkStorage linkStorage;



    /**
     * task state IDs. comparisons will be done by their references, so always
     * use the IDs
     */
    public final static String FT_IDLE = "idle";
    public final static String FT_STARTED = "started";
    public final static String FT_OPENCONNECTION = "opening connection";
    public final static String FT_CONNECTING = "connecting";
    public final static String FT_GETTING = "getting";
    public final static String FT_READING = "reading";
    public final static String FT_SCANNING = "scanning";
    public final static String FT_STORING = "storing";
    public final static String FT_READY = "ready";
    public final static String FT_CLOSING = "closing";
    public final static String FT_EXCEPTION = "exception";
    public final static String FT_INTERRUPTED = "interrupted";

    private volatile State taskState = new State(FT_IDLE);

    /**
     * the URLs found will be stored and only added to the message handler in the very
     * end, to avoid too many synchronizations
     */
    private volatile LinkedList foundUrls;

    /**
     * the URL to be get
     */
    protected volatile URLMessage actURLMessage;

    /**
     * the document title, if present
     */
    private volatile String title;

    /**
     * headers for HTTPClient
     */
    private static volatile NVPair headers[] = new NVPair[1];

    static
    {
        headers[0] = new HTTPClient.NVPair("User-Agent", Constants.CRAWLER_AGENT);

    }


    /**
     * Gets a copy of the current taskState
     *
     * @return   The taskState value
     */
    public State getTaskState()
    {
        return taskState.cloneState();
    }


    /**
     * Constructor for the FetcherTask object
     *
     * @param urlMessage   Description of the Parameter
     */
    public FetcherTask(URLMessage urlMessage)
    {
        actURLMessage = urlMessage;
    }


    /**
     * Gets the uRLMessages attribute of the FetcherTask object
     *
     * @return   The uRLMessages value
     */
    public URLMessage getActURLMessage()
    {
        return this.actURLMessage;
    }


    /**
     * Sets the document docStorage
     *
     * @param docStorage  The new docStorage
     */
    public static void setDocStorage(DocumentStorage docStorage)
    {
        FetcherTask.docStorage = docStorage;
    }

    /**
     * Sets the document linkStorage
     *
     * @param linkStorage  The new linkStorage
     */
    public static void setLinkStorage(LinkStorage linkStorage)
    {
        FetcherTask.linkStorage = linkStorage;
    }


    /**
     * Sets the messageHandler
     *
     * @param messageHandler  The new messageHandler
     */
    public static void setMessageHandler(MessageHandler messageHandler)
    {
        FetcherTask.messageHandler = messageHandler;
    }


    /**
     * @return   the URL as a string
     */
    public String getInfo()
    {
        return actURLMessage.getURLString();
    }


    /**
     * Gets the uRL attribute of the FetcherTask object
     *
     * @return   The uRL value
     */
    public URL getURL()
    {
        return actURLMessage.getUrl();
    }

    SimpleLogger log;
    SimpleLogger errorLog;
    //private long startTime;

    /**
     * this will be called by the fetcher thread and will do all the work
     *
     * @TODO probably split this up into different processing steps
     * @param thread  Description of the Parameter
     */
    public void run(ServerThread thread)
    {

        taskState.setState(FT_STARTED); // state information is always set to make the thread monitor happy

        log = thread.getLog();
        HostManager hm = ((FetcherThread)thread).getHostManager();

        errorLog = thread.getErrorLog();

        // startTime = System.currentTimeMillis();
        int threadNr = ((FetcherThread) thread).getThreadNumber();

        log.log("start");
        base = contextUrl = actURLMessage.getUrl();
        String urlString = actURLMessage.getURLString();
        String host = contextUrl.getHost();
        int hostPos = urlString.indexOf(host);
        int hostLen = host.length();

        HostInfo hi = hm.getHostInfo(host); // get and create

        if(!hi.isHealthy())
        {
            // we make this check as late as possible to get the most current information
            log.log("Bad Host: " + contextUrl + "; returning");
            System.out.println("[" + threadNr + "] bad host: " + this.actURLMessage.getUrl());

            taskState.setState(FT_READY, null);
            return;
        }

        foundUrls = new java.util.LinkedList();

        HTTPConnection conn = null;

        title = "*untitled*";

        int size = 1;

        InputStream in = null;
        bytesRead = 0;


        try
        {

            URL ipURL = contextUrl;

            taskState.setState(FT_OPENCONNECTION, urlString);

            log.log("connecting to " + ipURL.getHost());
            taskState.setState(FT_CONNECTING, ipURL);
            conn = new HTTPConnection(host);

            conn.setDefaultTimeout(75000);
            // 75 s
            conn.setDefaultAllowUserInteraction(false);

            taskState.setState(this.FT_GETTING, ipURL);
            log.log("getting");

            HTTPResponse response = conn.Get(ipURL.getFile(), "", headers);
            response.setReadIncrement(2720);
            int statusCode = response.getStatusCode();
            byte[] fullBuffer = null;
            String contentType = "";
            int contentLength = 0;

            if (statusCode != 404 && statusCode != 403)
            {
                // read up to Constants.FETCHERTASK_MAXFILESIZE bytes into a byte array
                taskState.setState(FT_READING, ipURL);
                contentType = response.getHeader("Content-Type");
                String length = response.getHeader("Content-Length");
                if (length != null)
                {
                    contentLength = Integer.parseInt(length);
                }
                log.log("reading");

                fullBuffer = response.getData(Constants.FETCHERTASK_MAXFILESIZE); // max. 2 MB
                if (fullBuffer != null)
                {
                    contentLength = fullBuffer.length;
                    this.bytesRead += contentLength;
                }
            }
            //conn.stop();    // close connection. todo: Do some caching...


            /*
             *  conn.disconnect();
             */
            if (isInterrupted)
            {
                System.out.println("FetcherTask: interrupted while reading. File truncated");
                log.log("interrupted while reading. File truncated");
            }
            else
            {
                if (fullBuffer != null)
                {
                    taskState.setState(FT_SCANNING, ipURL);

                    log.log("read file (" + fullBuffer.length + " bytes). Now scanning.");

                    if (contentType.startsWith("text/html"))
                    {

                        // ouch. I haven't found a better solution yet. just slower ones.
                        char[] fullCharBuffer = new char[contentLength];
                        new InputStreamReader(new ByteArrayInputStream(fullBuffer)).read(fullCharBuffer);
                        Tokenizer tok = new Tokenizer();
                        tok.setLinkHandler(this);
                        tok.parse(new SimpleCharArrayReader(fullCharBuffer));
                    }
                    else
                    {
                        // System.out.println("Discovered unknown content type: " + contentType + " at " + urlString);
                        errorLog.log("[" + threadNr + "] Discovered unknown content type at " + urlString + ": " + contentType + ". just storing");
                    }
                    log.log("scanned");
                }
                taskState.setState(FT_STORING, ipURL);
                linkStorage.storeLinks(foundUrls);
                //messageHandler.putMessages(foundUrls);
                docStorage.store(new WebDocument(contextUrl, contentType, fullBuffer, statusCode, actURLMessage.getReferer(), contentLength, title));
                log.log("stored");
            }
        }
        catch (InterruptedIOException e)
        {
            // timeout while reading this file
            System.out.println("[" + threadNr + "] FetcherTask: Timeout while opening: " + this.actURLMessage.getUrl());
            errorLog.log("error: Timeout: " + this.actURLMessage.getUrl());
            hi.badRequest();
        }
        catch (FileNotFoundException e)
        {
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] FetcherTask: File not Found: " + this.actURLMessage.getUrl());
            errorLog.log("error: File not Found: " + this.actURLMessage.getUrl());
        }
        catch(NoRouteToHostException e)
        {
            // router is down or firewall prevents to connect
            hi.setReachable(false);
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());
        }
        catch(ConnectException e)
        {
            // no server is listening at this port
            hi.setReachable(false);
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());
        }
        catch (SocketException e)
        {
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "]: SocketException:" + e.getMessage());
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());

        }
        catch(UnknownHostException e)
        {
            // IP Address not to be determined
            hi.setReachable(false);
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());

        }
        catch (IOException e)
        {
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: IOException: " + e.getClass().getName() + ": " + e.getMessage());

        }
        catch (OutOfMemoryError ome)
        {
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] Task " + this.taskNr + " OutOfMemory after " + size + " bytes");
            errorLog.log("error: OutOfMemory after " + size + " bytes");
        }
        catch (Throwable e)
        {
            taskState.setState(FT_EXCEPTION);
            System.out.println("[" + threadNr + "] " + e.getMessage() + " type: " + e.getClass().getName());
            e.printStackTrace();
            System.out.println("[" + threadNr + "]: stopping");
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage() + "; stopping");

        }
        finally
        {

            if (isInterrupted)
            {
                System.out.println("Task was interrupted");
                log.log("interrupted");
                taskState.setState(FT_INTERRUPTED);
            }
        }
        if (isInterrupted)
        {
            System.out.println("Task: closed everything");
        }
        /*
         *  }
         */
        taskState.setState(FT_CLOSING);
        conn.stop();

        taskState.setState(FT_READY);
        foundUrls = null;
    }


    /**
     * the interrupt method. not in use since the change to HTTPClient
     * @TODO decide if we need this anymore
     */
    public void interrupt()
    {
        System.out.println("FetcherTask: interrupted!");
        this.isInterrupted = true;
        /*
         *  try
         *  {
         *  if (conn != null)
         *  {
         *  ((HttpURLConnection) conn).disconnect();
         *  System.out.println("FetcherTask: disconnected URL Connection");
         *  conn = null;
         *  }
         *  if (in != null)
         *  {
         *  in.close();
         *  / possibly hangs at close() .> KeepAliveStream.close() -> MeteredStream.skip()
         *  System.out.println("FetcherTask: Closed Input Stream");
         *  in = null;
         *  }
         *  }
         *  catch (IOException e)
         *  {
         *  System.out.println("IOException while interrupting: ");
         *  e.printStackTrace();
         *  }
         *  System.out.println("FetcherTask: Set all IOs to null");
         */
    }


    /**
     * this is called whenever a link was found in the current document,
     * Don't create too many objects here, as this will be called
     * millions of times
     *
     * @param link  Description of the Parameter
     */
    public void handleLink(String link, String anchor, boolean isFrame)
    {
        try
        {
            // cut out Ref part


            int refPart = link.indexOf("#");
            //System.out.println(link);
            if (refPart == 0)
            {
                return;
            }
            else if (refPart > 0)
            {
                link = link.substring(0, refPart);
            }

            URL url = null;
            if (link.startsWith("http:"))
            {
                // distinguish between absolute and relative URLs

                url = new URL(link);
            }
            else
            {
                // relative url
                url = new URL(base, link);
            }

            URLMessage urlMessage =  new URLMessage(url, contextUrl, isFrame, anchor);

            String urlString = urlMessage.getURLString();

            foundUrls.add(urlMessage);
            //messageHandler.putMessage(new actURLMessage(url)); // put them in the very end
        }
        catch (MalformedURLException e)
        {
            //log.log("malformed url: base:" + base + " -+- link:" + link);
            log.log("warning: " + e.getClass().getName() + ": " + e.getMessage());
        }
        catch (Exception e)
        {
            log.log("warning: " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
        }

    }


    /**
     * called when a BASE tag was found
     *
     * @param base  the HREF attribute
     */
    public void handleBase(String base)
    {
        try
        {
            this.base = new URL(base);
        }
        catch (MalformedURLException e)
        {
            log.log("warning: " + e.getClass().getName() + ": " + e.getMessage() + " while converting '" + base + "' to URL in document " + contextUrl);
        }
    }


    /**
     * called when a TITLE tag was found
     *
     * @param title  the string between &lt;title> and &gt;/title>
     */
    public void handleTitle(String title)
    {
        this.title = title;
    }



    /*
     *  public void notifyOpened(ObservableInputStream in, long timeElapsed)
     *  {
     *  }
     *  public void notifyClosed(ObservableInputStream in, long timeElapsed)
     *  {
     *  }
     *  public void notifyRead(ObservableInputStream in, long timeElapsed, int nrRead, int totalRead)
     *  {
     *  if(totalRead / ((double)timeElapsed) < 0.3) // weniger als 300 bytes/s
     *  {
     *  System.out.println("Task " + this.taskNr + " stalled at pos " + totalRead + " with " + totalRead / (timeElapsed / 1000.0) + " bytes/s");
     *  }
     *  }
     *  public void notifyFinished(ObservableInputStream in, long timeElapsed, int totalRead)
     *  {
     *  /System.out.println("Task " + this.taskNr + " finished (" + totalRead + " bytes in " + timeElapsed + " ms with " + totalRead / (timeElapsed / 1000.0) + " bytes/s)");
     *  }
     */
    public long getBytesRead()
    {
        return bytesRead;
    }


    /**
     * do nothing if a warning occurs within the html parser
     *
     * @param message                  Description of the Parameter
     * @param systemID                 Description of the Parameter
     * @param line                     Description of the Parameter
     * @param column                   Description of the Parameter
     * @exception java.lang.Exception  Description of the Exception
     */
    public void warning(String message, String systemID, int line, int column)
        throws java.lang.Exception { }


    /**
     * do nothing if a fatal error occurs...
     *
     * @param message        Description of the Parameter
     * @param systemID       Description of the Parameter
     * @param line           Description of the Parameter
     * @param column         Description of the Parameter
     * @exception Exception  Description of the Exception
     */
    public void fatal(String message, String systemID, int line, int column)
        throws Exception
    {
        System.out.println("fatal error: " + message);
        log.log("fatal error: " + message);
    }

}

