/*
 *  ====================================================================
 *  The Apache Software License, Version 1.1
 *
 *  Copyright (c) 2001 The Apache Software Foundation.  All rights
 *  reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in
 *  the documentation and/or other materials provided with the
 *  distribution.
 *
 *  3. The end-user documentation included with the redistribution,
 *  if any, must include the following acknowledgment:
 *  "This product includes software developed by the
 *  Apache Software Foundation (http://www.apache.org/)."
 *  Alternately, this acknowledgment may appear in the software itself,
 *  if and wherever such third-party acknowledgments normally appear.
 *
 *  4. The names "Apache" and "Apache Software Foundation" and
 *  "Apache Lucene" must not be used to endorse or promote products
 *  derived from this software without prior written permission. For
 *  written permission, please contact apache@apache.org.
 *
 *  5. Products derived from this software may not be called "Apache",
 *  "Apache Lucene", nor may "Apache" appear in their name, without
 *  prior written permission of the Apache Software Foundation.
 *
 *  THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 *  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 *  ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 *  USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 *  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 *  OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 *  SUCH DAMAGE.
 *  ====================================================================
 *
 *  This software consists of voluntary contributions made by many
 *  individuals on behalf of the Apache Software Foundation.  For more
 *  information on the Apache Software Foundation, please see
 *  <http://www.apache.org/>.
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
import HTTPClient.*;
import java.net.*;
import java.io.*;
import java.util.*;
import java.text.*;
import de.lanlab.larm.parser.Tokenizer;
import de.lanlab.larm.parser.LinkHandler;
import de.lanlab.larm.net.*;

/**
 * this class gets the documents from the web. It connects to the server given
 * by the IP address in the URLMessage, gets the document, and forwards it to
 * the storage. If it's an HTML document, it will be parsed and all links will
 * be put into the message handler again. stores contents of the files in field
 * "contents"
 *
 * @author    Clemens Marschner
 * @created   28. Juni 2002
 * @version   $Id$
 */
public class FetcherTask
         implements InterruptableTask, LinkHandler, Serializable
{

    /**
     * Description of the Field
     */
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
     * the URL of the docuzment only valid within a doTask call
     */
    private volatile URL contextUrl;

    /**
     * the message handler the URL message comes from; same for all tasks
     */
    protected static volatile MessageHandler messageHandler;

    /**
     * actual number of bytes read only valid within a doTask call
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
    /**
     * Description of the Field
     */
    public final static String FT_STARTED = "started";
    /**
     * Description of the Field
     */
    public final static String FT_OPENCONNECTION = "opening connection";
    /**
     * Description of the Field
     */
    public final static String FT_CONNECTING = "connecting";
    /**
     * Description of the Field
     */
    public final static String FT_GETTING = "getting";
    /**
     * Description of the Field
     */
    public final static String FT_READING = "reading";
    /**
     * Description of the Field
     */
    public final static String FT_SCANNING = "scanning";
    /**
     * Description of the Field
     */
    public final static String FT_STORING = "storing";
    /**
     * Description of the Field
     */
    public final static String FT_READY = "ready";
    /**
     * Description of the Field
     */
    public final static String FT_CLOSING = "closing";
    /**
     * Description of the Field
     */
    public final static String FT_EXCEPTION = "exception";
    /**
     * Description of the Field
     */
    public final static String FT_INTERRUPTED = "interrupted";

    private volatile State taskState = new State(FT_IDLE);

    /**
     * the URLs found will be stored and only added to the message handler in
     * the very end, to avoid too many synchronizations
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
     * @param urlMessage  Description of the Parameter
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


    volatile SimpleLogger log;

    volatile SimpleLogger errorLog;

    volatile HostManager hostManager;
    volatile HostResolver hostResolver;

    //private long startTime;

    /**
     * this will be called by the fetcher thread and will do all the work
     *
     * @param thread  Description of the Parameter
     * @TODO          probably split this up into different processing steps
     */
    public void run(ServerThread thread)
    {


        taskState.setState(FT_STARTED);
        // state information is always set to make the thread monitor happy

        log = thread.getLog();
        hostManager = ((FetcherThread) thread).getHostManager();
        hostResolver = hostManager.getHostResolver();
        base = contextUrl = actURLMessage.getUrl();
        String urlString = actURLMessage.getURLString();
        String host = contextUrl.getHost().toLowerCase();
        HostInfo hi = hostManager.getHostInfo(host);
//        System.out.println("FetcherTask with " + urlString + " started");
        if(actURLMessage.linkType == URLMessage.LINKTYPE_REDIRECT)
        {
            taskState.setState(FT_READY, null);
            hi.releaseLock();
            return;     // we've already crawled that (see below)
        }

        NVPair[] headers = ((FetcherThread) thread).getDefaultHeaders();
        int numHeaders = ((FetcherThread) thread).getUsedDefaultHeaders();
        boolean isIncremental = false;
        if (actURLMessage instanceof WebDocument)
        {
            // this is an incremental crawl where we only have to check whether the doc crawled
            // is newer
            isIncremental = true;
            headers[numHeaders] = new NVPair("If-Modified-Since", HTTPClient.Util.httpDate(((WebDocument) actURLMessage).getLastModified()));
        }
        //HostManager hm = ((FetcherThread)thread).getHostManager();

        errorLog = thread.getErrorLog();

        // startTime = System.currentTimeMillis();
        int threadNr = ((FetcherThread) thread).getThreadNumber();

        log.log("start");
        int hostPos = urlString.indexOf(host);
        int hostLen = host.length();

        // get and create

        if (!hi.isHealthy())
        {
            // we make this check as late as possible to get the most current information
            log.log("Bad Host: " + contextUrl + "; returning");
//            System.out.println("[" + threadNr + "] bad host: " + this.actURLMessage.getUrl());

            taskState.setState(FT_READY, null);
            hi.releaseLock();
            return;
        }

        foundUrls = new java.util.LinkedList();

        HTTPConnection conn = null;

        title = "";

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
            Date date = null;

             if (isIncremental)
            {
                // experimental
                System.out.println("ftask: if modified since: " + HTTPClient.Util.httpDate(((WebDocument) actURLMessage).getLastModified()));
            }

            URL realURL;

            switch (statusCode)
            {
                case 404:                // file not found
                case 403:                    // access forbidden

                    // if this is an incremental crawl, remove the doc from the repository
                    if (isIncremental)
                    {
                        WebDocument d = (WebDocument) actURLMessage;
                        d.setResultCode(statusCode);
                        // the repository will remove the doc if this statuscode is matched
                        docStorage.store(d);
                    }
                    // otherwise, do nothing
                    // Todo: we could add an error marker to the referal link
                    break;
                case 304:
                    // not modified
                    System.out.println("ftask: -> not modified");
                    // "not modified since"
                    taskState.setState(FT_STORING, ipURL);
                    // let the repository take care of the links
                    // it will determine that this is the old document (because it already
                    // has a docId), and will put back the links associated with it
                    try
                    {
                        WebDocument doc = (WebDocument) this.actURLMessage;
                        doc.setModified(false);
                        docStorage.store(doc);
                        this.bytesRead += doc.getSize();
                    }
                    catch (ClassCastException e)
                    {
                        System.out.println("error while casting to WebDoc: " + actURLMessage.getInfo());
                    }
                    break;
                case 301:                // moved permanently
                case 302:                // moved temporarily
                case 303:                // see other
                case 307:                // temporary redirect
                    /*
                     *  this is a redirect. save it as a link and return.
                     *  note that we could read the doc from the open connection here, but this could mean
                     *  the filters were useless
                     */
                    realURL = response.getEffectiveURI().toURL();
                    foundUrls.add(new URLMessage(realURL, contextUrl, URLMessage.LINKTYPE_REDIRECT, "", hostResolver));
                    linkStorage.storeLinks(foundUrls);
                    break;
                default:
                    // this can be a 30x code that was resolved by the HTTPClient and is passed to us as 200
                    // we could turn this off and do it ourselves. But then we'd have to take care that
                    // we don't get into an endless redirection loop -> i.e. extend URLMessage by a counter
                    // at the moment we add the real URL to the message queue and mark it as a REDIRECT link
                    // that way it is added to the visited filter. Then we take care that we don't crawl it again

                    // the other possibility is that we receive a "Location:" header along with a 200 status code
                    // I have experienced that HTTPClient has an error with parsing this, so we do it ourselves
                    //String location = response.getHeader("Location");
                    realURL = response.getEffectiveURI().toURL();

                    /*if(location != null)
                    {
                        //System.out.println("interesting: location header with url " + location);
                        foundUrls.add(new URLMessage(new URL(location), contextUrl, URLMessage.LINKTYPE_REDIRECT, "", hostManager));
                        this.base = this.contextUrl = location;
                    }
                    else*/
                    if(!(realURL.equals(contextUrl)))
                    {
                        //System.out.println("interesting: redirect with url " + realURL + " -context: " + contextUrl);
                        foundUrls.add(new URLMessage(realURL, contextUrl, URLMessage.LINKTYPE_REDIRECT, "", hostResolver));
                        this.base = this.contextUrl = realURL;
                        //System.out.println(response);

                    }




                    if (isIncremental)
                    {
                        // experimental
                        System.out.println("ftask: -> was modified at " + response.getHeaderAsDate("Last-Modified"));
                    }
                    // read up to Constants.FETCHERTASK_MAXFILESIZE bytes into a byte array
                    taskState.setState(FT_READING, ipURL);
                    contentType = response.getHeader("Content-Type");
                    String length = response.getHeader("Content-Length");
                    date = response.getHeaderAsDate("Last-Modified");

                    if (length != null)
                    {
                        contentLength = Integer.parseInt(length);
                    }
                    log.log("reading");
                    realURL = response.getEffectiveURI().toURL();
                    if (contentType != null && contentType.startsWith("text/html"))
                    {
                        fullBuffer = response.getData(Constants.FETCHERTASK_MAXFILESIZE);
                        hi.releaseLock();
                        // max. 2 MB
                        if (fullBuffer != null)
                        {
                            contentLength = fullBuffer.length;
                            this.bytesRead += contentLength;
                        }

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

                                // convert the bytes to Java characters
                                // ouch. I haven't found a better solution yet. just slower ones.
                                // remember: for better runtime performance avoid decorators, since they
                                // multiply function calls
                                char[] fullCharBuffer = new char[contentLength];
                                new InputStreamReader(new ByteArrayInputStream(fullBuffer)).read(fullCharBuffer);
                                Tokenizer tok = new Tokenizer();
                                tok.setLinkHandler(this);
                                tok.parse(new SimpleCharArrayReader(fullCharBuffer));

                                taskState.setState(FT_STORING, ipURL);
                                linkStorage.storeLinks(foundUrls);
                                WebDocument d;
                                if (isIncremental)
                                {
                                    d = ((WebDocument) this.actURLMessage);
                                    d.setModified(true);
                                    // file is new or newer
                                    d.setUrl(contextUrl);
                                    d.setMimeType(contentType);
                                    d.setResultCode(statusCode);
                                    d.setSize(contentLength);
                                    d.setTitle(title);
                                    d.setLastModified(date);
                                }
                                else
                                {
                                    d = new WebDocument(contextUrl, contentType, statusCode, actURLMessage.getReferer(), contentLength, title, date, hostResolver);
                                }
                                d.addField("content", fullCharBuffer);
                                d.addField("contentBytes", fullBuffer);
                                docStorage.store(d);
                            }

                            log.log("scanned");
                        }

                        log.log("stored");
                    }
                    else
                    {
                        // System.out.println("Discovered unknown content type: " + contentType + " at " + urlString);
                        //errorLog.log("[" + threadNr + "] Discovered unknown content type at " + urlString + ": " + contentType + ". just storing");
                        taskState.setState(FT_STORING, ipURL);
                        linkStorage.storeLinks(foundUrls);
                        WebDocument d = new WebDocument(contextUrl, contentType, statusCode, actURLMessage.getReferer(),
                        /*
                         *  contentLength
                         */
                                0, title, date, hostResolver);
                        //d.addField("content", fullBuffer);
                        //d.addField("content", null);
                        docStorage.store(d);
                    }
                    break;
            }
            /*
             *  switch
             */
            //conn.stop();    // close connection. todo: Do some caching...

        }
        catch (InterruptedIOException e)
        {
            // timeout while reading this file
            //System.out.println("[" + threadNr + "] FetcherTask: Timeout while opening: " + this.actURLMessage.getUrl());
            errorLog.log("error: Timeout: " + this.actURLMessage.getUrl());
            hi.badRequest();
        }
        catch (FileNotFoundException e)
        {
            taskState.setState(FT_EXCEPTION);
            //System.out.println("[" + threadNr + "] FetcherTask: File not Found: " + this.actURLMessage.getUrl());
            errorLog.log("error: File not Found: " + this.actURLMessage.getUrl());
        }
        catch (NoRouteToHostException e)
        {
            // router is down or firewall prevents to connect
            hi.setReachable(false);
            taskState.setState(FT_EXCEPTION);
            //System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());
        }
        catch (ConnectException e)
        {
            // no server is listening at this port
            hi.setReachable(false);
            taskState.setState(FT_EXCEPTION);
            //System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());

        }
        catch (SocketException e)
        {
            taskState.setState(FT_EXCEPTION);
            //System.out.println("[" + threadNr + "]: SocketException:" + e.getMessage());
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());

        }
        catch (UnknownHostException e)
        {
            // IP Address not to be determined
            hi.setReachable(false);
            taskState.setState(FT_EXCEPTION);
            //System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
            // e.printStackTrace();
            errorLog.log("error: " + e.getClass().getName() + ": " + e.getMessage());

        }
        catch (IOException e)
        {
            taskState.setState(FT_EXCEPTION);
            //System.out.println("[" + threadNr + "] " + e.getClass().getName() + ": " + e.getMessage());
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
            hi.releaseLock();

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
     *
     * @TODO   decide if we need this anymore
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
     * this is called whenever a link was found in the current document, Don't
     * create too many objects here, as this will be called millions of times
     *
     * @param link     Description of the Parameter
     * @param anchor   Description of the Parameter
     * @param isFrame  Description of the Parameter
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
            if(url.getPath() == null || url.getPath().length() == 0)
            {
                url = new URL(url.getProtocol(), url.getHost(), url.getPort(), "/" + url.getFile());
            }
            URLMessage urlMessage = new URLMessage(url, contextUrl, isFrame ? URLMessage.LINKTYPE_FRAME : URLMessage.LINKTYPE_ANCHOR, anchor, hostResolver);

            //String urlString = urlMessage.getURLString();

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
    /**
     * Gets the bytesRead attribute of the FetcherTask object
     *
     * @return   The bytesRead value
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

