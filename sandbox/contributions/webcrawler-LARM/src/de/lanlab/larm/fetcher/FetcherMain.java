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

import de.lanlab.larm.threads.ThreadPoolObserver;
import de.lanlab.larm.threads.ThreadPool;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import de.lanlab.larm.gui.*;
import de.lanlab.larm.util.*;
import de.lanlab.larm.storage.*;
import javax.swing.UIManager;
import HTTPClient.*;
import org.apache.oro.text.regex.MalformedPatternException;


/**
 * ENTRY POINT: this class contains the main()-method of the application, does
 * all the initializing and optionally connects the fetcher with the GUI.
 *
 * @author    Clemens Marschner
 * @created   December 16, 2000
 * @version $Id$
 */
public class FetcherMain
{

    /**
     * the main message pipeline
     */
    protected MessageHandler messageHandler;

    /**
     * this filter records all incoming URLs and filters everything it already
     * knows
     */
    protected URLVisitedFilter urlVisitedFilter;

    /**
     * the scope filter filters URLs that fall out of the scope given by the
     * regular expression
     */
    protected URLScopeFilter urlScopeFilter;

    /*
     * The DNS resolver was supposed to hold the host addresses for all hosts
     * this is done by URL itself today
     *
     * protected DNSResolver dnsResolver;
     */

    /**
     * the robot exclusion filter looks if a robots.txt is present on a host
     * before it is first accessed
     */
    protected RobotExclusionFilter reFilter;

    /**
     * the host manager keeps track of all hosts and is used by the filters.
     */
    protected HostManager hostManager;

    /**
     * this rather flaky filter just filters out some URLs, i.e. different views
     * of Apache the apache DirIndex module. Has to be made
     * configurable in near future
     */
    protected KnownPathsFilter knownPathsFilter;

    /**
     * this is the main document fetcher. It contains a thread pool that fetches the
     * documents and stores them
     */
    protected Fetcher fetcher;


    /**
     * the thread monitor once was only a monitoring tool, but now has become a
     * vital part of the system that computes statistics and
     * flushes the log file buffers
     */

    protected ThreadMonitor monitor;

    /**
     * the storage is a central class that puts all fetched documents somewhere.
     * Several differnt implementations exist.
     */
    protected DocumentStorage storage;

    /**
     * the URL length filter filters URLs that are too long, i.e. because of errors
     * in the implementation of dynamic web sites
     */
    protected URLLengthFilter urlLengthFilter;

    /**
     * initializes all classes and registers anonymous adapter classes as
     * listeners for fetcher events.
     *
     * @param nrThreads  number of fetcher threads to be created
     */
    public FetcherMain(int nrThreads)
    {
        // to make things clear, this method is commented a bit better than
        // the rest of the program...

        // this is the main message queue. handlers are registered with
        // the queue, and whenever a message is put in it, they are passed to the
        // filters in a "chain of responibility" manner. Every listener can decide
        // to throw the message away
        messageHandler = new MessageHandler();

        // the storage is the class which saves a WebDocument somewhere, no
        // matter how it does it, whether it's in a file, in a database or
        // whatever


        // example for the (very slow) SQL Server storage:
        // this.storage = new SQLServerStorage("sun.jdbc.odbc.JdbcOdbcDriver","jdbc:odbc:search","sa","...",nrThreads);

        // the LogStorage used here does extensive logging. It logs all links and
        // document information.
        // it also saves all documents to page files. Probably this single storage
        // could also be replaced by a pipeline; or even incorporated into the
        // existing message pipeline
        SimpleLogger storeLog = new SimpleLogger("store", false);
        SimpleLogger linksLog = new SimpleLogger("links", false);


        StoragePipeline storage = new StoragePipeline();
        storage.addDocStorage(new LogStorage(storeLog, /* save in page files? */ false, /* logfile prefix */ "logs/pagefile"));
        storage.addLinkStorage(new LinkLogStorage(linksLog));
        storage.addLinkStorage(messageHandler);
        //storage.addStorage(new LuceneStorage(...));
        //storage.addStorage(new JMSStorage(...));

        // a third example would be the NullStorage, which converts the documents into
        // heat, which evaporates above the processor
        // NullStorage();

        // create the filters and add them to the message queue
        urlScopeFilter = new URLScopeFilter();

        urlVisitedFilter = new URLVisitedFilter(100000);

        // dnsResolver = new DNSResolver();
        hostManager = new HostManager(1000);

        reFilter = new RobotExclusionFilter(hostManager);

        fetcher = new Fetcher(nrThreads, storage, storage, hostManager);

        knownPathsFilter = new KnownPathsFilter();

        urlLengthFilter = new URLLengthFilter(255);

        // prevent message box popups
        HTTPConnection.setDefaultAllowUserInteraction(false);

        // prevent GZipped files from being decoded
        HTTPConnection.removeDefaultModule(HTTPClient.ContentEncodingModule.class);



        // initialize the threads
        fetcher.init();

        // the thread monitor watches the thread pool.

        monitor = new ThreadMonitor(urlLengthFilter,
                urlVisitedFilter,
                urlScopeFilter,
                /*dnsResolver,*/
                reFilter,
                messageHandler,
                fetcher.getThreadPool(),
                hostManager,
                5000        // wake up every 5 seconds
                );


        // add all filters to the handler.
        messageHandler.addListener(urlLengthFilter);
        messageHandler.addListener(urlScopeFilter);
        messageHandler.addListener(reFilter);
        messageHandler.addListener(urlVisitedFilter);
        messageHandler.addListener(knownPathsFilter);
        messageHandler.addListener(fetcher);

        /* uncomment this to enable HTTPClient logging
        try
        {
            HTTPClient.Log.setLogWriter(new java.io.FileWriter("logs/HttpClient.log"),false);
            HTTPClient.Log.setLogging(HTTPClient.Log.ALL, true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        */
    }


    /**
     * Sets the RexString attribute of the FetcherMain object
     *
     * @param restrictTo                          The new RexString value
     */
    public void setRexString(String restrictTo) throws MalformedPatternException
    {
        urlScopeFilter.setRexString(restrictTo);
    }


    /**
     * Description of the Method
     *
     * @param url                                 Description of Parameter
     * @param isFrame                             Description of the Parameter
     * @exception java.net.MalformedURLException  Description of Exception
     */
    public void putURL(URL url, boolean isFrame)
        throws java.net.MalformedURLException
    {
        try
        {
            messageHandler.putMessage(new URLMessage(url, null, isFrame, null));
        }
        catch (Exception e)
        {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
        //System.out.println("URLs geschrieben");
    }


    /**
     * Description of the Method
     */
    public void startMonitor()
    {
        monitor.start();
    }



    /*
     * the GUI is not working at this time. It was used in the very beginning, but
     * synchronous updates turned out to slow down the program a lot, even if the
     * GUI would be turned off. Thus, a lot
     * of Observer messages where removed later. Nontheless, it's quite cool to see
     * it working...
     *
     * @param f         Description of Parameter
     * @param startURL  Description of Parameter
     */

     /*
    public void initGui(FetcherMain f, String startURL)
    {
        // if we're on a windows platform, make it look a bit more convenient
        try
        {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        }
        catch (Exception e)
        {
            // dann halt nicht...
        }
        System.out.println("Init FetcherFrame");

        FetcherSummaryFrame fetcherFrame;
        fetcherFrame = new FetcherSummaryFrame();
        fetcherFrame.setSize(640, 450);
        fetcherFrame.setVisible(true);
        FetcherGUIController guiController = new FetcherGUIController(f, fetcherFrame, startURL);
    }
        */


    /**
     * The main program. parsed
     *
     * @param args  The command line arguments
     */
    public static void main(String[] args)
    {
        int nrThreads = 10;

        String startURL = "";
        String restrictTo = "http://141.84.120.82/ll/cmarschn/.*";
        boolean gui = false;
        boolean showInfo = false;
        System.out.println("LARM - LANLab Retrieval Machine - Fetcher - V 1.00 - (C) LANLab 2000-02");
        for (int i = 0; i < args.length; i++)
        {
            if (args[i].equals("-start"))
            {
                i++;
                startURL = args[i];
                System.out.println("Start-URL set to: " + startURL);
            }
            else if (args[i].equals("-restrictto"))
            {
                i++;
                restrictTo = args[i];
                System.out.println("Restricting URLs to " + restrictTo);
            }
            else if (args[i].equals("-threads"))
            {
                i++;
                nrThreads = Integer.parseInt(args[i]);
                System.out.println("Threads set to " + nrThreads);
            }
            else if (args[i].equals("-gui"))
            {
                gui = true;
            }
            else if (args[i].equals("-?"))
            {
                showInfo = true;
            }
            else
            {
                System.out.println("Unknown option: " + args[i] + "; use -? to get syntax");
                System.exit(0);
            }
        }

        //URL.setURLStreamHandlerFactory(new HttpTimeoutFactory(500));
        // replaced by HTTPClient

        FetcherMain f = new FetcherMain(nrThreads);
        if (showInfo || (startURL.equals("") && gui == false))
        {
            System.out.println("Usage: FetcherMain -start <URL> -restrictto <RegEx> [-threads <nr=10>]"); // [-gui]
            System.exit(0);
        }
        try
        {
            f.setRexString(restrictTo);

            if (gui)
            {
                // f.initGui(f, startURL);
            }
            else
            {
                try
                {
                    f.startMonitor();
                    f.putURL(new URL(startURL), false);
                }
                catch (MalformedURLException e)
                {
                    System.out.println("Malformed URL");

                }
            }
        }
        catch (MalformedPatternException e)
        {
            System.out.println("Wrong RegEx syntax. Must be a valid PERL RE");
        }
    }
}
