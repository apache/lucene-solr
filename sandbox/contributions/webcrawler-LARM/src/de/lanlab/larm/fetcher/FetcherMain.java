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
import de.lanlab.larm.util.*;
import de.lanlab.larm.storage.*;
import de.lanlab.larm.net.*;
import HTTPClient.*;
import org.apache.oro.text.regex.MalformedPatternException;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import javax.swing.UIManager;


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
     * the host resolver can change a host that occurs within a URL to a different
     * host, depending on the rules specified in a configuration file
     */
    protected HostResolver hostResolver;

    /**
     * this rather flaky filter just filters out some URLs, i.e. different views
     * of Apache the apache DirIndex module. Has to be made
     * configurable in near future
     */
    protected KnownPathsFilter knownPathsFilter;

    /**
     * the URL length filter filters URLs that are too long, i.e. because of errors
     * in the implementation of dynamic web sites
     */
    protected URLLengthFilter urlLengthFilter;


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
     * initializes all classes and registers anonymous adapter classes as
     * listeners for fetcher events.
     *
     * @param nrThreads  number of fetcher threads to be created
     */
    public FetcherMain(int nrThreads, String hostResolverFile) throws Exception
    {
        // to make things clear, this method is commented a bit better than
        // the rest of the program...

        // this is the main message queue. handlers are registered with
        // the queue, and whenever a message is put in it, the message is passed to the
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
        // it also saves all documents to page files.
        File logsDir = new File("logs");
        logsDir.mkdir();    // ensure log directory exists

        // in this experimental implementation, the crawler is pretty verbose
        // the SimpleLogger, however, is a FlyWeight logger which is buffered and
        // not thread safe by default
        SimpleLogger storeLog = new SimpleLogger("store", /* add date/time? */ false);
        SimpleLogger visitedLog = new SimpleLogger("URLVisitedFilter", /* add date/time? */ false);
        SimpleLogger scopeLog = new SimpleLogger("URLScopeFilter", /* add date/time? */ false);
        SimpleLogger pathsLog = new SimpleLogger("KnownPathsFilter", /* add date/time? */ false);
        SimpleLogger linksLog = new SimpleLogger("links", /* add date/time? */ false);
        SimpleLogger lengthLog = new SimpleLogger("length", /* add date/time? */ false);

        StoragePipeline storage = new StoragePipeline();


        // in the default configuration, the crawler will only save the document
        // information to store.log and the link information to links.log
        // The contents of the files are _not_ saved. If you set
        // "save in page files" to "true", they will be saved in "page files",
        // binary files each containing a set of documents. Here, the
        // maximum file size is ~50 MB (crawled files won't be split up into different
        // files). The logs/store.log file contains pointers to these files: a page
        // file number, the offset within that file, and the document's length

        // FIXME: default constructor for all storages + bean access methods
        storage.addDocStorage(new LogStorage(storeLog, /* save in page files? */ false,
                                             /* page file prefix */ "logs/pagefile"));
        storage.addLinkStorage(new LinkLogStorage(linksLog));
        storage.addLinkStorage(messageHandler);
        /*
        // experimental Lucene storage. will slow the crawler down *a lot*
        LuceneStorage luceneStorage = new LuceneStorage();
        luceneStorage.setAnalyzer(new org.apache.lucene.analysis.de.GermanAnalyzer());
        luceneStorage.setCreate(true);
	// FIXME: index name and path need to be configurable
        luceneStorage.setIndexName("luceneIndex");
        // the field names come from URLMessage.java and WebDocument.java. See
        // LuceneStorage source for details
        luceneStorage.setFieldInfo("url", LuceneStorage.INDEX | LuceneStorage.STORE);
        luceneStorage.setFieldInfo("content", LuceneStorage.INDEX | LuceneStorage.STORE | LuceneStorage.TOKEN);
        storage.addDocStorage(luceneStorage);
        */

        storage.open();

        //storage.addStorage(new JMSStorage(...));

        // create the filters and add them to the message queue
        urlScopeFilter = new URLScopeFilter(scopeLog);

        // dnsResolver = new DNSResolver();
        hostManager = new HostManager(1000);
        hostResolver = new HostResolver();
        hostResolver.initFromFile(hostResolverFile);
        hostManager.setHostResolver(hostResolver);

//        hostManager.addSynonym("www.fachsprachen.uni-muenchen.de", "www.fremdsprachen.uni-muenchen.de");
//        hostManager.addSynonym("www.uni-muenchen.de", "www.lmu.de");
//        hostManager.addSynonym("www.uni-muenchen.de", "uni-muenchen.de");
//        hostManager.addSynonym("webinfo.uni-muenchen.de", "www.webinfo.uni-muenchen.de");
//        hostManager.addSynonym("webinfo.uni-muenchen.de", "webinfo.campus.lmu.de");
//        hostManager.addSynonym("www.s-a.uni-muenchen.de", "s-a.uni-muenchen.de");

        reFilter = new RobotExclusionFilter(hostManager);

        fetcher = new Fetcher(nrThreads, storage, storage, hostManager);

        // prevent message box popups
        HTTPConnection.setDefaultAllowUserInteraction(false);

        // prevent GZipped files from being decoded
        HTTPConnection.removeDefaultModule(HTTPClient.ContentEncodingModule.class);

        urlVisitedFilter = new URLVisitedFilter(visitedLog, 100000);

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

         //uncomment this to enable HTTPClient logging
        /*
        try
        {
            HTTPClient.Log.setLogWriter(new java.io.OutputStreamWriter(System.out) //new java.io.FileWriter("logs/HttpClient.log")
            ,false);
            HTTPClient.Log.setLogging(HTTPClient.Log.ALL, true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        */

    }


    /**
     * Sets the RexString attribute of <code>UrlScopeFilter</code>.
     *
     * @param restrictTo the new RexString value
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
     //   throws java.net.MalformedURLException
    {
        try
        {
            messageHandler.putMessage(new URLMessage(url, null, isFrame == true ? URLMessage.LINKTYPE_FRAME : URLMessage.LINKTYPE_ANCHOR, null, this.hostResolver));
        }
        catch (Exception e)
        {
	    // FIXME: replace with logging
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
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
     * The main program.
     *
     * @param args  The command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        int nrThreads = 10;

        ArrayList startURLs = new ArrayList();
        String restrictTo = ".*";
        boolean gui = false;
        boolean showInfo = false;
        String hostResolverFile = "";
        System.out.println("LARM - LANLab Retrieval Machine - Fetcher - V 1.00 - B.20020914");
	// FIXME: consider using Jakarta Commons' CLI package for command line parameters

        for (int i = 0; i < args.length; i++)
        {
            if (args[i].equals("-start"))
            {
                i++;
                String arg = args[i];
                if(arg.startsWith("@"))
                {
                    // input is a file with one URL per line
                    String fileName = arg.substring(1);
                    System.out.println("reading URL file " + fileName);
                    try
                    {
                        BufferedReader r = new BufferedReader(new FileReader(fileName));
                        String line;
                        int count=0;
                        while((line = r.readLine()) != null)
                        {
                            try
                            {
                                startURLs.add(new URL(line));
                                count++;
                            }
                            catch (MalformedURLException e)
                            {
                                System.out.println("Malformed URL '" + line + "' in line " + (count+1) + " of file " + fileName);

                            }
                        }
                        r.close();
                        System.out.println("added " + count + " URLs from " + fileName);
                    }
                    catch(IOException e)
                    {
                        System.out.println("Couldn't read '" + fileName + "': " + e);
                    }
                }
                else
                {
                    System.out.println("got URL " + arg);
                    try
                    {
                        startURLs.add(new URL(arg));
                        System.out.println("Start-URL added: " + arg);
                    }
                    catch (MalformedURLException e)
                    {
                        System.out.println("Malformed URL '" + arg + "'");

                    }
                }
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
            else if (args[i].equals("-hostresolver"))
            {
                i++;
                hostResolverFile = args[i];
                System.out.println("reading host resolver props from  '" + hostResolverFile + "'");

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

        FetcherMain f = new FetcherMain(nrThreads, hostResolverFile);
        if (showInfo || "".equals(hostResolverFile) || (startURLs.isEmpty() && gui == false))
        {
            System.out.println("The LARM crawler\n" +
                               "\n" +
                               "The LARM crawler is a fast parallel crawler, currently designed for\n" +
                               "large intranets (up to a couple hundred hosts with some hundred thousand\n" +
                               "documents). It is currently restricted by a relatively high memory overhead\n" +
                               "per crawled host, and by a HashMap of already crawled URLs which is also held\n" +
                               "in memory.\n" +
                               "\n" +
                               "Usage:   FetcherMain <-start <URL>|@<filename>>+ -restrictto <RegEx>\n" +
                               "                    [-threads <nr=10>] [-hostresolver <filename>]\n" +
                               "\n" +
                               "Commands:\n" +
                               "         -start specify one or more URLs to start with. You can as well specify a file" +
                               "                that contains URLs, one each line\n" +
                               "         -restrictto a Perl 5 regular expression each URL must match. It is run against the\n" +
                               "                     _complete_ URL, including the http:// part\n" +
                               "         -threads  the number of crawling threads. defaults to 10\n" +
                               "         -hostresolver specify a file that contains rules for changing the host part of \n" +
                               "                       a URL during the normalization process (experimental).\n" +
                               "Caution: The <RegEx> is applied to the _normalized_ form of a URL.\n" +
                               "         See URLNormalizer for details\n" +
                               "Example:\n" +
                               "    -start @urls1.txt -start @urls2.txt -start http://localhost/ " +
                               "    -restrictto http://[^/]*\\.localhost/.* -threads 25\n" +
                               "\n" +
                               "The host resolver file may contain the following commands: \n" +
                               "  startsWith(part1) = part2\n" +
                               "      if host starts with part1, this part will be replaced by part2\n" +
                               "   endsWith(part1) = part2\n" +
                               "       if host ends with part1, this part will be replaced by part2. This is done after\n" +
                               "       startsWith was processed\n" +
                               "   synonym(host1) = host2\n" +
                               "       the keywords startsWith, endsWith and synonym are case sensitive\n" +
                               "       host1 will be replaced with host2. this is done _after_ startsWith and endsWith was \n" +
                               "       processed. Due to a bug in BeanUtils, dots are not allowed in the keys (in parentheses)\n" +
                               "       and have to be escaped with commas. To simplify, commas are also replaced in property \n" +
                               "       values. So just use commas instead of dots. The resulting host names are only used for \n" +
                               "       comparisons and do not have to be existing URLs (although the syntax has to be valid).\n" +
                               "       However, the names will often be passed to java.net.URL which will try to make a DNS name\n" +
                               "       resolution, which will time out if the server can't be found. \n" +
                               "   Example:" +
                               "     synonym(www1,host,com) = host,com\n" +
                               "     startsWith(www,) = ,\n" +
                               "     endsWith(host1,com) = host,com\n" +
                               "The crawler will show a status message every 5 seconds, which is printed by ThreadMonitor.java\n" +
                               "It will stop after the ThreadMonitor found the message queue and the crawling threads to be idle a \n" +
                               "couple of times.\n" +
                               "The crawled data will be saved within a logs/ directory. A cachingqueue/ directory is used for\n" +
                               "temporary queues.\n" +
                               "Note that this implementation is experimental, and that the command line options cover only a part \n" +
                               "of the parameters. Much of the configuration can only be done by modifying FetcherMain.java\n");
            System.exit(0);
        }
        try
        {
            f.setRexString(restrictTo);

            if (gui)
            {
                // f.initGui(f, startURL);
                // the GUI is not longer supported
            }
            else
            {
                f.startMonitor();
                for(Iterator it = startURLs.iterator(); it.hasNext(); )
                {
                    f.putURL((URL)it.next(), false);
                }
            }
        }
        catch (MalformedPatternException e)
        {
            System.out.println("Wrong RegEx syntax. Must be a valid PERL RE");
        }
    }
}
