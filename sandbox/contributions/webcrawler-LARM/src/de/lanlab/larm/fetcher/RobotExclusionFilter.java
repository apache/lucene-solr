/**
 * Title: LARM Lanlab Retrieval Machine<p>
 *
 * Description: <p>
 *
 * Copyright: Copyright (c)<p>
 *
 * Company: <p>
 *
 *
 *
 * @author    Clemens Marschner
 * @version   1.0
 */
package de.lanlab.larm.fetcher;

import de.lanlab.larm.util.SimpleObservable;
import de.lanlab.larm.util.State;
import java.util.*;
import java.net.*;
import java.io.*;
import org.apache.oro.text.perl.Perl5Util;
import de.lanlab.larm.util.*;
import de.lanlab.larm.threads.*;
import HTTPClient.*;

/**
 * this factory simply creates fetcher threads. It's gonna be passed to the
 * ThreadPool because the pool is creating the threads on its own
 *
 * @author    Administrator
 * @created   17. Februar 2002
 */
class REFThreadFactory extends ThreadFactory
{

    ThreadGroup threadGroup = new ThreadGroup("RobotExclusionFilter");


    /**
     * Description of the Method
     *
     * @param count  Description of the Parameter
     * @return       Description of the Return Value
     */
    public ServerThread createServerThread(int count)
    {
        ServerThread newThread = new ServerThread(count, "REF-" + count, threadGroup);
        newThread.setPriority(4);
        return newThread;
    }
}

/**
 * the RE filter obeys the robot exclusion standard. If a new host name is supposed
 * to be accessed, it first loads a "/robots.txt" on the given server and records the
 * disallows stated in that file.
 * The REFilter has a thread pool on its own to prevent the message handler from being
 * clogged up if the server doesn't respond. Incoming messages are queued while the
 * robots.txt is loaded.
 * The information is stored in HostInfo records of the host manager class
 *
 * @author    Clemens Marschner
 * @created   17. Februar 2002
 */
public class RobotExclusionFilter extends Filter implements MessageListener
{


    protected HostManager hostManager;

    protected SimpleLogger log;


    /**
     * Constructor for the RobotExclusionFilter object
     *
     * @param hm  Description of the Parameter
     */
    public RobotExclusionFilter(HostManager hm)
    {
        log = new SimpleLogger("RobotExclusionFilter");
        hostManager = hm;
        rePool = new ThreadPool(2, new REFThreadFactory());
        rePool.init();
        log.setFlushAtOnce(true);
        log.log("refilter: initialized");
    }


    /**
     * called by the message handler
     */
    public void notifyAddedToMessageHandler(MessageHandler handler)
    {
        this.messageHandler = handler;
    }


    MessageHandler messageHandler = null;
    ThreadPool rePool;


    /**
     * method that handles each URL request<p>
     *
     * This method will get the robots.txt file the first time a server is
     * requested. See the description above.
     *
     * @param message
     *      the (URL)Message
     * @return
     *      the original message or NULL if this host had a disallow on that URL
     * @link{http://info.webcrawler.com/mak/projects/robots/norobots.html})
     */

    public Message handleRequest(Message message)
    {
        //log.logThreadSafe("handleRequest: got message: " + message);
        try
        {
            // assert message instanceof URLMessage;
            URLMessage urlMsg = ((URLMessage) message);
            URL url = urlMsg.getUrl();
            //assert url != null;
            HostInfo h = hostManager.getHostInfo(url.getHost());
            if (!h.isRobotTxtChecked() && !h.isLoadingRobotsTxt())
            {
                log.logThreadSafe("handleRequest: starting to get robots.txt");
                // probably this results in Race Conditions here

                rePool.doTask(new RobotExclusionTask(h), new Integer(h.id));
                h.setLoadingRobotsTxt(true);
            }

            synchronized (h)
            {
                // isLoading...() and queuedRequest.insert() must be atomic
                if (h.isLoadingRobotsTxt())
                {

                    //log.logThreadSafe("handleRequest: other thread is loading");
                    // assert h.queuedRequests != null
                    h.queuedRequests.insert(message);
                    // not thread safe
                    log.logThreadSafe("handleRequest: queued file " + url);
                    return null;
                }
            }

            //log.logThreadSafe("handleRequest: no thread is loading; robots.txt loaded");
            //log.logThreadSafe("handleRequest: checking if allowed");
            String path = url.getPath();
            if (path == null || path.equals(""))
            {
                path = "/";
            }

            if (h.isAllowed(path))
            {
                // log.logThreadSafe("handleRequest: file " + urlMsg.getURLString() + " ok");
                return message;
            }
            log.logThreadSafe("handleRequest: file " + urlMsg.getURLString() + " filtered");
            this.filtered++;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }


    private static volatile NVPair headers[] = new NVPair[1];

    static
    {
        headers[0] = new HTTPClient.NVPair("User-Agent", Constants.CRAWLER_AGENT);

    }


    /**
     * the task that actually loads and parses the robots.txt files
     *
     * @author    Clemens Marschner
     * @created   17. Februar 2002
     */
    class RobotExclusionTask implements InterruptableTask
    {
        HostInfo hostInfo;



        /**
         * Constructor for the RobotExclusionTask object
         *
         * @param hostInfo  Description of the Parameter
         */
        public RobotExclusionTask(HostInfo hostInfo)
        {
            this.hostInfo = hostInfo;
        }


        /**
         * dummy
         *
         * @return   The info value
         */
        public String getInfo()
        {
            return "";
        }


        /**
         * not used
         */
        public void interrupt() { }


        /**
         * gets a robots.txt file and adds the information to the hostInfo
         * structure
         *
         * @param thread  the server thread (passed by the thread pool)
         */
        public void run(ServerThread thread)
        {
            // assert hostInfo != null;
            String threadName = Thread.currentThread().getName();

            log.logThreadSafe("task " + threadName + ": starting to load " + hostInfo.hostName);
            //hostInfo.setLoadingRobotsTxt(true);
            String[] disallows = null;
            boolean errorOccured = false;
            try
            {
                log.logThreadSafe("task " + threadName + ": getting connection");
                HTTPConnection conn = new HTTPConnection(hostInfo.hostName);
                conn.setTimeout(30000);
                // wait at most 20 secs

                HTTPResponse res = conn.Get("/robots.txt", (String) null, headers);
                log.logThreadSafe("task " + threadName + ": got connection.");
                if (res.getStatusCode() != 200)
                {
                    errorOccured = true;
                }
                else
                {

                    log.logThreadSafe("task " + threadName + ": reading");
                    byte[] file = res.getData(40000);
                    // max. 40 kb
                    log.logThreadSafe("task " + threadName + ": reading done. parsing");
                    disallows = parse(new BufferedReader(new InputStreamReader(new ByteArrayInputStream(file))));
                    log.logThreadSafe("task " + threadName + ": parsing done. found " + disallows.length + " disallows");
                    // assert disallows != null
                    // HostInfo hostInfo = hostManager.getHostInfo(this.hostName);
                    // assert hostInfo != null
                    log.logThreadSafe("task " + threadName + ": setting disallows");
                }
            }
            catch (java.net.UnknownHostException e)
            {
                hostInfo.setReachable(false);
                log.logThreadSafe("task " + threadName + ": unknown host. setting to unreachable");
                errorOccured = true;
            }
            catch (java.net.NoRouteToHostException e)
            {
                hostInfo.setReachable(false);
                log.logThreadSafe("task " + threadName + ": no route to. setting to unreachable");
                errorOccured = true;
            }
            catch (java.net.ConnectException e)
            {
                hostInfo.setReachable(false);
                log.logThreadSafe("task " + threadName + ": connect exception. setting to unreachable");
                errorOccured = true;
            }
            catch (java.io.InterruptedIOException e)
            {
                // time out. fatal in this case
                hostInfo.setReachable(false);
                log.logThreadSafe("task " + threadName + ": time out. setting to unreachable");
                errorOccured = true;
            }

            catch (Throwable e)
            {
                errorOccured = true;
                log.log("task " + threadName + ": unknown exception: " + e.getClass().getName() + ": " + e.getMessage() + ". continuing");
                log.log(e);

            }
            finally
            {
                if (errorOccured)
                {
                    synchronized (hostInfo)
                    {
                        hostInfo.setRobotsChecked(true, null);
                        // crawl everything
                        hostInfo.setLoadingRobotsTxt(false);
                        log.logThreadSafe("task " + threadName + ": error occured");
                        log.logThreadSafe("task " + threadName + ": now put " + hostInfo.queuedRequests.size() + " queueud requests back");
                        hostInfo.isLoadingRobotsTxt = false;
                        putBackURLs();
                    }
                }
                else
                {
                    synchronized (hostInfo)
                    {
                        hostInfo.setRobotsChecked(true, disallows);
                        log.logThreadSafe("task " + threadName + ": done");
                        log.logThreadSafe("task " + threadName + ": now put " + hostInfo.queuedRequests.size() + " queueud requests back");
                        hostInfo.isLoadingRobotsTxt = false;
                        putBackURLs();
                    }
                }
            }
        }


        /**
         * put back queued URLs
         */
        private void putBackURLs()
        {
            while (hostInfo.queuedRequests.size() > 0)
            {
                messageHandler.putMessage((Message) hostInfo.queuedRequests.remove());
            }
            log.logThreadSafe("task " + Thread.currentThread().getName() + ": finished");
            hostInfo.queuedRequests = null;
        }


        /**
         * this parses the robots.txt file. It was taken from the PERL implementation
         * Since this is only rarely called, it's not optimized for speed
         *
         * @param r                the robots.txt file
         * @return                 the disallows
         * @exception IOException  any IOException
         */
        public String[] parse(BufferedReader r)
            throws IOException
        {
            // taken from Perl
            Perl5Util p = new Perl5Util();
            String line;
            boolean isMe = false;
            boolean isAnon = false;
            ArrayList disallowed = new ArrayList();
            String ua = null;

            while ((line = r.readLine()) != null)
            {
                if (p.match("/^#.*/", line))
                {
                    // a comment
                    continue;
                }
                line = p.substitute("s/\\s*\\#.* //", line);
                if (p.match("/^\\s*$/", line))
                {
                    if (isMe)
                    {
                        break;
                    }
                }
                else if (p.match("/^User-Agent:\\s*(.*)/i", line))
                {
                    ua = p.group(1);
                    ua = p.substitute("s/\\s+$//", ua);
                    if (isMe)
                    {
                        break;
                    }
                    else if (ua.equals("*"))
                    {
                        isAnon = true;
                    }
                    else if (Constants.CRAWLER_AGENT.startsWith(ua))
                    {
                        isMe = true;
                    }
                }
                else if (p.match("/^Disallow:\\s*(.*)/i", line))
                {
                    if (ua == null)
                    {
                        isAnon = true;
                        // warn...
                    }
                    String disallow = p.group(1);
                    if (disallow != null && disallow.length() > 0)
                    {
                        // assume we have a relative path
                        ;
                    }
                    else
                    {
                        disallow = "/";
                    }
                    if (isMe || isAnon)
                    {
                        disallowed.add(disallow);
                    }
                }
                else
                {
                    // warn: unexpected line
                }
            }
            String[] disalloweds = new String[disallowed.size()];
            disallowed.toArray(disalloweds);
            return disalloweds;
        }

    }

}
