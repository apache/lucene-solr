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


import de.lanlab.larm.threads.*;
import java.util.*;
import java.text.*;
import java.io.*;
import de.lanlab.larm.util.State;
import de.lanlab.larm.util.SimpleLoggerManager;

/**
 * this monitor takes a sample of every thread every x milliseconds,
 * and logs a lot of information. In the near past it has evolved into the multi
 * purpose monitoring and maintenance facility.
 * At the moment it prints status information
 * to log files and to the console
 * @TODO this can be done better. Probably with an agent where different services
 * can be registered to be called every X seconds
 * @version $Id$
 */
public class ThreadMonitor extends Observable implements Runnable
{
    /**
     * a reference to the thread pool that's gonna be observed
     */
    private ThreadPool threadPool;


    class Sample
    {
        long bytesRead;
        long  docsRead;
        long time;
        public Sample(long bytesRead, long docsRead, long time)
        {
            this.bytesRead = bytesRead;
            this.docsRead = docsRead;
            this.time = time;
        }
    }

    ArrayList bytesReadPerPeriod;

    /**
     * Zeit zwischen den Messungen
     */
    int sampleDelta;

    /**
     * the thread where this monitor runs in. Will run with high priority
     */
    Thread thread;


    URLVisitedFilter urlVisitedFilter;
    URLScopeFilter   urlScopeFilter;
//    DNSResolver dnsResolver;
    RobotExclusionFilter reFilter;
    MessageHandler messageHandler;
    URLLengthFilter urlLengthFilter;
    HostManager hostManager;

    public final static double KBYTE = 1024;
    public final static double MBYTE = 1024 * KBYTE;
    public final static double ONEGBYTE = 1024 * MBYTE;


    String formatBytes(long lbytes)
    {
        double bytes = (double)lbytes;
        if(bytes >= ONEGBYTE)
        {
            return fractionFormat.format((bytes/ONEGBYTE)) + " GB";
        }
        else if(bytes >= MBYTE)
        {
            return fractionFormat.format(bytes/MBYTE) + " MB";
        }
        else if(bytes >= KBYTE)
        {
            return fractionFormat.format(bytes/KBYTE) + " KB";
        }
        else
        {
            return fractionFormat.format(bytes) + " Bytes";
        }

    }

    /**
     * a logfile where status information is posted
     * FIXME: put that in a seperate class (double code in FetcherTask)
     */
    PrintWriter logWriter;
    private SimpleDateFormat formatter
     = new SimpleDateFormat ("hh:mm:ss:SSSS");
    private DecimalFormat fractionFormat = new DecimalFormat("0.00");

    long startTime = System.currentTimeMillis();

    private void log(String text)
    {
        try
        {
            logWriter.println(formatter.format(new Date()) + ";" + (System.currentTimeMillis()-startTime) + ";"  + text);
            logWriter.flush();
        }
        catch(Exception e)
        {
            System.out.println("Couldn't write to logfile");
        }
    }

    /**
     * construct the monitor gets a reference to all monitored filters
     * @param threadPool  the pool to be observed
     * @param sampleDelta time in ms between samples
     */
    public ThreadMonitor(URLLengthFilter urlLengthFilter,
                         URLVisitedFilter urlVisitedFilter,
                         URLScopeFilter urlScopeFilter,
                         /*DNSResolver dnsResolver,*/
                         RobotExclusionFilter reFilter,
                         MessageHandler messageHandler,
                         ThreadPool threadPool,
                         HostManager hostManager,
                         int sampleDelta)
    {
        this.urlLengthFilter = urlLengthFilter;
        this.urlVisitedFilter = urlVisitedFilter;
        this.urlScopeFilter   = urlScopeFilter;
       /* this.dnsResolver = dnsResolver;*/
        this.hostManager = hostManager;
        this.reFilter = reFilter;
        this.messageHandler = messageHandler;

        this.threadPool = threadPool;
        bytesReadPerPeriod = new ArrayList();
        this.sampleDelta = sampleDelta;
        this.thread = new Thread(this, "ThreadMonitor");
        this.thread.setPriority(7);

        try
        {
            File logDir = new File("logs");
            logDir.mkdir();
            logWriter = new PrintWriter(new BufferedWriter(new FileWriter("logs/ThreadMonitor.log")));
        }
        catch(IOException e)
        {
            System.out.println("Couldn't create logfile (ThreadMonitor)");
        }

    }

    /**
     * java.lang.Threads run method. To be invoked via start()
     * the monitor's main thread takes the samples every sampleDelta ms
     * Since Java is not real time, it remembers
     */
    public void run()
    {
        int nothingReadCount = 0;
        long lastPeriodBytesRead = -1;
        long monitorRunCount = 0;
        long startTime = System.currentTimeMillis();
        log("time;overallBytesRead;overallTasksRun;urlsQueued;urlsWaiting;isWorkingOnMessage;urlsScopeFiltered;urlsVisitedFiltered;urlsREFiltered;memUsed;memFree;totalMem;nrHosts;visitedSize;visitedStringSize;urlLengthFiltered");
        while(true)
        {
            try
            {
                try
                {
                    thread.sleep(sampleDelta);
                }
                catch(InterruptedException e)
                {
                    return;
                }

                Iterator threadIterator = threadPool.getThreadIterator();
                int i=0;
                StringBuffer bytesReadString = new StringBuffer(200);
                StringBuffer rawBytesReadString = new StringBuffer(200);
                StringBuffer tasksRunString = new StringBuffer(200);
                long overallBytesRead = 0;
                long overallTasksRun  = 0;
                long now = System.currentTimeMillis();
                boolean finished = false;
                //System.out.print("\f");
                /*while(!finished)
                {
                    boolean restart = false;*/
                boolean allThreadsIdle = true;
                StringBuffer sb = new StringBuffer(500);

                while(threadIterator.hasNext())
                {
                    FetcherThread thread = (FetcherThread)threadIterator.next();
                    long totalBytesRead = thread.getTotalBytesRead();
                    overallBytesRead += totalBytesRead;
                    bytesReadString.append(formatBytes(totalBytesRead)).append( "; ");
                    rawBytesReadString.append(totalBytesRead).append("; ");
                    long tasksRun = thread.getTotalTasksRun();
                    overallTasksRun += tasksRun;
                    tasksRunString.append(tasksRun).append("; ");

                    // check task status
                    State state = thread.getTaskState();
                    //StringBuffer sb = new StringBuffer(200);
                    sb.setLength(0);
                    System.out.println(sb + "[" + thread.getThreadNumber() + "] " + state.getState() + " for " +
                                       (now - state.getStateSince() ) + " ms " +
                                       (state.getInfo() != null ? "(" + state.getInfo() +")" : "")
                                       );
                    if(!(state.getState().equals(FetcherThread.STATE_IDLE)))
                    {
                        //if(allThreadsIdle) System.out.println("(not all threads are idle, '"+state.getState()+"' != '"+FetcherThread.STATE_IDLE+"')");
                        allThreadsIdle = false;
                    }
                    if (((state.equals(FetcherTask.FT_CONNECTING)) || (state.equals(FetcherTask.FT_GETTING)) || (state.equals(FetcherTask.FT_READING)) || (state.equals(FetcherTask.FT_CLOSING)))
                        && ((now - state.getStateSince()) > 160000))
                    {
                        System.out.println("****Restarting Thread " + thread.getThreadNumber());
                        threadPool.restartThread(thread.getThreadNumber());
                        break;  // Iterator is invalid
                    }

                }
                /*if(restart)
                {
                    continue;
                }
                finished = true;
                }*/
                /*
                if(overallBytesRead == lastPeriodBytesRead)
                {
                    *
                    disabled kickout feature - cm

                    nothingReadCount ++;
                   System.out.println("Anomaly: nothing read during the last period(s). " + (20-nothingReadCount+1) + " periods to exit");
                    if(nothingReadCount > 20)  // nothing happens anymore
                    {
                        log("Ending");
                        System.out.println("End at " + new Date().toString());
                        // print some information
                        System.exit(0);
                    }


                }
                else
                {
                    nothingReadCount = 0;
                }*/

                lastPeriodBytesRead = overallBytesRead;

                //State reState = new State("hhh"); //reFilter.getState();
                sb.setLength(0);
                //System.out.println(sb + "Robot-Excl.Filter State: " + reState.getState() + " since " + (now-reState.getStateSince()) + " ms " + (reState.getInfo() != null ? " at " + reState.getInfo() : ""));

                addSample(new Sample(overallBytesRead, overallTasksRun, System.currentTimeMillis()));
                int nrHosts = ((FetcherTaskQueue)threadPool.getTaskQueue()).getNumHosts();
                int visitedSize       = urlVisitedFilter.size();
                int visitedStringSize = urlVisitedFilter.getStringSize();

                double bytesPerSecond = getAverageBytesRead();
                double docsPerSecond = getAverageDocsRead();
                sb.setLength(0);
                System.out.println(sb + "\nBytes total:          " + formatBytes(overallBytesRead) + "  (" + formatBytes((long)(((double)overallBytesRead)*1000/(System.currentTimeMillis()-startTime))) + " per second since start)" +
                                   "\nBytes per Second:     " + formatBytes((int)bytesPerSecond) + " (50 secs)" +
                                   "\nDocs per Second:      " + docsPerSecond +
                                   "\nBytes per Thread:     " + bytesReadString);
                double docsPerSecondTotal = ((double)overallTasksRun)*1000/(System.currentTimeMillis()-startTime);
                sb.setLength(0);
                System.out.println(sb + "Docs read total:      " + overallTasksRun + "    Docs/s: " + fractionFormat.format(docsPerSecondTotal) +
                                   "\nDocs p.thread:        " + tasksRunString);

                long memUsed = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
                long memFree = Runtime.getRuntime().freeMemory();
                long totalMem = Runtime.getRuntime().totalMemory();
                sb.setLength(0);
                System.out.println(sb + "Mem used:             " + formatBytes(memUsed) +  ", free: " + formatBytes(memFree) + "     total VM: " + totalMem);
                int urlsQueued = messageHandler.getQueued();
                int urlsWaiting = threadPool.getQueueSize();
                boolean isWorkingOnMessage = messageHandler.isWorkingOnMessage();
                int urlsScopeFiltered = urlScopeFilter.getFiltered();
                int urlsVisitedFiltered = urlVisitedFilter.getFiltered();
                int urlsREFiltered = reFilter.getFiltered();
                int urlLengthFiltered = urlLengthFilter.getFiltered();
                sb.setLength(0);
                System.out.println(sb + "URLs queued:          " + urlsQueued + "     waiting: " + urlsWaiting);
                sb.setLength(0);
                System.out.println(sb + "Message is being processed: " + isWorkingOnMessage);
                sb.setLength(0);
                System.out.println(sb + "URLs Filtered: length: " + urlLengthFiltered + "      scope: " + urlsScopeFiltered + "     visited: " + urlsVisitedFiltered + "      robot.txt: " + urlsREFiltered);
                sb.setLength(0);
                System.out.println(sb + "Visited size: " + visitedSize + "; String Size in VisitedFilter: " + visitedStringSize + "; Number of Hosts: " + nrHosts + "; hosts in Host Manager: " + hostManager.getSize() + "\n");
                sb.setLength(0);
                log(sb + "" + now + ";" + overallBytesRead + ";" + overallTasksRun + ";" + urlsQueued + ";" + urlsWaiting + ";" + isWorkingOnMessage + ";" + urlsScopeFiltered + ";" + urlsVisitedFiltered + ";" + urlsREFiltered + ";" + memUsed + ";" + memFree + ";" + totalMem + ";" + nrHosts + ";" + visitedSize + ";" + visitedStringSize + ";" + rawBytesReadString + ";" + urlLengthFiltered);


                if(!isWorkingOnMessage && (urlsQueued == 0) && (urlsWaiting == 0) && allThreadsIdle)
                {
                    nothingReadCount++;
                    if(nothingReadCount > 3)
                    {
                        SimpleLoggerManager.getInstance().flush();
                        System.exit(0);
                    }

                }
                else
                {
                    nothingReadCount = 0;
                }

                this.setChanged();
                this.notifyObservers();

                // Request Garbage Collection
                monitorRunCount++;

                if(monitorRunCount % 6 == 0)
                {
                    System.runFinalization();
                }

                if(monitorRunCount % 2 == 0)
                {
                    System.gc();
                    SimpleLoggerManager.getInstance().flush();
                }

            }
            catch(Exception e)
            {
                System.out.println("Monitor: Exception: " + e.getClass().getName());
                e.printStackTrace();
            }
        }
    }

    /**
     * start the thread
     */
    public void start()
    {
        this.clear();
        thread.start();
    }

    /**
     * interrupt the monitor thread
     */
    public void interrupt()
    {
        thread.interrupt();
    }


    public synchronized void clear()
    {
        //sampleTimeStamps.clear();
        /*for(int i=0; i < timeSamples.length; i++)
        {
            timeSamples[i].clear();
        }
        */
    }

/*    public synchronized double getAverageReadCount(int maxPeriods)
    {
        int lastPeriod = bytesReadPerPeriod.size()-1;
        int periods = Math.min(lastPeriod, maxPeriods);
        if(periods < 2)
        {
            return 0.0;
        }


        long bytesLastPeriod =   ((Sample)bytesReadPerPeriod.get(lastPeriod)).bytesRead;
        long bytesBeforePeriod = ((Sample)bytesReadPerPeriod.get(lastPeriod - periods)).bytesRead;
        long bytesRead = bytesLastPeriod - bytesBeforePeriod;

        long endTime = ((Long)sampleTimeStamps.get(sampleTimeStamps.size()-1)).longValue();
        long startTime = ((Long)sampleTimeStamps.get(sampleTimeStamps.size()-1 - periods)).longValue();
        long duration = endTime - startTime;
        System.out.println("bytes read: " + bytesRead + " duration in s: " + duration/1000.0 + " = " + ((double)bytesRead) / (duration/1000.0) + " per second");

        return ((double)bytesRead) / (duration/1000.0);
    }
*/

    /*public synchronized double getDocsPerSecond(int maxPeriods)
    {
        int lastPeriod = bytesReadPerPeriod.size()-1;
        int periods = Math.min(lastPeriod, maxPeriods);
        if(periods < 2)
        {
            return 0.0;
        }


        long docsLastPeriod =   ((Sample)bytesReadPerPeriod.get(lastPeriod)).docsRead;
        long docsBeforePeriod = ((Sample)bytesReadPerPeriod.get(lastPeriod - periods)).docsRead;
        long docsRead = docsLastPeriod - docsBeforePeriod;

        long endTime = ((Long)sampleTimeStamps.get(sampleTimeStamps.size()-1)).longValue();
        long startTime = ((Long)sampleTimeStamps.get(sampleTimeStamps.size() - periods)).longValue();
        long duration = endTime - startTime;
        System.out.println("docs read: " + docsRead + " duration in s: " + duration/1000.0 + " = " + ((double)docsRead) / (duration/1000.0) + " per second");

        return ((double)docsRead) / (duration/1000.0);
    }*/

    /**
     * retrieves the number of threads whose byteCount is below the threshold
     * @param maxPeriods the number of periods to look back
     * @param threshold  the number of bytes per second that acts as the threshold for a stalled thread
     */
    /*public synchronized int getStalledThreadCount(int maxPeriods, double threshold)
    {
        int periods = Math.min(sampleTimeStamps.size(), maxPeriods);
        int stalledThreads = 0;
        int j=0, i=0;
        if(periods > 1)
        {
            for(j=0; j<timeSamples.length; j++)
            {
                long threadByteCount = 0;
                ArrayList actArrayList = timeSamples[j];
                double bytesPerSecond = 0;
                try
                {
                    for(i=0; i<periods; i++)
                    {

                        Sample actSample = (Sample)(actArrayList.get(i));
                        threadByteCount += actSample.bytesRead;
                    }
                }
                catch(Exception e)
                {
                    System.out.println("getAverageReadCount: " + e.getClass().getName() + ": " + e.getMessage() + "(" + i + ";" + j + ")");
                    e.printStackTrace();
                }

                bytesPerSecond = ((double)threadByteCount) /
                       ((double)((Long)sampleTimeStamps.get(sampleTimeStamps.size()-1)).longValue()
                      - ((Long)sampleTimeStamps.get(sampleTimeStamps.size()-periods)).longValue()) * 1000.0;
                if(bytesPerSecond < threshold)
                {
                    stalledThreads++;
                }
            }
        }

        return stalledThreads;
    }
*/

    int samples=0;

    public void addSample(Sample s)
    {
        if(samples < 10)
        {
            bytesReadPerPeriod.add(s);
            samples++;
        }
        else
        {
            bytesReadPerPeriod.set(samples % 10, s);
        }
    }

    public double getAverageBytesRead()
    {
        Iterator i = bytesReadPerPeriod.iterator();
        Sample oldest = null;
        Sample newest = null;
        while(i.hasNext())
        {

            Sample s = (Sample)i.next();
            if(oldest == null)
            {
                oldest = newest = s;
            }
            else
            {
                if(s.time < oldest.time)
                {
                    oldest = s;
                }
                else if(s.time > newest.time)
                {
                    newest = s;
                }
            }
        }
        return ((newest.bytesRead - oldest.bytesRead)/((newest.time - oldest.time)/1000.0));
    }
    public double getAverageDocsRead()
    {
        Iterator i = bytesReadPerPeriod.iterator();
        Sample oldest = null;
        Sample newest = null;
        while(i.hasNext())
        {

            Sample s = (Sample)i.next();
            if(oldest == null)
            {
                oldest = newest = s;
            }
            else
            {
                if(s.time < oldest.time)
                {
                    oldest = s;
                }
                else if(s.time > newest.time)
                {
                    newest = s;
                }
            }
        }
        return ((newest.docsRead - oldest.docsRead)/((newest.time - oldest.time)/1000.0));
    }
}


