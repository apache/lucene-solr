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

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.awt.event.*;
import de.lanlab.larm.gui.*;
import de.lanlab.larm.threads.*;

/**
 * this was used to connect the GUI to the fetcher
 * @TODO put this into the GUI package, probably?
 * @version $Id$
 */
public class FetcherGUIController implements ActionListener
{
    FetcherMain  fetcherMain;
    FetcherSummaryFrame  fetcherFrame;


    public FetcherGUIController(FetcherMain fetcherMainPrg, FetcherSummaryFrame fetcherFrameWin, String defaultStartURL)
    {
        this.fetcherMain  = fetcherMainPrg;
        this.fetcherFrame = fetcherFrameWin;

        fetcherFrame.setRestrictTo(fetcherMain.urlScopeFilter.getRexString());
        fetcherFrame.setStartURL(defaultStartURL);

        fetcherMain.fetcher.addThreadPoolObserver(
            new ThreadPoolObserver()
            {
               public void threadUpdate(int threadNr, String action, String info)
               {
                    String status = threadNr + ": " + action + ": " + info;
                    fetcherFrame.setIdleThreadsCount(fetcherMain.fetcher.getIdleThreadsCount());
                    fetcherFrame.setBusyThreadsCount(fetcherMain.fetcher.getBusyThreadsCount());
                    fetcherFrame.setWorkingThreadsCount(fetcherMain.fetcher.getWorkingThreadsCount());
               }

               public void queueUpdate(String info, String action)
               {
                    fetcherFrame.setRequestQueueCount(fetcherMain.fetcher.getQueueSize());
               }
            }
        );

        fetcherMain.monitor.addObserver(new Observer()
        {
            public void update(Observable o, Object arg)
            {
                // der ThreadMonitor wurde geupdated
                //fetcherFrame.setStalledThreads(fetcherMain.monitor.getStalledThreadCount(10, 500.0));
                //fetcherFrame.setBytesPerSecond(fetcherMain.monitor.getAverageReadCount(5));
                // fetcherFrame.setDocsPerSecond(fetcherMain.monitor.getDocsPerSecond(5));
                // wir nutzen die Gelegenheit, den aktuellen Speicherbestand auszugeben
                fetcherFrame.setFreeMem(Runtime.getRuntime().freeMemory());
                fetcherFrame.setTotalMem(Runtime.getRuntime().totalMemory());

            }

        });

    /*	fetcherMain.reFilter.addObserver(
            new Observer()
            {
                public void update(Observable o, Object arg)
                {
                    fetcherFrame.setRobotsTxtCount(fetcherMain.reFilter.getExcludingHostsCount());
                }
            }
        );*/

        fetcherMain.messageHandler.addMessageQueueObserver(new Observer()
            {
                public void update(Observable o, Object arg)
                {
                    // a message has been added or deleted

                    fetcherFrame.setURLsQueued(fetcherMain.messageHandler.getQueued());
                }

            }
        );

        // this observer will be called if a filter has decided to throw a
        // message away.
        fetcherMain.messageHandler.addMessageProcessorObserver(new Observer()
            {
                public void update(Observable o, Object arg)
                {
                    if(arg == fetcherMain.urlScopeFilter)
                    {
                        fetcherFrame.setScopeFiltered(fetcherMain.urlScopeFilter.getFiltered());
                    }
                    else if(arg == fetcherMain.urlVisitedFilter)
                    {
                        fetcherFrame.setVisitedFiltered(fetcherMain.urlVisitedFilter.getFiltered());
                    }
                    else if(arg == fetcherMain.reFilter)
                    {
                        fetcherFrame.setURLsCaughtCount(fetcherMain.reFilter.getFiltered());
                    }
                    else // it's the fetcher
                    {
                        fetcherFrame.setDocsRead(fetcherMain.fetcher.getDocsRead());
                    }
                }
            }
        );

        fetcherFrame.addWindowListener(
            new WindowAdapter()
            {
                public void windowClosed(WindowEvent e)
                {
                    System.out.println("window Closed");
                    System.exit(0);
                }


            }
        );

        fetcherFrame.addStartButtonListener((ActionListener)this);
    }

    /**
     *   will be called when the start button is pressed
     */
    public void actionPerformed(ActionEvent e)
    {
        System.out.println("Füge Start-URL ein");
        try
        {
           // urlVisitedFilter.printAllURLs();
           // urlVisitedFilter.clearHashtable();
            fetcherMain.setRexString(fetcherFrame.getRestrictTo());
            fetcherMain.startMonitor();
            fetcherMain.putURL(new URL(fetcherFrame.getStartURL()), false);
        }
        catch(Exception ex)
        {
            System.out.println("actionPerformed: Exception: " + ex.getMessage());
        }
    }

}


