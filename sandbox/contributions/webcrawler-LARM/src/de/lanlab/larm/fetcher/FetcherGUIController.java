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


