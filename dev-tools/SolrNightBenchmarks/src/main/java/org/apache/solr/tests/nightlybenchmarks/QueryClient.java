package org.apache.solr.tests.nightlybenchmarks;

import java.io.IOException;
import java.util.Random;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

public class QueryClient implements Runnable {
	
	public enum QueryType { TERM_NUMERIC_QUERY, RANGE_NUMERIC_QUERY, GREATER_THAN_NUMERIC_QUERY, LESS_THAN_NUMERIC_QUERY }
	
	String urlString; 
	int queueSize;
	int threadCount;
	String collectionName;
	SolrParams params;
	ConcurrentUpdateSolrClient solrClient;
	QueryType queryType;
	int threadID;
	boolean setThreadReadyFlag = false;
	long numberOfThreads = 0;
	long startTime = 0;
	long delayEstimationBySeconds = 0;

	public static boolean running;
	public static long queryCount = 0;
	public static long minQtime = Long.MAX_VALUE;
	public static long maxQtime = Long.MIN_VALUE;
	public static long queryFailureCount = 0;
	public static long threadReadyCount = 0;
	
    Random random = new Random();

	@SuppressWarnings("deprecation")
	public QueryClient(String urlString, int queueSize, int threadCount, String collectionName, QueryType queryType, long numberOfThreads, long delayEstimationBySeconds) {
		super();
		this.urlString = urlString;
		this.queueSize = queueSize;
		this.threadCount = threadCount;
		this.collectionName = collectionName;
		this.queryType = queryType;		
		minQtime = Long.MAX_VALUE;
		maxQtime = Long.MIN_VALUE;
		this.numberOfThreads = numberOfThreads;
		this.delayEstimationBySeconds = delayEstimationBySeconds;
		
		solrClient = new ConcurrentUpdateSolrClient(urlString, queueSize, threadCount);
		Util.postMessage("\r" + this.toString() + "** CREATING CLIENT ...", MessageType.RED_TEXT, false);		
	}
	
	public void run() {
		
		long elapsedTime;

		startTime = System.nanoTime();		
		while (true) {
			
			if (!setThreadReadyFlag) {
				setThreadReadyFlag = true;
				setThreadReadyCount();
			}
			
			if (running == true) {
				// Critical Section ....
				SolrResponse response;
				try {
					  NamedList<String> list = new NamedList<>();
				      list.add("defType", "edismax");
				      list.add("wt", "json");
					
						 if (this.queryType == QueryType.TERM_NUMERIC_QUERY) {
							  list.add("q", "RandomIntField:"+ SolrIndexingClient.intList.get(random.nextInt(SolrIndexingClient.documentCount)));
						 } else if (this.queryType == QueryType.RANGE_NUMERIC_QUERY) {
							 
							 			int ft_1 = SolrIndexingClient.intList.get(random.nextInt(SolrIndexingClient.documentCount));
							 			int ft_2 = SolrIndexingClient.intList.get(random.nextInt(SolrIndexingClient.documentCount));
							 			
							 			if (ft_2 > ft_1) {
											  list.add("q", "RandomIntField:["+ ft_1 + " TO " + ft_2 + "]");
							 			} else {
											  list.add("q", "RandomIntField:["+ ft_2 + " TO " + ft_1 + "]");
							 			}
							 
						 } else if (this.queryType == QueryType.GREATER_THAN_NUMERIC_QUERY) {
							  list.add("q", "RandomIntField:["+ SolrIndexingClient.intList.get(random.nextInt(SolrIndexingClient.documentCount)) + " TO *]");
						 } else if (this.queryType == QueryType.LESS_THAN_NUMERIC_QUERY) {
							  list.add("q", "RandomIntField:[* TO " + SolrIndexingClient.intList.get(random.nextInt(SolrIndexingClient.documentCount)) + "]");
						 }

						 params = SolrParams.toSolrParams(list);

						 			response = solrClient.query(collectionName, params);
						 			
						 			if ((System.nanoTime() - startTime) >= (delayEstimationBySeconds * 1000000000)) {
							    		setQueryCounter();
							    		elapsedTime = response.getElapsedTime();
							    		setMinMaxQTime(elapsedTime);
							    		Util.postMessage("\r" + response.toString(), MessageType.BLUE_TEXT, false);
							    		Util.postMessage("\r" + this.toString() + " < " + elapsedTime + " > ", MessageType.WHITE_TEXT, false);
						 			} else {
							    		Util.postMessage("\rWaiting For Estimation to start ...", MessageType.BLUE_TEXT, false);
						 			}

				} catch (SolrServerException | IOException e) {
					//e.printStackTrace();
					setQueryFailureCount();
				}	    		

			} else if (running == false) {
				// Break out from loop ...
				Util.postMessage("\r" + this.toString() + "** Getting out of critical section ...", MessageType.RED_TEXT, false);		
				break;
			}			
		}
		
	    solrClient.close();	    
		return;
	}
	
	private synchronized void setQueryCounter() {
		
		if (running == false) {
			return;
		}
		
		queryCount++;
	}

	private synchronized void setQueryFailureCount() {
		
		queryFailureCount++;
	
	}
	
	private synchronized void setThreadReadyCount() {
		
		threadReadyCount++;
	
	}

	private synchronized void setMinMaxQTime(long QTime) {

		if (running == false) {
			return;
		}
		
		if (QTime < minQtime) {
			minQtime = QTime;
		}
		
		if (QTime > maxQtime) {
			maxQtime = QTime;
		}
		
	}
	
	public static void reset() {
		running = false;
		queryCount = 0;
		minQtime = Long.MAX_VALUE;
		maxQtime = Long.MIN_VALUE;
		queryFailureCount = 0;
		threadReadyCount = 0;
	}

}
