package org.apache.solr.tests.nightlybenchmarks;

import java.io.IOException;
import java.util.Random;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

public class ThreadedNumericQueryClient implements Runnable {
	
	public enum NumericQueryType { TERM_NUMERIC_QUERY, RANGE_NUMERIC_QUERY, GREATER_THAN_NUMERIC_QUERY, LESS_THAN_NUMERIC_QUERY }
	
	String urlString; 
	int queueSize;
	int threadCount;
	String collectionName;
	SolrParams params;
	ConcurrentUpdateSolrClient solrClient;
	NumericQueryType queryType;

	public static boolean running;
	public static int queryCount = 0;
	public static long minQtime = Long.MAX_VALUE;
	public static long maxQtime = Long.MIN_VALUE;
	
    Random r = new Random();

	@SuppressWarnings("deprecation")
	public ThreadedNumericQueryClient(String urlString, int queueSize, int threadCount, String collectionName, NumericQueryType queryType) {
		super();
		this.urlString = urlString;
		this.queueSize = queueSize;
		this.threadCount = threadCount;
		this.collectionName = collectionName;
		this.queryType = queryType;		
		minQtime = Long.MAX_VALUE;
		maxQtime = Long.MIN_VALUE;		
		solrClient = new ConcurrentUpdateSolrClient(urlString, queueSize, threadCount);

		Util.postMessage("** CREATING CLIENT ...", MessageType.RESULT_ERRROR, false);		
	}
	
	public void run() {
		
		long elapsedTime;
		
		while (true) {
			if (running == true) {
				// Critical Section ....
				SolrResponse response;
				try {
					  NamedList<String> list = new NamedList<>();
				      list.add("defType", "edismax");
				      list.add("wt", "json");
					
						 if (this.queryType == NumericQueryType.TERM_NUMERIC_QUERY) {
							  list.add("q", "RandomIntField:"+ SolrIndexingClient.intList.get(r.nextInt(SolrIndexingClient.documentCount)));
						 } else if (this.queryType == NumericQueryType.RANGE_NUMERIC_QUERY) {
							 
							 			int ft_1 = SolrIndexingClient.intList.get(r.nextInt(SolrIndexingClient.documentCount));
							 			int ft_2 = SolrIndexingClient.intList.get(r.nextInt(SolrIndexingClient.documentCount));
							 			
							 			if (ft_2 > ft_1) {
											  list.add("q", "RandomIntField:["+ ft_1 + " TO " + ft_2 + "]");
							 			} else {
											  list.add("q", "RandomIntField:["+ ft_2 + " TO " + ft_1 + "]");
							 			}
							 
						 } else if (this.queryType == NumericQueryType.GREATER_THAN_NUMERIC_QUERY) {
							  list.add("q", "RandomIntField:["+ SolrIndexingClient.intList.get(r.nextInt(SolrIndexingClient.documentCount)) + " TO *]");
						 } else if (this.queryType == NumericQueryType.LESS_THAN_NUMERIC_QUERY) {
							  list.add("q", "RandomIntField:[* TO " + SolrIndexingClient.intList.get(r.nextInt(SolrIndexingClient.documentCount)) + "]");
						 }

						 params = SolrParams.toSolrParams(list);

						 			response = solrClient.query(collectionName, params);
						    		setQueryCounter();
						    		elapsedTime = response.getElapsedTime();
						    		setMinMaxQTime(elapsedTime);
						    		Util.postMessage(response.toString(), MessageType.GENERAL, false);
						    		Util.postMessage(this.toString() + " < " + elapsedTime + " > ", MessageType.ACTION, false);

				} catch (SolrServerException | IOException e) {
					e.printStackTrace();
				}	    		

			} else if (running == false) {
				// Break out from loop ...
				Util.postMessage("** Getting out of critical section ...", MessageType.RESULT_ERRROR, false);		
				break;
			}			
		}
		
	    solrClient.close();
		
	}
	
	private synchronized void setQueryCounter() {
		queryCount++;
	}
	
	private synchronized void setMinMaxQTime(long QTime) {
		
		if (QTime < minQtime) {
			minQtime = QTime;
		}
		
		if (QTime > maxQtime) {
			maxQtime = QTime;
		}
		
	}

}
