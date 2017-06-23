package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

public class QueryClient implements Runnable {

	public enum QueryType {
		
		TERM_NUMERIC_QUERY, 
		RANGE_NUMERIC_QUERY, 
		GREATER_THAN_NUMERIC_QUERY, 
		LESS_THAN_NUMERIC_QUERY, 
		AND_NUMERIC_QUERY, 
		OR_NUMERIC_QUERY
		
	}

	String urlString;
	String collectionName;
	SolrParams params;
	HttpSolrClient solrClient;
	QueryType queryType;
	int threadID;
	boolean setThreadReadyFlag = false;
	long numberOfThreads = 0;
	long startTime = 0;
	long delayEstimationBySeconds = 0;

	public static boolean running;
	public static long queryCount = 0;
	public static long totalQTime = 0;
	public static long minQtime = Long.MAX_VALUE;
	public static long maxQtime = Long.MIN_VALUE;
	public static long queryFailureCount = 0;
	public static long threadReadyCount = 0;
	public static DescriptiveStatistics percentiles;
	public static boolean percentilesObjectCreated = false;
	public static long[] qTimePercentileList = new long[10000000];
	public static int qTimePercentileListPointer = 0;

	public static ConcurrentLinkedQueue<String> termNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> greaterNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> lesserNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> rangeNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> andNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> orNumericQueryParameterList = new ConcurrentLinkedQueue<String>();


	Random random = new Random();

	public QueryClient(String urlString, String collectionName, QueryType queryType,
			long numberOfThreads, long delayEstimationBySeconds) {
		super();
		this.urlString = urlString;
		this.collectionName = collectionName;
		this.queryType = queryType;
		this.numberOfThreads = numberOfThreads;
		this.delayEstimationBySeconds = delayEstimationBySeconds;

		solrClient = new HttpSolrClient.Builder(urlString).build();
		Util.postMessage("\r" + this.toString() + "** QUERY CLIENT CREATED ...", MessageType.GREEN_TEXT, false);
	}
	
	public static void prepare() {
		Util.postMessage("** Preparing Term Query queue ...", MessageType.CYAN_TEXT, false);
		
		termNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		greaterNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		lesserNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		rangeNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		andNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		orNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		
		String line = "";
		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + "Numeric-Term-Query.txt"))) {

			while ((line = br.readLine()) != null) {
								termNumericQueryParameterList.add(line.trim());
								greaterNumericQueryParameterList.add(line.trim());
								lesserNumericQueryParameterList.add(line.trim());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Util.postMessage("** Queue preparation COMPLETE [READY NOW] ...", MessageType.GREEN_TEXT, false);
		Util.postMessage("** Preparing query pair data queue ...", MessageType.CYAN_TEXT, false);
		
		line = "";
		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + "Numeric-Pair-Query-Data.txt"))) {

			while ((line = br.readLine()) != null) {
								rangeNumericQueryParameterList.add(line.trim());
								andNumericQueryParameterList.add(line.trim());
								orNumericQueryParameterList.add(line.trim());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Util.postMessage("** Pair data queue preparation COMPLETE [READY NOW] ...", MessageType.GREEN_TEXT, false);
	
	}

	public void run() {

		long elapsedTime;

		NamedList<String> requestParams = new NamedList<>();
		requestParams.add("defType", "edismax");
		requestParams.add("wt", "json");

		startTime = System.currentTimeMillis();
		while (true) {

			if (!setThreadReadyFlag) {
				setThreadReadyFlag = true;
				setThreadReadyCount();
			}

			if (running == true) {
				// Critical Section ....
				SolrResponse response = null;
				try {
					requestParams.remove("q");
					
					if (this.queryType == QueryType.TERM_NUMERIC_QUERY) {
						requestParams.add("q", "Int1:"+ termNumericQueryParameterList.poll());
					} else if (this.queryType == QueryType.RANGE_NUMERIC_QUERY) {

						String pairData[] = rangeNumericQueryParameterList.poll().trim().split(",");
						
						int ft_1 = Integer.parseInt(pairData[0]);
						int ft_2 = Integer.parseInt(pairData[1]);

						if (ft_2 > ft_1) {
							requestParams.add("q", "Int1:[" + ft_1 + " TO " + ft_2 + "]");
						} else {
							requestParams.add("q", "Int1:[" + ft_2 + " TO " + ft_1 + "]");
						}

					} else if (this.queryType == QueryType.GREATER_THAN_NUMERIC_QUERY) {
						requestParams.add("q", "Int1:["
								+ greaterNumericQueryParameterList.poll()
								+ " TO *]");
					} else if (this.queryType == QueryType.LESS_THAN_NUMERIC_QUERY) {
						requestParams.add("q", "Int1:[* TO "
								+ lesserNumericQueryParameterList.poll()
								+ "]");
					} else if (this.queryType == QueryType.AND_NUMERIC_QUERY) {

						String pairData[] = andNumericQueryParameterList.poll().trim().split(",");
						
						int ft_1 = Integer.parseInt(pairData[0]);
						int ft_2 = Integer.parseInt(pairData[1]);

						requestParams.add("q", "Int1:" + ft_1 + " AND Int1:" + ft_2);

					} else if (this.queryType == QueryType.OR_NUMERIC_QUERY) {

						String pairData[] = orNumericQueryParameterList.poll().trim().split(",");
						
						int ft_1 = Integer.parseInt(pairData[0]);
						int ft_2 = Integer.parseInt(pairData[1]);

						requestParams.add("q", "Int1:" + ft_1 + " OR Int1:" + ft_2);

					} 

					params = SolrParams.toSolrParams(requestParams);

					response = this.fireQuery(collectionName, params);

					if ((System.currentTimeMillis() - startTime) >= (delayEstimationBySeconds * 1000)) {
						setQueryCounter();
						elapsedTime = response.getElapsedTime();
						setTotalQTime(elapsedTime);
						setMinMaxQTime(elapsedTime);
					} else {
						// This is deliberately done to warm up document cache ...
						requestParams.remove("q");
						requestParams.add("q", "*:*");
						params = SolrParams.toSolrParams(requestParams);
						response = solrClient.query(collectionName, params);
					}

				} catch (SolrServerException | IOException e) {
					setQueryFailureCount();
				}

			} else if (running == false) {
				// Break out from loop ...
				Util.postMessage("\r" + this.toString() + "** Getting out of critical section ...",
						MessageType.RED_TEXT, false);
				break;
			}
		}

		
		return;
	}

	private synchronized SolrResponse fireQuery(String collectionName, SolrParams params)
			throws SolrServerException, IOException {

		return solrClient.query(collectionName, params);

	}

	private synchronized void setQueryCounter() {

		if (running == false) {
			return;
		}

		queryCount++;
	}

	private synchronized void setTotalQTime(long qTime) {

		if (running == false) {
			return;
		}

		totalQTime += qTime;
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
		
		qTimePercentileList[qTimePercentileListPointer++] = QTime;
		
		if (QTime < minQtime) {
			minQtime = QTime;
		}

		if (QTime > maxQtime) {
			maxQtime = QTime;
		}

	}

	public static double getNthPercentileQTime(double percentile) {

		if(!percentilesObjectCreated) {
	
			double[] finalQtime = new double[qTimePercentileListPointer];
			for (int i = 0; i < (finalQtime.length); i++) {
					finalQtime[i] = qTimePercentileList[i];
			}			
			Arrays.sort(finalQtime);			
			percentiles = new DescriptiveStatistics(finalQtime);
			percentilesObjectCreated = true;
		}
		
		return percentiles.getPercentile(percentile);

	}

	public static void reset() {
		running = false;
		queryCount = 0;
		minQtime = Long.MAX_VALUE;
		maxQtime = Long.MIN_VALUE;
		queryFailureCount = 0;
		threadReadyCount = 0;
		percentiles = null;
		percentilesObjectCreated = false;
		qTimePercentileList = new long[10000000];
		qTimePercentileListPointer = 0;
		totalQTime = 0;

		
		termNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		greaterNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		lesserNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		rangeNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		andNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		orNumericQueryParameterList = new ConcurrentLinkedQueue<String>();

	}

}
