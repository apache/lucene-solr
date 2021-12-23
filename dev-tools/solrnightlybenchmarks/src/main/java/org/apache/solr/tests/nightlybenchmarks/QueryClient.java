/*
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

package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * This class provides implementation for Query Client for Solr Standalone and Solr Cloud.
 * @author Vivek Narang
 *
 */
public class QueryClient implements Runnable {
	
	/**
	 * A enum defining the type of client to use for querying.
	 */
	public enum QueryClientType {
		HTTP_SOLR_CLIENT,
		CLOUD_SOLR_CLIENT
	}

	/**
	 * An enum defining various types of queries.
	 */
	public enum QueryType {

		TERM_NUMERIC_QUERY, 
		RANGE_NUMERIC_QUERY, 
		GREATER_THAN_NUMERIC_QUERY, 
		LESS_THAN_NUMERIC_QUERY, 
		AND_NUMERIC_QUERY, 
		OR_NUMERIC_QUERY, 
		SORTED_NUMERIC_QUERY, 
		SORTED_TEXT_QUERY, 
		TEXT_TERM_QUERY, 
		TEXT_PHRASE_QUERY,
		HIGHLIGHT_QUERY,
		CLASSIC_TERM_FACETING,
		CLASSIC_RANGE_FACETING,
		JSON_TERM_FACETING,
		JSON_RANGE_FACETING

	}
	
	public final static Logger logger = Logger.getLogger(QueryClient.class);
	public static ConcurrentLinkedQueue<String> termNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> greaterNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> lesserNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> rangeNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> andNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> orNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> sortedNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> textTerms = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> textPhrases = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> highlightTerms = new ConcurrentLinkedQueue<String>();
	public static ConcurrentLinkedQueue<String> rangeFacetRanges = new ConcurrentLinkedQueue<String>();
	public static boolean running;
	public static long queryCount = 0;
	public static long totalQTime = 0;
	public static long minQtime = Long.MAX_VALUE;
	public static long maxQtime = Long.MIN_VALUE;
	public static long queryFailureCount = 0;
	public static long threadReadyCount = 0;
	public static DescriptiveStatistics percentiles;
	public static boolean percentilesObjectCreated = false;
	public static long[] qTimePercentileList = new long[1000000];
	public static int qTimePercentileListPointer = 0;
	public static QueryType queryType;
	public static long queryCountLimit;
	public static long startTime = 0;
	public static long endTime = 0;
	public static boolean startTimeLock = false;
	public static long threadGroupStartTime = 0;

    CountDownLatch latch;
	String urlString;
	String collectionName;
	SolrParams params;
	SolrClient solrClient;
	int threadID;
	boolean setThreadReadyFlag = false;
	long numberOfThreads = 0;
	long delayEstimationBySeconds = 0;
	Random random = new Random();

	/**
	 * Constructor.
	 * 
	 * @param urlString
	 * @param collectionName
	 * @param queryType
	 * @param numberOfThreads
	 * @param delayEstimationBySeconds
	 */
	public QueryClient(String urlString, String collectionName, long numberOfThreads,
			long delayEstimationBySeconds, QueryClientType queryClientType, String zookeeperURL, CountDownLatch latch) {
		
		super();
		this.urlString = urlString;
		this.collectionName = collectionName;
		this.numberOfThreads = numberOfThreads;
		this.delayEstimationBySeconds = delayEstimationBySeconds;
		this.latch = latch;
		
		if (queryClientType == QueryClientType.HTTP_SOLR_CLIENT) {
			solrClient = new HttpSolrClient.Builder(urlString).build();
		} else if (queryClientType == QueryClientType.CLOUD_SOLR_CLIENT) {
			solrClient = new CloudSolrClient.Builder().withZkHost(zookeeperURL).build();
		}

		logger.debug(this.toString() + " QUERY CLIENT CREATED ... Testing Type: " + queryType);
	}

	/**
	 * A method invoked to inject the query terms in the data variables for the
	 * threads to use.
	 * @throws Exception 
	 */
	public static void prepare() throws Exception {

		logger.debug("Preparing Term Query queue ...");

		termNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		greaterNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		lesserNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		rangeNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		andNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		orNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		sortedNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		textTerms = new ConcurrentLinkedQueue<String>();
		textPhrases = new ConcurrentLinkedQueue<String>();
		highlightTerms = new ConcurrentLinkedQueue<String>();
		rangeFacetRanges = new ConcurrentLinkedQueue<String>();

		String line = "";
		if (QueryClient.queryType == QueryType.TERM_NUMERIC_QUERY || QueryClient.queryType == QueryType.GREATER_THAN_NUMERIC_QUERY 
				|| QueryClient.queryType == QueryType.LESS_THAN_NUMERIC_QUERY || QueryClient.queryType == QueryType.CLASSIC_TERM_FACETING
				|| QueryClient.queryType == QueryType.JSON_TERM_FACETING) {
			try (BufferedReader br = new BufferedReader(
					new FileReader(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_TERM_DATA))) {
	
				while ((line = br.readLine()) != null) {
					if (QueryClient.queryType == QueryType.TERM_NUMERIC_QUERY) {
						termNumericQueryParameterList.add(line.trim());
					} else if (QueryClient.queryType == QueryType.GREATER_THAN_NUMERIC_QUERY 
							|| QueryClient.queryType == QueryType.CLASSIC_TERM_FACETING || QueryClient.queryType == QueryType.JSON_TERM_FACETING) {
						greaterNumericQueryParameterList.add(line.trim());
					} else if (QueryClient.queryType == QueryType.LESS_THAN_NUMERIC_QUERY) {
						lesserNumericQueryParameterList.add(line.trim());
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing Term Query queue [COMPLETE] ...");
		} else if (QueryClient.queryType == QueryType.RANGE_NUMERIC_QUERY) {
			logger.debug("Preparing query pair data queue ...");
	
			line = "";
			try (BufferedReader br = new BufferedReader(
					new FileReader(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_PAIR_DATA))) {
	
				while ((line = br.readLine()) != null) {
					rangeNumericQueryParameterList.add(line.trim());
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			
			logger.debug("Preparing query pair data queue [COMPLETE] ...");			
		} else if (QueryClient.queryType == QueryType.TEXT_TERM_QUERY) {
			logger.debug("Preparing text terms query data ...");			
	
			line = "";
			try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.TEXT_TERM_DATA))) {
	
				while ((line = br.readLine()) != null) {
					textTerms.add(line.trim());
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing text terms query data [COMPLETE]...");			
		} else if (QueryClient.queryType == QueryType.TEXT_PHRASE_QUERY) {
			logger.debug("Preparing text phrase query data ...");			
	
			line = "";
			try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.TEXT_PHRASE_DATA))) {
	
				while ((line = br.readLine()) != null) {
					textPhrases.add(line.trim());
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing text phrase query data [COMPLETE] ...");			
		} else if (QueryClient.queryType == QueryType.AND_NUMERIC_QUERY || QueryClient.queryType == QueryType.OR_NUMERIC_QUERY) {
			logger.debug("Preparing query pair data for AND/OR queue ...");			
	
			line = "";
			try (BufferedReader br = new BufferedReader(
					new FileReader(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_QUERY_AND_OR_DATA))) {
	
				while ((line = br.readLine()) != null) {
					if (QueryClient.queryType == QueryType.AND_NUMERIC_QUERY) {
						andNumericQueryParameterList.add(line.trim());
					} else if (QueryClient.queryType == QueryType.OR_NUMERIC_QUERY) {
						orNumericQueryParameterList.add(line.trim());
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing query pair data for AND/OR queue [COMPLETE] ...");			
		} else if (QueryClient.queryType == QueryType.SORTED_NUMERIC_QUERY || QueryClient.queryType == QueryType.SORTED_TEXT_QUERY) {
			logger.debug("Preparing sorted query pair data queue ...");			
	
			line = "";
			try (BufferedReader br = new BufferedReader(
					new FileReader(Util.TEST_DATA_DIRECTORY + Util.NUMERIC_SORTED_QUERY_PAIR_DATA))) {
	
				while ((line = br.readLine()) != null) {
					sortedNumericQueryParameterList.add(line.trim());
				}
	
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing sorted query pair data queue [COMPLETE] ...");			
		} else if (QueryClient.queryType == QueryType.HIGHLIGHT_QUERY) {
			logger.debug("Preparing highlight terms data queue ...");			
	
			line = "";
			try (BufferedReader br = new BufferedReader(
					new FileReader(Util.TEST_DATA_DIRECTORY + Util.HIGHLIGHT_TERM_DATA))) {
	
				while ((line = br.readLine()) != null) {
					highlightTerms.add(line.trim());
				}
	
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing highlight terms data queue [COMPLETE] ...");			
		} else if (QueryClient.queryType == QueryType.CLASSIC_RANGE_FACETING 
				|| QueryClient.queryType == QueryType.JSON_RANGE_FACETING) {
			logger.debug("Preparing range facet data queue ...");			
	
			line = "";
			try (BufferedReader br = new BufferedReader(
					new FileReader(Util.TEST_DATA_DIRECTORY + Util.RANGE_FACET_DATA))) {
	
				while ((line = br.readLine()) != null) {
					rangeFacetRanges.add(line.trim());
				}
	
			} catch (Exception e) {
				logger.error(e.getMessage());
				throw new Exception(e.getMessage());
			}
			logger.debug("Preparing range facet data queue [COMPLETE]...");			
		}
		
		logger.debug("Starting State:| " + termNumericQueryParameterList.size() + "| "
				+ greaterNumericQueryParameterList.size() + "| " + lesserNumericQueryParameterList.size() + "| "
				+ andNumericQueryParameterList.size() + "| " + orNumericQueryParameterList.size() + "| "
				+ sortedNumericQueryParameterList.size() + "| " + rangeNumericQueryParameterList.size() + "| "
				+ textTerms.size() + "| " + textPhrases.size() + "| " + highlightTerms.size()
				+ "| " + rangeFacetRanges.size());			
	}

	/**
	 * A method used by various query threads.
	 */
	public void run() {
 
		long elapsedTime;

		NamedList<String> requestParams = new NamedList<>();
		requestParams.add("defType", "edismax");
		requestParams.add("wt", "json");

		threadGroupStartTime = System.currentTimeMillis();
		while (true) {

			if (!setThreadReadyFlag) {
				setThreadReadyFlag = true;
				setThreadReadyCount();
			}

			if (running == true) {
				// Critical Section: When actual querying begins.
				SolrResponse response = null;
				try {
					
					if ((System.currentTimeMillis() - threadGroupStartTime) >= (delayEstimationBySeconds * 1000)) {
						
						if (!startTimeLock) {
							startTime = System.currentTimeMillis();
							startTimeLock = true;
						}
						
						requestParams.remove("q");
						requestParams.remove("sort");
						requestParams.remove("hl");
						requestParams.remove("hl.fl");
						requestParams.remove("facet");
						requestParams.remove("facet.field");
						requestParams.remove("facet.range");
						requestParams.remove("f.Int2_i.facet.range.start");
						requestParams.remove("f.Int2_i.facet.range.end");
						requestParams.remove("f.Int2_i.facet.range.gap");
						requestParams.remove("json.facet");
	
						if (QueryClient.queryType == QueryType.TERM_NUMERIC_QUERY) {
							requestParams.add("q", "Int1_i:" + termNumericQueryParameterList.poll());
						} else if (QueryClient.queryType == QueryType.RANGE_NUMERIC_QUERY) {
	
							if (rangeNumericQueryParameterList == null || rangeNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
							
							String pairData[] = rangeNumericQueryParameterList.poll().trim().split(",");
	
							int ft_1 = Integer.parseInt(pairData[0]);
							int ft_2 = Integer.parseInt(pairData[1]);
	
							if (ft_2 > ft_1) {
								requestParams.add("q", "Int1_i:[" + ft_1 + " TO " + ft_2 + "]");
							} else {
								requestParams.add("q", "Int1_i:[" + ft_2 + " TO " + ft_1 + "]");
							}
	
						} else if (QueryClient.queryType == QueryType.SORTED_NUMERIC_QUERY) {
	
							if (sortedNumericQueryParameterList == null || sortedNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							String pairData[] = sortedNumericQueryParameterList.poll().trim().split(",");
	
							int ft_1 = Integer.parseInt(pairData[0]);
							int ft_2 = Integer.parseInt(pairData[1]);
	
							if (ft_2 > ft_1) {
								requestParams.add("q", "id:[" + ft_1 + " TO " + ft_2 + "]");
							} else {
								requestParams.add("q", "id:[" + ft_2 + " TO " + ft_1 + "]");
							}
	
							requestParams.add("sort", "Int1_i asc");
	
						} else if (QueryClient.queryType == QueryType.SORTED_TEXT_QUERY) {
	
							if (sortedNumericQueryParameterList == null || sortedNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							String pairData[] = sortedNumericQueryParameterList.poll().trim().split(",");
	
							int ft_1 = Integer.parseInt(pairData[0]);
							int ft_2 = Integer.parseInt(pairData[1]);
	
							if (ft_2 > ft_1) {
								requestParams.add("q", "id:[" + ft_1 + " TO " + ft_2 + "]");
							} else {
								requestParams.add("q", "id:[" + ft_2 + " TO " + ft_1 + "]");
							}
	
							requestParams.add("sort", "Text_s asc");
	
						} else if (QueryClient.queryType == QueryType.GREATER_THAN_NUMERIC_QUERY) {
							
							if (greaterNumericQueryParameterList == null || greaterNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
							
							requestParams.add("q", "Int1_i:[" + greaterNumericQueryParameterList.poll() + " TO *]");
						} else if (QueryClient.queryType == QueryType.LESS_THAN_NUMERIC_QUERY) {
							
							if (lesserNumericQueryParameterList == null || lesserNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
							
							requestParams.add("q", "Int1_i:[* TO " + lesserNumericQueryParameterList.poll() + "]");
						} else if (QueryClient.queryType == QueryType.AND_NUMERIC_QUERY) {
	
							if (andNumericQueryParameterList == null || andNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							String pairData[] = andNumericQueryParameterList.poll().trim().split(",");
	
							int ft_1 = Integer.parseInt(pairData[0]);
							int ft_2 = Integer.parseInt(pairData[1]);
	
							requestParams.add("q", "Int1_i:" + ft_1 + " AND Int2_i:" + ft_2);
	
						} else if (QueryClient.queryType == QueryType.OR_NUMERIC_QUERY) {
	
							if (orNumericQueryParameterList == null || orNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							String pairData[] = orNumericQueryParameterList.poll().trim().split(",");
	
							int ft_1 = Integer.parseInt(pairData[0]);
							int ft_2 = Integer.parseInt(pairData[1]);
	
							requestParams.add("q", "Int1_i:" + ft_1 + " OR Int2_i:" + ft_2);
	
						} else if (QueryClient.queryType == QueryType.TEXT_TERM_QUERY) {
							
							if (textTerms == null || textTerms.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
							
							requestParams.add("q", "Article_t:" + textTerms.poll().trim());
						} else if (QueryClient.queryType == QueryType.TEXT_PHRASE_QUERY) {
							
							if (textPhrases == null || textPhrases.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
							
							requestParams.add("q", "Title_t:" + textPhrases.poll().trim());
						} else if (QueryClient.queryType == QueryType.HIGHLIGHT_QUERY) {
	
							if (highlightTerms == null || highlightTerms.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							requestParams.add("hl", "on");
							requestParams.add("hl.fl", "Article_t");
							requestParams.add("q", "Article_t:" + highlightTerms.poll());
						} else if (QueryClient.queryType == QueryType.CLASSIC_TERM_FACETING) {
	
							if (greaterNumericQueryParameterList == null || greaterNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
							
							requestParams.add("q", "Int1_i:[" + greaterNumericQueryParameterList.poll() + " TO *]");
							requestParams.add("facet", "true");
							requestParams.add("facet.field", "Category_t");
						} else if (QueryClient.queryType == QueryType.JSON_TERM_FACETING) {
	
							if (greaterNumericQueryParameterList == null || greaterNumericQueryParameterList.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							requestParams.add("q", "Int1_i:[" + greaterNumericQueryParameterList.poll() + " TO *]");
							requestParams.add("facet", "true");
							requestParams.add("json.facet", "{categories:{ terms: { field : Category_t }}}");
						} else if (QueryClient.queryType == QueryType.CLASSIC_RANGE_FACETING) {
	
							if (rangeFacetRanges == null || rangeFacetRanges.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							String pairData[] = rangeFacetRanges.poll().trim().split(",");
							requestParams.add("q", "*:*");
							requestParams.add("facet", "true");
							requestParams.add("facet.range", "Int2_i");
							
							long ft_1 = Long.parseLong(pairData[0]) - 1;
							long ft_2 = Long.parseLong(pairData[1]) - 1;
	
							if (ft_2 > ft_1) {
								requestParams.add("f.Int2_i.facet.range.start", "" + ft_1);
								requestParams.add("f.Int2_i.facet.range.end", "" + ft_2);
								requestParams.add("f.Int2_i.facet.range.gap", pairData[2].trim());
	
							} else {
								requestParams.add("f.Int2_i.facet.range.start", "" + ft_2);
								requestParams.add("f.Int2_i.facet.range.end", "" + ft_1);
								requestParams.add("f.Int2_i.facet.range.gap", pairData[2].trim());
							}
							
						} else if (QueryClient.queryType == QueryType.JSON_RANGE_FACETING) {
	
							if (rangeFacetRanges == null || rangeFacetRanges.size() == 0) {
								logger.error("Queue exhausted, no more items left ...");
								throw new RuntimeException("Queue exhausted, no more items left ...");
							}
	
							String pairData[] = rangeFacetRanges.poll().trim().split(",");
							requestParams.add("q", "*:*");
							requestParams.add("facet", "true");
							
							long ft_1 = Long.parseLong(pairData[0]) - 1;
							long ft_2 = Long.parseLong(pairData[1]) - 1;
	
							if (ft_2 > ft_1) {
								requestParams.add("json.facet", "{ prices : { range : {field : Int2_i ,start : "+ ft_1 +", end : "+ ft_2 +", gap : "+ pairData[2].trim() +"}}}");
							} else {
								requestParams.add("json.facet", "{ prices : { range : {field : Int2_i ,start : "+ ft_2 +", end : "+ ft_1 +", gap : "+ pairData[2].trim() +"}}}");
							}
	
						}
	
						params = SolrParams.toSolrParams(requestParams);
						response = this.fireQuery(collectionName, params);

						setQueryCounter();
						elapsedTime = response.getElapsedTime();
						setTotalQTime(elapsedTime);
						setMinMaxQTime(elapsedTime);
						
						if (QueryClient.queryCount >= QueryClient.queryCountLimit) {
							 if (QueryClient.running != false) {
								
								QueryClient.running = false;
								endTime = System.currentTimeMillis();

								logger.debug("Ending State: | " + termNumericQueryParameterList.size() + "| "
										+ greaterNumericQueryParameterList.size() + "| " + lesserNumericQueryParameterList.size() + "| "
										+ andNumericQueryParameterList.size() + "| " + orNumericQueryParameterList.size() + "| "
										+ sortedNumericQueryParameterList.size() + "| " + rangeNumericQueryParameterList.size() + "| "
										+ textTerms.size() + "| " + textPhrases.size() + "| " + highlightTerms.size()
										+ "| " + rangeFacetRanges.size());
							 }
 						 	 logger.debug(this.toString() + " Getting out of critical section from check block ...");
							 break;
						}
					} else {
						// This is deliberately done to warm up document cache.
						requestParams.remove("q");
						requestParams.add("q", "*:*");
						params = SolrParams.toSolrParams(requestParams);
						response = solrClient.query(collectionName, params);
					}
					
				} catch (SolrServerException | IOException e) {
					setQueryFailureCount();
					logger.error(e.getMessage());
					throw new RuntimeException(e.getMessage());
				}

			} else if (running == false) {
				// Break out from loop ...				
				logger.debug("Ending State: | " + termNumericQueryParameterList.size() + "| "
						+ greaterNumericQueryParameterList.size() + "| " + lesserNumericQueryParameterList.size() + "| "
						+ andNumericQueryParameterList.size() + "| " + orNumericQueryParameterList.size() + "| "
						+ sortedNumericQueryParameterList.size() + "| " + rangeNumericQueryParameterList.size() + "| "
						+ textTerms.size() + "| " + textPhrases.size() + "| " + highlightTerms.size()
						+ "| " + rangeFacetRanges.size());
				
				logger.debug(this.toString() + " Getting out of critical section ...");
				break;
			}
		}
		
		if (solrClient != null) {			
			try {
				solrClient.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				throw new RuntimeException(e.getMessage());
			}
		}
		
        latch.countDown();
		return;
	}

	/**
	 * A method used by running threads to fire a query.
	 * 
	 * @param collectionName
	 * @param params
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private synchronized SolrResponse fireQuery(String collectionName, SolrParams params)
			throws SolrServerException, IOException {
		
		return solrClient.query(collectionName, params, METHOD.POST);
	}

	/**
	 * A method to count the number of queries executed by all the threads.
	 */
	private synchronized void setQueryCounter() {

		if (running == false) {
			return;
		}
		queryCount++;
	}

	/**
	 * A method used by threads to sum up the total qtime for all the queries.
	 * 
	 * @param qTime
	 */
	private synchronized void setTotalQTime(long qTime) {

		if (running == false) {
			return;
		}
		totalQTime += qTime;
	}

	/**
	 * A method used by the running threads to count the number of queries
	 * failing.
	 */
	private synchronized void setQueryFailureCount() {
		queryFailureCount++;
	}

	/**
	 * A method used by the threads to count the number of threads up and ready
	 * for running.
	 */
	private synchronized void setThreadReadyCount() {
		threadReadyCount++;
	}

	/**
	 * A method called by the running methods to compute the minumum and maximum
	 * qtime across all the queries fired.
	 * 
	 * @param QTime
	 */
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

	/**
	 * A method used to compute the nth percentile Qtime for all the queries
	 * fired by all the threads.
	 * 
	 * @param percentile
	 * @return
	 */
	public static double getNthPercentileQTime(double percentile) {

		if (!percentilesObjectCreated) {

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

	/**
	 * A method used to for reseting the static data variables to get ready for
	 * the next cycle.
	 */
	public static void reset() {

		termNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		greaterNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		lesserNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		rangeNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		andNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		orNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		sortedNumericQueryParameterList = new ConcurrentLinkedQueue<String>();
		textTerms = new ConcurrentLinkedQueue<String>();
		textPhrases = new ConcurrentLinkedQueue<String>();
		highlightTerms = new ConcurrentLinkedQueue<String>();
		rangeFacetRanges = new ConcurrentLinkedQueue<String>();
		running = false;
		queryCount = 0;
		minQtime = Long.MAX_VALUE;
		maxQtime = Long.MIN_VALUE;
		queryFailureCount = 0;
		threadReadyCount = 0;
		percentiles = null;
		percentilesObjectCreated = false;
		qTimePercentileList = new long[1000000];
		qTimePercentileListPointer = 0;
		totalQTime = 0;
		queryCountLimit = 0;
		startTime = 0;
		endTime = 0;
		startTimeLock = false;
		threadGroupStartTime = 0;
	}
}