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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

enum SolrClientType {
	HTTP_SOLR_CLIENT, CLOUD_SOLR_CLIENT, CONCURRENT_UPDATE_SOLR_CLIENT
};

enum ActionType {
	INDEX, PARTIAL_UPDATE
}

/**
 * This class provides the implementation for indexing client.
 * @author Vivek Narang
 *
 */
public class SolrIndexingClient {
	
	public final static Logger logger = Logger.getLogger(SolrIndexingClient.class);

	public static long documentCount;
	public static String solrCommitHistoryData;
	public static String amazonFoodData;
	public static String textDocumentLocation;
	@SuppressWarnings("unused")
	private String host;
	private String port;
	private String commitId;
	private Map<String, String> returnMetricMap;
	Random r = new Random();

	/**
	 * Constructor.
	 * 
	 * @param host
	 * @param port
	 * @param commitId
	 */
	public SolrIndexingClient(String host, String port, String commitId) {
		super();
		this.host = host;
		this.port = port;
		this.commitId = commitId;
	}

	/**
	 * A method used for indexing text data.
	 * 
	 * @param numDocuments
	 * @param urlString
	 * @param collectionName
	 * @param queueSize
	 * @param threadCount
	 * @param type
	 * @param captureMetrics
	 * @param deleteData
	 * @param clientType
	 * @param zookeeperIp
	 * @param zookeeperPort
	 * @return Map
	 * @throws Exception 
	 */
	@SuppressWarnings("deprecation")
	public Map<String, String> indexData(long numDocuments, String urlString, String collectionName, int queueSize,
			int threadCount, TestType type, boolean captureMetrics, boolean deleteData, SolrClientType clientType,
			String zookeeperIp, String zookeeperPort, ActionType action) throws Exception {

		documentCount = numDocuments;
		
		logger.info("Indexing documents through " + clientType + " with following parameters: Document Count:"
				+ numDocuments + ", Url:" + urlString + ", collectionName:" + collectionName + ", QueueSize:"
				+ queueSize + ", Threadcount:" + threadCount + ", TestType:" + type + ", Capture Metrics: "
				+ captureMetrics + ", Delete Data:" + deleteData + ", Zookeeper IP:" + zookeeperIp + ", Zookeeper Port:"
				+ zookeeperPort + ", Action Type:" + action);

		HttpSolrClient httpSolrClient = null;
		CloudSolrClient cloudSolrClient = null;
		ConcurrentUpdateSolrClient concurrentUpdateSolrClient = null;

		if (clientType == SolrClientType.HTTP_SOLR_CLIENT) {
			httpSolrClient = new HttpSolrClient.Builder(urlString).build();
		} else if (clientType == SolrClientType.CLOUD_SOLR_CLIENT) {
			cloudSolrClient = new CloudSolrClient.Builder().withZkHost(zookeeperIp + ":" + zookeeperPort).build();
			cloudSolrClient.connect();
			cloudSolrClient.setDefaultCollection(collectionName);
		} else if (clientType == SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT) {
			concurrentUpdateSolrClient = new ConcurrentUpdateSolrClient.Builder(urlString).withQueueSize(queueSize)
					.withThreadCount(threadCount).build();
		}

		int numberOfDocuments = 0;
		String line = "";
		String cvsSplitBy = ",";

		Thread thread = null;
		if (captureMetrics) {
			thread = new Thread(new MetricCollector(this.commitId, type, this.port));
			thread.start();
		}

		long end = 0;
		long start = System.currentTimeMillis();

		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {

				SolrInputDocument document = new SolrInputDocument();
				line.trim();

				String[] data = line.split(cvsSplitBy);
				
				if (action == ActionType.INDEX) {

					document.addField("id", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
					document.addField("Title_t", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
					document.addField("Article_t", data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
					document.addField("Category_t", data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
					document.addField("Int1_i", Integer.parseInt(data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
					document.addField("Int2_i", Integer.parseInt(data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
					document.addField("Float1_f", Float.parseFloat(data[6].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
					document.addField("Long1_l", Long.parseLong(data[7].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
					document.addField("Double1_d", Double.parseDouble(data[8].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
					document.addField("Text_s", data[9].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				
				} else if (action == ActionType.PARTIAL_UPDATE) {

					document.addField("id", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
					Map<String,Object> fieldModifier = new HashMap<>(1);
					fieldModifier.put("set", RandomStringUtils.randomAlphabetic(20));
					document.addField("Text_s", fieldModifier);
					
				}				
				
				if (clientType == SolrClientType.HTTP_SOLR_CLIENT) {
					httpSolrClient.add(document);
				} else if (clientType == SolrClientType.CLOUD_SOLR_CLIENT) {
					cloudSolrClient.add(collectionName, document);
				} else if (clientType == SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT) {
					concurrentUpdateSolrClient.add(collectionName, document);
				}

				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
			end = System.currentTimeMillis();

			logger.info("Committing the documents ...");

			if (clientType == SolrClientType.HTTP_SOLR_CLIENT) {
				httpSolrClient.commit(collectionName);
			} else if (clientType == SolrClientType.CLOUD_SOLR_CLIENT) {
				cloudSolrClient.commit(collectionName);
			} else if (clientType == SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT) {
				concurrentUpdateSolrClient.commit(collectionName);
			}

			if (deleteData) {
				logger.info("Deleting documents from index ...");

				if (clientType == SolrClientType.HTTP_SOLR_CLIENT) {
					httpSolrClient.deleteByQuery("*:*");
					httpSolrClient.commit(collectionName);
				} else if (clientType == SolrClientType.CLOUD_SOLR_CLIENT) {
					cloudSolrClient.deleteByQuery(collectionName, "*:*");
					cloudSolrClient.commit(collectionName);
				} else if (clientType == SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT) {
					concurrentUpdateSolrClient.deleteByQuery(collectionName, "*:*");
					concurrentUpdateSolrClient.commit(collectionName);
				}

				logger.info("Deleting documents from index [COMPLETE] ...");
			}

			logger.info("Closing the Solr connection ...");

			if (clientType == SolrClientType.HTTP_SOLR_CLIENT) {
				httpSolrClient.close();
			} else if (clientType == SolrClientType.CLOUD_SOLR_CLIENT) {
				cloudSolrClient.close();
			} else if (clientType == SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT) {
				concurrentUpdateSolrClient.shutdownNow();
				concurrentUpdateSolrClient.close();
			}

			logger.info("Time taken to index " + numberOfDocuments + " documents is: " + (double) (end - start)
					+ " millisecond(s)");

			br.close();
			returnMetricMap = new HashMap<String, String>();

			Date dNow = new Date();
			SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			returnMetricMap.put("TimeStamp", ft.format(dNow));
			returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
			returnMetricMap.put("IndexingTime", "" + (end - start));
			returnMetricMap.put("IndexingThroughput",
					"" + (double) numberOfDocuments / ((double) ((end - start) / 1000d)));
			returnMetricMap.put("ThroughputUnit", "doc/sec");
			returnMetricMap.put("CommitID", this.commitId);

			if (captureMetrics) {
				thread.stop();
			}

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		
		logger.info("Indexing documents COMPLETE ...");
		return returnMetricMap;
	}
}