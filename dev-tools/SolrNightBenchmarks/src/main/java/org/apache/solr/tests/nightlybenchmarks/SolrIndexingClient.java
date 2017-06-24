package org.apache.solr.tests.nightlybenchmarks;

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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexingClient {

	public enum SolrClientType {
		HTTP_SOLR_CLIENT, CLOUD_SOLR_CLIENT, CONCURRENT_UPDATE_SOLR_CLIENT
	};

	@SuppressWarnings("unused")
	private String host;
	private String port;
	public static String solrCommitHistoryData;
	public static String amazonFoodData;
	public static String textDocumentLocation;
	private String commitId;
	Random r = new Random();

	public static int documentCount;

	public SolrIndexingClient(String host, String port, String commitId) {
		super();
		this.host = host;
		this.port = port;
		this.commitId = commitId;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> indexData(int numDocuments, String urlString, boolean captureMetrics, boolean deleteData) {

		documentCount = numDocuments;
		
		Util.postMessage("** Indexing documents through HTTP client ..." + urlString , MessageType.CYAN_TEXT, false);

		HttpSolrClient solrClient = new HttpSolrClient.Builder(urlString).build();

		int numberOfDocuments = 0;
		String line = "";
		String cvsSplitBy = ",";

		Thread thread = null;
		if (captureMetrics) {
			thread = new Thread(new MetricCollector(this.commitId, TestType.STANDALONE_INDEXING_THROUGHPUT_SERIAL, this.port));
			thread.start();
		}

		long end = 0;
		long start = System.currentTimeMillis();
		
		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {
	
				SolrInputDocument document = new SolrInputDocument();				
				line.trim();
				
				String[] data = line.split(cvsSplitBy);

				document.addField("ID", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text1", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Int1", Integer.parseInt(data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Long1", Long.parseLong(data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Category1", data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text2", data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				
				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
			end = System.currentTimeMillis();

			solrClient.commit();
			Util.postMessage("** Committing the documents ...", MessageType.WHITE_TEXT, false);
			
			if (deleteData) {
			Util.postMessage("** DELETE ...", MessageType.WHITE_TEXT, false);
			solrClient.deleteByQuery("*:*");
			solrClient.commit();
			Util.postMessage("** DELETE COMPLETE ...", MessageType.WHITE_TEXT, false);
			}
			
			Util.postMessage("** Closing the Solr connection ...", MessageType.GREEN_TEXT, false);
			solrClient.close();
			Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double) (end - start)
					+ " millisecond(s)", MessageType.RED_TEXT, false);
			
			br.close();


		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		Map<String, String> returnMetricMap = new HashMap<String, String>();

		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		returnMetricMap.put("TimeStamp", ft.format(dNow));
		returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
		returnMetricMap.put("IndexingTime", "" + (end - start));
		returnMetricMap.put("IndexingThroughput",
				"" + (double) numberOfDocuments / ((double) ((end - start)/1000d)));
		returnMetricMap.put("ThroughputUnit", "doc/sec");
		returnMetricMap.put("CommitID", this.commitId);

		if (captureMetrics) {
			thread.stop();
		}
		
		Util.postMessage("** Indexing documents COMPLETE ...", MessageType.GREEN_TEXT,
				false);
		return returnMetricMap;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> indexData(int numDocuments, String urlString, String zookeeperIp,
			String zookeeperPort, String collectionName, boolean captureMetrics, TestType type, boolean deleteData) {

		Util.postMessage("** Indexing documents through cloud client ..." + urlString + " | " + collectionName , MessageType.CYAN_TEXT, false);

		documentCount = numDocuments;

		CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(zookeeperIp + ":" + zookeeperPort)
				.build();
		solrClient.connect();
		solrClient.setDefaultCollection(collectionName);

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

				document.addField("ID", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text1", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Int1", Integer.parseInt(data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Long1", Long.parseLong(data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Category1", data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text2", data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				
				solrClient.add(collectionName, document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
			end = System.currentTimeMillis();
			
			solrClient.commit();
			Util.postMessage("** Committing the documents ...", MessageType.WHITE_TEXT, false);

			if (deleteData) {
			Util.postMessage("** DELETE ...", MessageType.WHITE_TEXT, false);
			solrClient.deleteByQuery(collectionName,"*:*");
			solrClient.commit(collectionName);
			Util.postMessage("** DELETE COMPLETE ...", MessageType.WHITE_TEXT, false);
			}
			
			Util.postMessage("** Closing the Solr connection ...", MessageType.GREEN_TEXT, false);
			solrClient.close();
			Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double) (end - start)
					+ " millisecond(s)", MessageType.RED_TEXT, false);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		Map<String, String> returnMetricMap = new HashMap<String, String>();

		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		returnMetricMap.put("TimeStamp", ft.format(dNow));
		returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
		returnMetricMap.put("IndexingTime", "" + (double)(end - start));
		returnMetricMap.put("IndexingThroughput",
				"" + (double) numberOfDocuments / ((double)((end - start)/1000d)));
		returnMetricMap.put("ThroughputUnit", "doc/sec");
		returnMetricMap.put("CommitID", this.commitId);

		if (captureMetrics) {
			thread.stop();
		}
		
		Util.postMessage(returnMetricMap.toString(), MessageType.YELLOW_TEXT, false);
		
		Util.postMessage("** Indexing documents COMPLETE ...", MessageType.GREEN_TEXT,
				false);
		return returnMetricMap;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> indexData(int numDocuments, String urlString, String collectionName, int queueSize, int threadCount, TestType type, boolean captureMetrics, boolean deleteData) throws InterruptedException {
		
		documentCount = numDocuments;
		LinkedList<SolrInputDocument> docList = new LinkedList<SolrInputDocument>();

		Util.postMessage("** Indexing documents through concurrent client ..." + urlString + " | " + collectionName + " | " + queueSize + " | " + threadCount , MessageType.CYAN_TEXT, false);
		
		ConcurrentUpdateSolrClient solrClient = new ConcurrentUpdateSolrClient.Builder(urlString).withQueueSize(queueSize).withThreadCount(threadCount).build();

		int numberOfDocuments = 0;
		String line = "";
		String cvsSplitBy = ",";

		Thread thread = null;
		if (captureMetrics) {
			thread = new Thread(new MetricCollector(this.commitId, type, this.port));
			thread.start();
		}
		
		long end = 0;
		long start = 0;

		Util.postMessage("** Indexing the documents ...", MessageType.WHITE_TEXT, false);

		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {
				
				SolrInputDocument document = new SolrInputDocument();				
				line.trim();
				
				String[] data = line.split(cvsSplitBy);

				document.addField("ID", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text1", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Int1", Integer.parseInt(data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Long1", Long.parseLong(data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Category1", data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text2", data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				
				docList.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
			
			int length  = docList.size();
			start = System.currentTimeMillis();
			for (int i = 0 ; i < length; i ++) {
				solrClient.add(collectionName, docList.get(i));
			}
			//solrClient.blockUntilFinished();
			end =  System.currentTimeMillis();

			Util.postMessage("** Committing the documents ...", MessageType.WHITE_TEXT, false);
			solrClient.commit(collectionName);

			
			if (deleteData) {
			Util.postMessage("** DELETE ...", MessageType.WHITE_TEXT, false);
			solrClient.deleteByQuery(collectionName,"*:*");
			solrClient.commit(collectionName);
			Util.postMessage("** DELETE COMPLETE ...", MessageType.WHITE_TEXT, false);
			}
			
			Util.postMessage("** Closing the Solr connection ...", MessageType.GREEN_TEXT, false);
			solrClient.shutdownNow();
			solrClient.close();
			Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (end - start)
					+ " millisecond(s)", MessageType.RED_TEXT, false);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		Map<String, String> returnMetricMap = new HashMap<String, String>();

		Date dNow = new Date();
		SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		returnMetricMap.put("TimeStamp", ft.format(dNow));
		returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
		returnMetricMap.put("IndexingTime", "" + (end - start));
		returnMetricMap.put("IndexingThroughput",
				"" + (double)numberOfDocuments /((double)(end - start)/1000d));
		returnMetricMap.put("ThroughputUnit", "doc/sec");
		returnMetricMap.put("CommitID", this.commitId);

		if (captureMetrics) {
			thread.stop();
		}
		Util.postMessage(returnMetricMap.toString(), MessageType.YELLOW_TEXT, false);

		Util.postMessage("** Indexing documents COMPLETE ...", MessageType.GREEN_TEXT,
				false);
		return returnMetricMap;
	}
	
}
