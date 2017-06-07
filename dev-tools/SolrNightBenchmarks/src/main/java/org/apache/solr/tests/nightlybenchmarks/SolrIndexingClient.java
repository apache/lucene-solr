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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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
	@SuppressWarnings("unused")
	private String collection;
	public static String solrCommitHistoryData;
	public static String amazonFoodDataLocation;
	private String commitId;
	Random r = new Random();

	public static List<Integer> intList = new LinkedList<Integer>();
	public static int documentCount;

	public SolrIndexingClient(String host, String port, String collection, String commitId) {
		super();
		this.host = host;
		this.port = port;
		this.collection = collection;
		this.commitId = commitId;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> indexAmazonFoodData(int numDocuments, String urlString) {

		documentCount = numDocuments;

		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.WHITE_TEXT, false);

		HttpSolrClient solrClient = new HttpSolrClient.Builder(urlString).build();

		long start = 0;
		long end = 0;
		int numberOfDocuments = 0;
		String line = "";
		String cvsSplitBy = ",";
		int value;

		Thread thread = new Thread(new MetricCollector(this.commitId, TestType.STANDALONE_INDEXING, this.port));
		thread.start();

		try (BufferedReader br = new BufferedReader(new FileReader(amazonFoodDataLocation))) {

			start = System.nanoTime();
			while ((line = br.readLine()) != null) {

				String[] foodData = line.split(cvsSplitBy);
				SolrInputDocument document = new SolrInputDocument();

				document.addField("Id", foodData[0]);
				document.addField("ProductId", foodData[1]);
				document.addField("UserId", foodData[2]);
				document.addField("ProfileName", foodData[3].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("HelpfulnessNumerator", foodData[4]);
				document.addField("HelpfulnessDenominator", foodData[5]);
				document.addField("Score", foodData[6]);
				document.addField("Time", foodData[7]);
				document.addField("Summary", foodData[8].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("Text", foodData[9].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("RandomLongField", r.nextLong());

				value = r.nextInt(Integer.MAX_VALUE);
				document.addField("RandomIntField", value);
				intList.add(value);

				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
			solrClient.commit();
			end = System.nanoTime();
			Util.postMessage("", MessageType.BLUE_TEXT, false);
			Util.postMessage("** Committing the documents ...", MessageType.WHITE_TEXT, false);

			Util.postMessage("** Closing the Solr connection ...", MessageType.GREEN_TEXT, false);
			solrClient.close();
			Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double) (end - start)
					+ " nanosecond(s)", MessageType.RED_TEXT, false);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		Map<String, String> returnMetricMap = new HashMap<String, String>();

		returnMetricMap.put("TimeStamp", Util.TEST_TIME);
		returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
		returnMetricMap.put("IndexingTime", "" + (double) ((double) (end - start) / (double) 1000000000));
		returnMetricMap.put("IndexingThroughput",
				"" + (double) numberOfDocuments / ((double) ((double) end - start) / (double) 1000000000));
		returnMetricMap.put("ThroughputUnit", "doc/sec");
		returnMetricMap.put("CommitID", this.commitId);

		thread.stop();

		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.WHITE_TEXT,
				false);
		return returnMetricMap;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> indexAmazonFoodData(int numDocuments, String urlString, String zookeeperIp,
			String zookeeperPort, String collectionName) {

		documentCount = numDocuments;
		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.WHITE_TEXT, false);

		CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(zookeeperIp + ":" + zookeeperPort)
				.build();
		solrClient.connect();
		solrClient.setDefaultCollection(collectionName);

		long start = 0;
		long end = 0;
		int numberOfDocuments = 0;
		String line = "";
		String cvsSplitBy = ",";
		int value;

		Thread thread = new Thread(new MetricCollector(this.commitId, TestType.CLOUD_INDEXING_REGULAR, this.port));
		thread.start();

		try (BufferedReader br = new BufferedReader(new FileReader(amazonFoodDataLocation))) {

			start = System.nanoTime();
			while ((line = br.readLine()) != null) {

				String[] foodData = line.split(cvsSplitBy);
				SolrInputDocument document = new SolrInputDocument();

				document.addField("Id", foodData[0]);
				document.addField("ProductId", foodData[1]);
				document.addField("UserId", foodData[2]);
				document.addField("ProfileName", foodData[3].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("HelpfulnessNumerator", foodData[4]);
				document.addField("HelpfulnessDenominator", foodData[5]);
				document.addField("Score", foodData[6]);
				document.addField("Time", foodData[7]);
				document.addField("Summary", foodData[8].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("Text", foodData[9].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("RandomLongField", r.nextLong());

				value = r.nextInt(Integer.MAX_VALUE);
				document.addField("RandomIntField", value);
				intList.add(value);

				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
			solrClient.commit();
			end = System.nanoTime();
			Util.postMessage("", MessageType.BLUE_TEXT, false);
			Util.postMessage("** Committing the documents ...", MessageType.WHITE_TEXT, false);

			Util.postMessage("** Closing the Solr connection ...", MessageType.GREEN_TEXT, false);
			solrClient.close();
			Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double) (end - start)
					+ " nanosecond(s)", MessageType.RED_TEXT, false);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		Map<String, String> returnMetricMap = new HashMap<String, String>();

		returnMetricMap.put("TimeStamp", Util.TEST_TIME);
		returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
		returnMetricMap.put("IndexingTime", "" + (double) ((double) (end - start) / (double) 1000000000));
		returnMetricMap.put("IndexingThroughput",
				"" + (double) numberOfDocuments / ((double) ((double) end - start) / (double) 1000000000));
		returnMetricMap.put("ThroughputUnit", "doc/sec");
		returnMetricMap.put("CommitID", this.commitId);

		thread.stop();

		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.WHITE_TEXT,
				false);
		return returnMetricMap;
	}

	@SuppressWarnings("deprecation")
	public Map<String, String> indexAmazonFoodData(int numDocuments, String urlString, String collectionName, int queueSize, int threadCount) {

		documentCount = numDocuments;

		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.WHITE_TEXT, false);

		ConcurrentUpdateSolrClient solrClient = new ConcurrentUpdateSolrClient(urlString, queueSize, threadCount);

		long start = 0;
		long end = 0;
		int numberOfDocuments = 0;
		String line = "";
		String cvsSplitBy = ",";
		int value;

		Thread thread = new Thread(new MetricCollector(this.commitId, TestType.CLOUD_INDEXING_CONCURRENT, this.port));
		thread.start();

		try (BufferedReader br = new BufferedReader(new FileReader(amazonFoodDataLocation))) {

			start = System.nanoTime();
			while ((line = br.readLine()) != null) {

				String[] foodData = line.split(cvsSplitBy);
				SolrInputDocument document = new SolrInputDocument();

				document.addField("Id", foodData[0]);
				document.addField("ProductId", foodData[1]);
				document.addField("UserId", foodData[2]);
				document.addField("ProfileName", foodData[3].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("HelpfulnessNumerator", foodData[4]);
				document.addField("HelpfulnessDenominator", foodData[5]);
				document.addField("Score", foodData[6]);
				document.addField("Time", foodData[7]);
				document.addField("Summary", foodData[8].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("Text", foodData[9].replaceAll("'", "").replaceAll("\"", ""));
				document.addField("RandomLongField", r.nextLong());

				value = r.nextInt(Integer.MAX_VALUE);
				document.addField("RandomIntField", value);
				intList.add(value);

				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}

			solrClient.blockUntilFinished();

			solrClient.commit();
			end = System.nanoTime();
			Util.postMessage("", MessageType.BLUE_TEXT, false);
			Util.postMessage("** Committing the documents ...", MessageType.WHITE_TEXT, false);

			Util.postMessage("** Closing the Solr connection ...", MessageType.GREEN_TEXT, false);
			solrClient.shutdownNow();
			solrClient.close();
			Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double) (end - start)
					+ " nanosecond(s)", MessageType.RED_TEXT, false);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		Map<String, String> returnMetricMap = new HashMap<String, String>();

		returnMetricMap.put("TimeStamp", Util.TEST_TIME);
		returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
		returnMetricMap.put("IndexingTime", "" + (double) ((double) (end - start) / (double) 1000000000));
		returnMetricMap.put("IndexingThroughput",
				"" + (double) numberOfDocuments / ((double) ((double) end - start) / (double) 1000000000));
		returnMetricMap.put("ThroughputUnit", "doc/sec");
		returnMetricMap.put("CommitID", this.commitId);

		thread.stop();

		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.WHITE_TEXT,
				false);
		return returnMetricMap;
	}

}
