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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.solr.tests.nightlybenchmarks.QueryClient.QueryType;

enum ConfigurationType {
	STANDALONE, CLOUD
}

public class Tests {

	public static SolrCloud cloud;
	public static SolrNode node;
	public static int queryThreadCount = Integer.parseInt(Util.QUERY_THREAD_COUNT);

	public static boolean createCollectionTestStandalone(String commitID) {

		try {
			SolrNode node = new SolrNode(commitID, "", "", false);

			node.doAction(SolrNodeAction.NODE_START);

			Util.getEnvironmentInformationFromMetricAPI(commitID, node.port);

			BenchmarkReportData.returnStandaloneCreateCollectionMap = node.createCollection("Core-" + UUID.randomUUID(),
					"Collection-" + UUID.randomUUID());

			node.doAction(SolrNodeAction.NODE_STOP);
			node.cleanup();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public static boolean indexingTestsStandalone(String commitID, long numDocuments) {

		try {
			SolrNode node = new SolrNode(commitID, "", "", false);

			node.doAction(SolrNodeAction.NODE_START);
			Util.getEnvironmentInformationFromMetricAPI(commitID, node.port);
			node.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());

			SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, commitID);
			BenchmarkReportData.metricMapStandalone = client.indexData(numDocuments,
					node.getBaseUrl() + node.collectionName, null, 0, 0, TestType.STANDALONE_INDEXING_THROUGHPUT_SERIAL,
					true, true, SolrClientType.HTTP_SOLR_CLIENT, null, null);

			node.doAction(SolrNodeAction.NODE_STOP);
			node.cleanup();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public static boolean indexingTestsStandaloneConcurrent(String commitID, long numDocuments) {

		try {
			SolrNode node = new SolrNode(commitID, "", "", false);
			node.doAction(SolrNodeAction.NODE_START);
			node.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());
			SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, commitID);

			String collectionName1 = "" + UUID.randomUUID();
			node.createCollection("Core-" + UUID.randomUUID(), collectionName1);
			BenchmarkReportData.metricMapStandaloneConcurrent1 = client.indexData(numDocuments, node.getBaseUrl(),
					collectionName1, 1000, 1, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_1, true, true,
					SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
			node.deleteCollection(collectionName1);
			String collectionName2 = "" + UUID.randomUUID();
			node.createCollection("Core-" + UUID.randomUUID(), collectionName2);
			BenchmarkReportData.metricMapStandaloneConcurrent2 = client.indexData(numDocuments, node.getBaseUrl(),
					collectionName2, 1000, 2, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_2, true, true,
					SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
			node.deleteCollection(collectionName2);
			String collectionName3 = "" + UUID.randomUUID();
			node.createCollection("Core-" + UUID.randomUUID(), collectionName3);
			BenchmarkReportData.metricMapStandaloneConcurrent3 = client.indexData(numDocuments, node.getBaseUrl(),
					collectionName3, 1000, 3, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_3, true, true,
					SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
			node.deleteCollection(collectionName3);

			node.doAction(SolrNodeAction.NODE_STOP);
			node.cleanup();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public static boolean indexingTestsCloudSerial(String commitID, long numDocuments, int nodes, String shards,
			String replicas) {

		Util.postMessage("** INITIATING TEST: Indexing Cloud Serial Nodes:" + nodes + " Shards:" + shards + " Replicas:"
				+ replicas, MessageType.PURPLE_TEXT, false);

		try {

			SolrCloud cloud = new SolrCloud(nodes, shards, replicas, commitID, null, "localhost", true);
			Tests.cloud = cloud;
			SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, commitID);

			if (nodes == 2 && shards == "1" && replicas == "2") {
				BenchmarkReportData.metricMapCloudSerial_2N1S2R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_2N1S2R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort);
			} else if (nodes == 2 && shards == "2" && replicas == "1") {
				BenchmarkReportData.metricMapCloudSerial_2N2S1R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_2N2S1R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort);
			} else if (nodes == 3 && shards == "1" && replicas == "3") {
				BenchmarkReportData.metricMapCloudSerial_3N1S3R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_3N1S3R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort);
			} else if (nodes == 4 && shards == "2" && replicas == "2") {
				BenchmarkReportData.metricMapCloudSerial_4N2S2R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_4N2S2R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort);
			}

			cloud.shutdown();
			BenchmarkReportData.returnCloudCreateCollectionMap = cloud.returnMapCreateCollection;

		} catch (Exception e) {
			e.printStackTrace();
		}

		Util.postMessage("** COMPLETING TEST: Indexing Cloud Serial Nodes:" + nodes + " Shards:" + shards + " Replicas:"
				+ replicas, MessageType.GREEN_TEXT, false);

		return true;
	}

	public static boolean indexingTestsCloudConcurrent(String commitID, long numDocuments, int nodes, String shards,
			String replicas) {

		Util.postMessage("** INITIATING TEST: Indexing Cloud Concurrent Nodes:" + nodes + " Shards:" + shards
				+ " Replicas:" + replicas, MessageType.PURPLE_TEXT, false);

		try {

			SolrCloud cloud = new SolrCloud(nodes, shards, replicas, commitID, null, "localhost", false);
			Tests.cloud = cloud;
			SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, commitID);

			if (nodes == 2 && shards == "1" && replicas == "2") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);

				BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, 1000, 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, 1000, 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, 1000, 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 2 && shards == "2" && replicas == "1") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, 1000, 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, 1000, 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, 1000, 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 3 && shards == "1" && replicas == "3") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, 1000, 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, 1000, 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, 1000, 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 4 && shards == "2" && replicas == "2") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, 1000, 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, 1000, 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, 1000, 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);
				cloud.deleteCollection(collectionName3);

			}

			cloud.shutdown();

		} catch (Exception e) {
			e.printStackTrace();
		}

		Util.postMessage("** COMPLETING TEST: Indexing Cloud Concurrent Nodes:" + nodes + " Shards:" + shards
				+ " Replicas:" + replicas, MessageType.GREEN_TEXT, false);

		return true;
	}

	private static Map<String, String> numericQueryTests(String commitID, QueryClient.QueryType queryType,
			int numberOfThreads, int estimationDuration, long delayEstimationBySeconds, String baseURL,
			String collectionName) {

		try {
			QueryClient.reset();
			QueryClient.prepare();

			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			LinkedList<QueryClient> list = new LinkedList<QueryClient>();

			for (int i = 0; i < numberOfThreads; i++) {
				QueryClient client = new QueryClient(baseURL, collectionName, queryType, numberOfThreads,
						delayEstimationBySeconds);
				list.add(client);
			}

			QueryClient.running = true;

			for (int i = 0; i < numberOfThreads; i++) {
				executorService.execute(list.get(i));
			}

			Thread.sleep(estimationDuration * 1000);

			QueryClient.running = false;

			executorService.shutdownNow();

			Thread.sleep(5000);

			Map<String, String> returnMap = new HashMap<String, String>();

			Date dNow = new Date();
			SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			returnMap.put("TimeStamp", ft.format(dNow));
			returnMap.put("CommitID", commitID);
			returnMap.put("TotalQueriesExecuted", "" + QueryClient.queryCount);
			returnMap.put("QueriesPerSecond",
					"" + (double) (QueryClient.queryCount / (QueryClient.totalQTime / 1000d)));
			returnMap.put("MinQTime", "" + QueryClient.minQtime);
			returnMap.put("MaxQTime", "" + QueryClient.maxQtime);
			returnMap.put("QueryFailureCount", "" + QueryClient.queryFailureCount);
			returnMap.put("TotalQTime", "" + QueryClient.totalQTime);
			returnMap.put("75thQtime", "" + QueryClient.getNthPercentileQTime(75));
			returnMap.put("95thQtime", "" + QueryClient.getNthPercentileQTime(95));
			returnMap.put("99thQtime", "" + QueryClient.getNthPercentileQTime(99));
			returnMap.put("99.9thQtime", "" + QueryClient.getNthPercentileQTime(99.9));

			Util.postMessage(returnMap.toString(), MessageType.RED_TEXT, false);
			QueryClient.reset();

			return returnMap;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private static String setUpCloudForFeatureTests(String commitID, long documentCount, int solrNodes, String shards,
			String replicas, int queueSize) throws InterruptedException {

		Util.postMessage("** Setting up cloud for feature tests ...", MessageType.PURPLE_TEXT, false);

		SolrCloud cloud = new SolrCloud(solrNodes, shards, replicas, commitID, null, "localhost", true);
		Tests.cloud = cloud;
		SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, commitID);

		cloudClient.indexData(documentCount, cloud.getuRL(), cloud.collectionName, queueSize, 2, null, false, false,
				SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null);

		return cloud.port;
	}

	private static String setUpStandaloneNodeForFeatureTests(String commitID, long numDocuments) {

		Util.postMessage("** Setting up standalone node for feature tests ...", MessageType.PURPLE_TEXT, false);

		try {
			SolrNode snode = new SolrNode(commitID, "", "", false);
			snode.doAction(SolrNodeAction.NODE_START);
			snode.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());

			SolrIndexingClient client = new SolrIndexingClient("localhost", snode.port, commitID);

			client.indexData(numDocuments, snode.getBaseUrl() + snode.collectionName, null, 0, 0, null, false, false,
					SolrClientType.HTTP_SOLR_CLIENT, null, null);

			node = snode;

			return node.port;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return "0";
	}

	private static void shutDownCloud() throws IOException, InterruptedException {

		Util.postMessage("** Shutting down cloud for feature tests ...", MessageType.PURPLE_TEXT, false);

		cloud.shutdown();
	}

	private static void shutDownStandalone() throws IOException, InterruptedException {

		Util.postMessage("** Shutting down standalone node for feature tests ...", MessageType.PURPLE_TEXT, false);
		node.deleteCollection(node.collectionName);
		node.doAction(SolrNodeAction.NODE_STOP);
		node.cleanup();
	}

	@SuppressWarnings("deprecation")
	public static void runNumericTestsCloud(long numDocuments) throws IOException, InterruptedException {

		Util.postMessage("** INITIATING TEST: Numeric query on cloud ...", MessageType.PURPLE_TEXT, false);

		String port = Tests.setUpCloudForFeatureTests(Util.COMMIT_ID, numDocuments, 2, "2", "1", 5000);

		Thread numericQueryTNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_CLOUD, port));
		numericQueryTNQMetricC.start();

		BenchmarkReportData.numericQueryTNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.TERM_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryTNQMetricC.stop();

		Thread numericQueryRNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_CLOUD, port));
		numericQueryRNQMetricC.start();

		BenchmarkReportData.numericQueryRNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.RANGE_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryRNQMetricC.stop();

		Thread numericQueryLNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_CLOUD, port));
		numericQueryLNQMetricC.start();

		BenchmarkReportData.numericQueryLNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.LESS_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryLNQMetricC.stop();

		Thread numericQueryGNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_CLOUD, port));
		numericQueryGNQMetricC.start();

		BenchmarkReportData.numericQueryGNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.GREATER_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryGNQMetricC.stop();

		Thread numericQueryANQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_CLOUD, port));
		numericQueryANQMetricC.start();

		BenchmarkReportData.numericQueryANQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.AND_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryANQMetricC.stop();

		Thread numericQueryONQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_CLOUD, port));
		numericQueryONQMetricC.start();

		BenchmarkReportData.numericQueryONQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.OR_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryONQMetricC.stop();
		
		Thread numericQuerySNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE, port));
		numericQuerySNQMetricC.start();

		BenchmarkReportData.numericQuerySNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.SORTED_NUMERIC_QUERY,
				queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQuerySNQMetricC.stop();
		
		Thread numericQueryTTQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_CLOUD, port));
		numericQueryTTQMetricC.start();
		
		BenchmarkReportData.numericQueryTTQMetricC = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TEXT_TERM_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryTTQMetricC.stop();		

		Thread numericQueryPTQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_CLOUD, port));
		numericQueryPTQMetricC.start();
		
		BenchmarkReportData.numericQueryPTQMetricC = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TEXT_PHRASE_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryPTQMetricC.stop();

		Tests.shutDownCloud();

	}

	@SuppressWarnings("deprecation")
	public static void runNumericQueryTestsStandalone(long numDocuments) throws IOException, InterruptedException {

		Util.postMessage("** INITIATING TEST: Numeric query on standalone node ...", MessageType.PURPLE_TEXT, false);

		String port = Tests.setUpStandaloneNodeForFeatureTests(Util.COMMIT_ID, numDocuments);

		Thread numericQueryTNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_STANDALONE, port));
		numericQueryTNQMetricS.start();

		BenchmarkReportData.numericQueryTNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryTNQMetricS.stop();

		Thread numericQueryRNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_STANDALONE, port));
		numericQueryRNQMetricS.start();

		BenchmarkReportData.numericQueryRNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryRNQMetricS.stop();

		Thread numericQueryLNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_STANDALONE, port));
		numericQueryLNQMetricS.start();

		BenchmarkReportData.numericQueryLNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryLNQMetricS.stop();

		Thread numericQueryGNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_STANDALONE, port));
		numericQueryGNQMetricS.start();

		BenchmarkReportData.numericQueryGNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryGNQMetricS.stop();

		Thread numericQueryANQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_STANDALONE, port));
		numericQueryANQMetricS.start();

		BenchmarkReportData.numericQueryANQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryANQMetricS.stop();

		Thread numericQueryONQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_STANDALONE, port));
		numericQueryONQMetricS.start();

		BenchmarkReportData.numericQueryONQMetricS = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.OR_NUMERIC_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryONQMetricS.stop();
		
		Thread numericQuerySNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE, port));
		numericQuerySNQMetricS.start();

		BenchmarkReportData.numericQuerySNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.SORTED_NUMERIC_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQuerySNQMetricS.stop();

		Thread numericQueryTTQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_STANDALONE, port));
		numericQueryTTQMetricS.start();
		
		BenchmarkReportData.numericQueryTTQMetricS = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TEXT_TERM_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryTTQMetricS.stop();		

		Thread numericQueryPTQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_STANDALONE, port));
		numericQueryPTQMetricS.start();
		
		BenchmarkReportData.numericQueryPTQMetricS = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TEXT_PHRASE_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryPTQMetricS.stop();

		
		Tests.shutDownStandalone();

	}

}