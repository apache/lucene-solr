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

/**
 * 
 * @author Vivek Narang
 *
 */
public class Tests {

	public static SolrCloud cloud;
	public static SolrNode node;
	public static int queryThreadCount = Integer.parseInt(Util.QUERY_THREAD_COUNT);

	/**
	 * A collection creating test on standalone mode. 
	 * @param commitID
	 * @return boolean
	 */
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

	/**
	 * A method for testing indexing of test data on the solr standalone mode. 
	 * @param commitID
	 * @param numDocuments
	 * @return boolean
	 */
	public static boolean indexingTestsStandalone(String commitID, long numDocuments, ActionType action) {

		try {
			SolrNode node = new SolrNode(commitID, "", "", false);

			node.doAction(SolrNodeAction.NODE_START);
			Util.getEnvironmentInformationFromMetricAPI(commitID, node.port);
			node.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());

			SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, commitID);

			if (action == ActionType.INDEX) {
			
				BenchmarkReportData.metricMapIndexingStandalone = client.indexData(numDocuments,
						node.getBaseUrl() + node.collectionName, null, 0, 0, TestType.STANDALONE_INDEXING_THROUGHPUT_SERIAL,
						true, true, SolrClientType.HTTP_SOLR_CLIENT, null, null, ActionType.INDEX);

		    } else if (action == ActionType.PARTIAL_UPDATE) {
			
			    client.indexData(numDocuments,
							node.getBaseUrl() + node.collectionName, null, 0, 0, TestType.PARTIAL_UPDATE_THROUGHPUT_STANDALONE,
							false, false, SolrClientType.HTTP_SOLR_CLIENT, null, null, ActionType.INDEX);
			    	
			    BenchmarkReportData.metricMapPartialUpdateStandalone = client.indexData(numDocuments,
						node.getBaseUrl() + node.collectionName, null, 0, 0, TestType.PARTIAL_UPDATE_THROUGHPUT_STANDALONE,
						true, true, SolrClientType.HTTP_SOLR_CLIENT, null, null, ActionType.PARTIAL_UPDATE);

		    }

			node.doAction(SolrNodeAction.NODE_STOP);
			node.cleanup();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * A method for testing indexing of test data on the solr standalone mode using concurrent client.  
	 * @param commitID
	 * @param numDocuments
	 * @return boolean
	 */
	public static boolean indexingTestsStandaloneConcurrent(String commitID, long numDocuments, ActionType action) {

		try {
			
			if (action == ActionType.INDEX) {
			
				SolrNode node = new SolrNode(commitID, "", "", false);
				node.doAction(SolrNodeAction.NODE_START);
				node.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());
				SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, commitID);
	
				String collectionName1 = "" + UUID.randomUUID();
				node.createCollection("Core-" + UUID.randomUUID(), collectionName1);
				BenchmarkReportData.metricMapStandaloneIndexingConcurrent1 = client.indexData(numDocuments, node.getBaseUrl(),
						collectionName1, (int)Math.floor(numDocuments/100) , 1, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_1, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				node.deleteCollection(collectionName1);
				String collectionName2 = "" + UUID.randomUUID();
				node.createCollection("Core-" + UUID.randomUUID(), collectionName2);
				BenchmarkReportData.metricMapStandaloneIndexingConcurrent2 = client.indexData(numDocuments, node.getBaseUrl(),
						collectionName2, (int)Math.floor(numDocuments/100), 2, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_2, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				node.deleteCollection(collectionName2);
				String collectionName3 = "" + UUID.randomUUID();
				node.createCollection("Core-" + UUID.randomUUID(), collectionName3);
				BenchmarkReportData.metricMapStandaloneIndexingConcurrent3 = client.indexData(numDocuments, node.getBaseUrl(),
						collectionName3, (int)Math.floor(numDocuments/100), 3, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_3, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				node.deleteCollection(collectionName3);
	
				node.doAction(SolrNodeAction.NODE_STOP);
				node.cleanup();
			
			} else if (action == ActionType.PARTIAL_UPDATE) {
				
				SolrNode node = new SolrNode(commitID, "", "", false);
				node.doAction(SolrNodeAction.NODE_START);
				node.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());
				SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, commitID);
	
				String collectionName1 = "" + UUID.randomUUID();
				node.createCollection("Core-" + UUID.randomUUID(), collectionName1);
				client.indexData(numDocuments, node.getBaseUrl(),
						collectionName1, (int)Math.floor(numDocuments/100) , 1, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_1, false, false,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);

				BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1 = client.indexData(numDocuments, node.getBaseUrl(),
						collectionName1, (int)Math.floor(numDocuments/100) , 1, TestType.STANDALONE_PARTIAL_UPDATE_THROUGHPUT_CONCURRENT_1, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.PARTIAL_UPDATE);
				
				node.deleteCollection(collectionName1);
				
				String collectionName2 = "" + UUID.randomUUID();
				node.createCollection("Core-" + UUID.randomUUID(), collectionName2);
				client.indexData(numDocuments, node.getBaseUrl(),
						collectionName2, (int)Math.floor(numDocuments/100), 2, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_2, false, false,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);

				BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2 = client.indexData(numDocuments, node.getBaseUrl(),
						collectionName2, (int)Math.floor(numDocuments/100), 2, TestType.STANDALONE_PARTIAL_UPDATE_THROUGHPUT_CONCURRENT_2, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.PARTIAL_UPDATE);

				node.deleteCollection(collectionName2);
				
				String collectionName3 = "" + UUID.randomUUID();
				node.createCollection("Core-" + UUID.randomUUID(), collectionName3);
				client.indexData(numDocuments, node.getBaseUrl(),
						collectionName3, (int)Math.floor(numDocuments/100), 3, TestType.STANDALONE_INDEXING_THROUGHPUT_CONCURRENT_3, false, false,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);

				BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3 = client.indexData(numDocuments, node.getBaseUrl(),
						collectionName3, (int)Math.floor(numDocuments/100), 3, TestType.STANDALONE_PARTIAL_UPDATE_THROUGHPUT_CONCURRENT_3, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.PARTIAL_UPDATE);
				
				node.deleteCollection(collectionName3);
	
				node.doAction(SolrNodeAction.NODE_STOP);
				node.cleanup();

			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * A method for testing indexing throughput on solr cloud using serial client. 
	 * @param commitID
	 * @param numDocuments
	 * @param nodes
	 * @param shards
	 * @param replicas
	 * @return boolean
	 */
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
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort, ActionType.INDEX);
			} else if (nodes == 2 && shards == "2" && replicas == "1") {
				BenchmarkReportData.metricMapCloudSerial_2N2S1R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_2N2S1R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort, ActionType.INDEX);
			} else if (nodes == 3 && shards == "1" && replicas == "3") {
				BenchmarkReportData.metricMapCloudSerial_3N1S3R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_3N1S3R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort, ActionType.INDEX);
			} else if (nodes == 4 && shards == "2" && replicas == "2") {
				BenchmarkReportData.metricMapCloudSerial_4N2S2R = cloudClient.indexData(numDocuments, cloud.getuRL(),
						cloud.collectionName, 0, 0, TestType.CLOUD_INDEXING_THROUGHPUT_SERIAL_4N2S2R, true, true,
						SolrClientType.CLOUD_SOLR_CLIENT, cloud.zookeeperIp, cloud.zookeeperPort, ActionType.INDEX);
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

	/**
	 * A method for testing indexing throughput on solr cloud using concurrent update client. 
	 * @param commitID
	 * @param numDocuments
	 * @param nodes
	 * @param shards
	 * @param replicas
	 * @return Map
	 */
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
						cloud.getuRL(), collectionName1, (int)Math.floor(numDocuments/100), 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, (int)Math.floor(numDocuments/100), 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, (int)Math.floor(numDocuments/100), 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 2 && shards == "2" && replicas == "1") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, (int)Math.floor(numDocuments/100), 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, (int)Math.floor(numDocuments/100), 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, (int)Math.floor(numDocuments/100), 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 3 && shards == "1" && replicas == "3") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, (int)Math.floor(numDocuments/100), 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, (int)Math.floor(numDocuments/100), 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, (int)Math.floor(numDocuments/100), 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 4 && shards == "2" && replicas == "2") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName1, (int)Math.floor(numDocuments/100), 1,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_1T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName2, (int)Math.floor(numDocuments/100), 2,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_2T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R = cloudClient.indexData(numDocuments,
						cloud.getuRL(), collectionName3, (int)Math.floor(numDocuments/100), 3,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_3T, true, true,
						SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);
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

	/**
	 * A method for testing indexing throughput on solr cloud using a custom concurrent indexing client. 
	 * @param commitID
	 * @param numDocuments
	 * @param nodes
	 * @param shards
	 * @param replicas
	 * @return Map
	 */
	public static boolean indexingTestsCloudConcurrentCustomClient(String commitID, long numDocuments, int nodes,
			String shards, String replicas) {

		Util.postMessage("** INITIATING TEST: Indexing Cloud Concurrent Nodes:" + nodes + " Shards:" + shards
				+ " Replicas:" + replicas, MessageType.PURPLE_TEXT, false);

		try {

			SolrCloud cloud = new SolrCloud(nodes, shards, replicas, commitID, null, "localhost", false);
			Tests.cloud = cloud;

			if (nodes == 2 && shards == "1" && replicas == "2") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName1, 1, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_1T, cloud.port, true);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName2, 2, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_2T, cloud.port, true);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName3, 3, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N1S2R_3T, cloud.port, true);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 2 && shards == "2" && replicas == "1") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName1, 1, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_1T, cloud.port, true);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName2, 2, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_2T, cloud.port, true);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName3, 3, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_2N2S1R_3T, cloud.port, true);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 3 && shards == "1" && replicas == "3") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName1, 1, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_1T, cloud.port, true);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName2, 2, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_2T, cloud.port, true);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName3, 3, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_3N1S3R_3T, cloud.port, true);
				cloud.deleteCollection(collectionName3);

			} else if (nodes == 4 && shards == "2" && replicas == "2") {

				String collectionName1 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName1, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName1, 1, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_1T, cloud.port, true);
				cloud.deleteCollection(collectionName1);

				String collectionName2 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName2, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName2, 2, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_2T, cloud.port, true);
				cloud.deleteCollection(collectionName2);

				String collectionName3 = "" + UUID.randomUUID();
				cloud.createCollection(collectionName3, null, shards, replicas);
				BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R = Tests.cloudConcurrentIndexing(
						cloud.zookeeperIp + ":" + cloud.zookeeperPort, collectionName3, 3, 60, Util.COMMIT_ID,
						TestType.CLOUD_INDEXING_THROUGHPUT_CONCURRENT_4N2S2R_3T, cloud.port, true);
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

	/**
	 * A method used by the custom concurrent indexing client. 
	 * @param zookeeperURL
	 * @param collectionName
	 * @param numberOfThreads
	 * @param estimationDuration
	 * @param commitId
	 * @param type
	 * @param port
	 * @param captureMetrics
	 * @return Map
	 */
	@SuppressWarnings("deprecation")
	private static Map<String, String> cloudConcurrentIndexing(String zookeeperURL, String collectionName,
			int numberOfThreads, int estimationDuration, String commitId, TestType type, String port,
			boolean captureMetrics) {

		Util.postMessage("** Indexing documents through custom cloud concurrent client with parameters: Url:" + zookeeperURL
				+ ", Collection Name:" + collectionName + " Thread Count:" + numberOfThreads + " Estimation Duration:"
				+ estimationDuration + " seconds", MessageType.CYAN_TEXT, false);

		try {
			CloudConcurrentIndexingClient.reset();
			CloudConcurrentIndexingClient.prepare();

			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			LinkedList<CloudConcurrentIndexingClient> list = new LinkedList<CloudConcurrentIndexingClient>();

			for (int i = 0; i < numberOfThreads; i++) {
				CloudConcurrentIndexingClient client = new CloudConcurrentIndexingClient(zookeeperURL, collectionName);
				list.add(client);
			}

			Thread thread = null;
			if (captureMetrics) {
				thread = new Thread(new MetricCollector(commitId, type, port));
				thread.start();
			}

			CloudConcurrentIndexingClient.running = true;

			for (int i = 0; i < numberOfThreads; i++) {
				executorService.execute(list.get(i));
			}

			Thread.sleep(estimationDuration * 1000);

			CloudConcurrentIndexingClient.running = false;

			if (captureMetrics) {
				thread.stop();
			}

			for (int i = 0; i < numberOfThreads; i++) {
				list.get(i).closeAndCommit();
			}

			executorService.shutdownNow();

			Thread.sleep(5000);

			Map<String, String> returnMap = new HashMap<String, String>();

			Date dNow = new Date();
			SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

			returnMap.put("TimeStamp", ft.format(dNow));
			returnMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
			returnMap.put("IndexingTime", "" + CloudConcurrentIndexingClient.totalTime);
			returnMap.put("IndexingThroughput", "" + ((CloudConcurrentIndexingClient.documentCount)
					/ Math.floor(CloudConcurrentIndexingClient.totalTime / 1000d)));
			returnMap.put("ThroughputUnit", "doc/sec");
			returnMap.put("CommitID", Util.COMMIT_ID);

			Util.postMessage(returnMap.toString(), MessageType.RED_TEXT, false);
			CloudConcurrentIndexingClient.reset();

			return returnMap;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * A method implementing the querying tests. 
	 * @param commitID
	 * @param queryType
	 * @param numberOfThreads
	 * @param estimationDuration
	 * @param delayEstimationBySeconds
	 * @param baseURL
	 * @param collectionName
	 * @return Map
	 */
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

	/**
	 * A method used to set up a solr cloud instance for testing on the cloud mode. 
	 * @param commitID
	 * @param documentCount
	 * @param solrNodes
	 * @param shards
	 * @param replicas
	 * @param queueSize
	 * @return String
	 * @throws InterruptedException
	 */
	private static String setUpCloudForFeatureTests(String commitID, long documentCount, int solrNodes, String shards,
			String replicas, int queueSize) throws InterruptedException {

		Util.postMessage("** Setting up cloud for feature tests ...", MessageType.PURPLE_TEXT, false);

		SolrCloud cloud = new SolrCloud(solrNodes, shards, replicas, commitID, null, "localhost", true);
		Tests.cloud = cloud;
		SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, commitID);

		cloudClient.indexData(documentCount, cloud.getuRL(), cloud.collectionName, queueSize, 2, null, false, false,
				SolrClientType.CONCURRENT_UPDATE_SOLR_CLIENT, null, null, ActionType.INDEX);

		return cloud.port;
	}

	/**
	 * A method used for setting up solr in standalone mode for testing on the standalone mode. 
	 * @param commitID
	 * @param numDocuments
	 * @return String
	 */
	private static String setUpStandaloneNodeForFeatureTests(String commitID, long numDocuments) {

		Util.postMessage("** Setting up standalone node for feature tests ...", MessageType.PURPLE_TEXT, false);

		try {
			SolrNode snode = new SolrNode(commitID, "", "", false);
			snode.doAction(SolrNodeAction.NODE_START);
			snode.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());

			SolrIndexingClient client = new SolrIndexingClient("localhost", snode.port, commitID);

			client.indexData(numDocuments, snode.getBaseUrl() + snode.collectionName, null, 0, 0, null, false, false,
					SolrClientType.HTTP_SOLR_CLIENT, null, null, ActionType.INDEX);

			node = snode;

			return node.port;

		} catch (Exception e) {
			e.printStackTrace();
		}

		return "0";
	}

	/**
	 * A method used for shutting down the solr cloud instance. 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void shutDownCloud() throws IOException, InterruptedException {

		Util.postMessage("** Shutting down cloud for feature tests ...", MessageType.PURPLE_TEXT, false);

		cloud.shutdown();
	}

	/**
	 * A method used for shutting down the solr standalone mode instance. 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static void shutDownStandalone() throws IOException, InterruptedException {

		Util.postMessage("** Shutting down standalone node for feature tests ...", MessageType.PURPLE_TEXT, false);
		node.deleteCollection(node.collectionName);
		node.doAction(SolrNodeAction.NODE_STOP);
		node.cleanup();
	}

	/**
	 * A method used for setting up various configuration to be tested on cloud mode. 
	 * @param numDocuments
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("deprecation")
	public static void queryTestsCloud(long numDocuments) throws IOException, InterruptedException {

		Util.postMessage("** INITIATING TEST: Numeric query on cloud ...", MessageType.PURPLE_TEXT, false);

		String port = Tests.setUpCloudForFeatureTests(Util.COMMIT_ID, numDocuments, 2, "2", "1", 5000);

		Thread numericQueryTNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_CLOUD, port));
		numericQueryTNQMetricC.start();

		BenchmarkReportData.queryTNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.TERM_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryTNQMetricC.stop();

		Thread numericQueryRNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_CLOUD, port));
		numericQueryRNQMetricC.start();

		BenchmarkReportData.queryRNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.RANGE_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryRNQMetricC.stop();

		Thread numericQueryLNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_CLOUD, port));
		numericQueryLNQMetricC.start();

		BenchmarkReportData.queryLNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.LESS_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryLNQMetricC.stop();

		Thread numericQueryGNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_CLOUD, port));
		numericQueryGNQMetricC.start();

		BenchmarkReportData.queryGNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.GREATER_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryGNQMetricC.stop();

		Thread numericQueryANQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_CLOUD, port));
		numericQueryANQMetricC.start();

		BenchmarkReportData.queryANQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.AND_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryANQMetricC.stop();

		Thread numericQueryONQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_CLOUD, port));
		numericQueryONQMetricC.start();

		BenchmarkReportData.queryONQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryClient.QueryType.OR_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryONQMetricC.stop();

		Thread numericQuerySNQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE, port));
		numericQuerySNQMetricC.start();

		BenchmarkReportData.querySNQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQuerySNQMetricC.stop();

		Thread numericQueryTTQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_CLOUD, port));
		numericQueryTTQMetricC.start();

		BenchmarkReportData.queryTTQMetricC = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TEXT_TERM_QUERY,
				queryThreadCount, 120, 30,  Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryTTQMetricC.stop();

		Thread numericQueryPTQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_CLOUD, port));
		numericQueryPTQMetricC.start();

		BenchmarkReportData.queryPTQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, queryThreadCount, 120, 30,  Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryPTQMetricC.stop();
		
		Thread numericQuerySTQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_CLOUD, port));
		numericQuerySTQMetricC.start();

		BenchmarkReportData.querySTQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, queryThreadCount, 120, 30,  Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQuerySTQMetricC.stop();
		
		Thread numericQueryHTQMetricC = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_CLOUD, port));
		numericQueryHTQMetricC.start();

		BenchmarkReportData.queryHTQMetricC = Tests.numericQueryTests(Util.COMMIT_ID,QueryType.HIGHLIGHT_QUERY, 
				queryThreadCount, 120, 30, Tests.cloud.getuRL(), Tests.cloud.collectionName);

		numericQueryHTQMetricC.stop();

		Tests.shutDownCloud();

	}

	/**
	 * A method used for setting up configurations to be tested on solr standalone mode. 
	 * @param numDocuments
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("deprecation")
	public static void queryTestsStandalone(long numDocuments) throws IOException, InterruptedException {

		Util.postMessage("** INITIATING TEST: Numeric query on standalone node ...", MessageType.PURPLE_TEXT, false);

		String port = Tests.setUpStandaloneNodeForFeatureTests(Util.COMMIT_ID, numDocuments);

		Thread numericQueryTNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_STANDALONE, port));
		numericQueryTNQMetricS.start();

		BenchmarkReportData.queryTNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryTNQMetricS.stop();

		Thread numericQueryRNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_STANDALONE, port));
		numericQueryRNQMetricS.start();

		BenchmarkReportData.queryRNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryRNQMetricS.stop();

		Thread numericQueryLNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_STANDALONE, port));
		numericQueryLNQMetricS.start();

		BenchmarkReportData.queryLNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryLNQMetricS.stop();

		Thread numericQueryGNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_STANDALONE, port));
		numericQueryGNQMetricS.start();

		BenchmarkReportData.queryGNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryGNQMetricS.stop();

		Thread numericQueryANQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_STANDALONE, port));
		numericQueryANQMetricS.start();

		BenchmarkReportData.queryANQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryANQMetricS.stop();

		Thread numericQueryONQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_STANDALONE, port));
		numericQueryONQMetricS.start();

		BenchmarkReportData.queryONQMetricS = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.OR_NUMERIC_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryONQMetricS.stop();

		Thread numericQuerySNQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE, port));
		numericQuerySNQMetricS.start();

		BenchmarkReportData.querySNQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQuerySNQMetricS.stop();

		Thread numericQueryTTQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_STANDALONE, port));
		numericQueryTTQMetricS.start();

		BenchmarkReportData.queryTTQMetricS = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TEXT_TERM_QUERY,
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryTTQMetricS.stop();

		Thread numericQueryPTQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_STANDALONE, port));
		numericQueryPTQMetricS.start();

		BenchmarkReportData.queryPTQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryPTQMetricS.stop();

		Thread numericQuerySTQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_STANDALONE, port));
		numericQuerySTQMetricS.start();

		BenchmarkReportData.querySTQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, queryThreadCount, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQuerySTQMetricS.stop();
		
		Thread numericQueryHTQMetricS = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_STANDALONE, port));
		numericQueryHTQMetricS.start();

		BenchmarkReportData.queryHTQMetricS = Tests.numericQueryTests(Util.COMMIT_ID,QueryType.HIGHLIGHT_QUERY, 
				queryThreadCount, 120, 30, Tests.node.getBaseUrl(), Tests.node.collectionName);

		numericQueryHTQMetricS.stop();

		Tests.shutDownStandalone();
	}
}