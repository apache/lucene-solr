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
			String numberOfThreads_, int estimationDuration, long delayEstimationBySeconds, String baseURL,
			String collectionName) {

			int numberOfThreads = Integer.parseInt(numberOfThreads_);
		try {
			QueryClient.reset();
			QueryClient.queryType = queryType;
			QueryClient.prepare();

			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			LinkedList<QueryClient> list = new LinkedList<QueryClient>();

			for (int i = 0; i < numberOfThreads; i++) {
				QueryClient client = new QueryClient(baseURL, collectionName, numberOfThreads,
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
					"" + ((float)QueryClient.queryCount / (float)(estimationDuration)));
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
		
		Thread numericQueryTNQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_CLOUD_2T, port));
		numericQueryTNQMetricC1.start();

		BenchmarkReportData.queryTNQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryTNQMetricC1.stop();

		Thread numericQueryTNQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_CLOUD_4T, port));
		numericQueryTNQMetricC2.start();

		BenchmarkReportData.queryTNQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryTNQMetricC2.stop();

		Thread numericQueryTNQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_CLOUD_6T, port));
		numericQueryTNQMetricC3.start();
		
		BenchmarkReportData.queryTNQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);

		numericQueryTNQMetricC3.stop();

		Thread numericQueryTNQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_CLOUD_8T, port));
		numericQueryTNQMetricC4.start();
		
		BenchmarkReportData.queryTNQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
	
		numericQueryTNQMetricC4.stop();
		
		
		
		
		Thread numericQueryRNQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_CLOUD_2T, port));
		numericQueryRNQMetricC1.start();

		BenchmarkReportData.queryRNQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryRNQMetricC1.stop();

		Thread numericQueryRNQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_CLOUD_4T, port));
		numericQueryRNQMetricC2.start();

		BenchmarkReportData.queryRNQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryRNQMetricC2.stop();

		Thread numericQueryRNQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_CLOUD_6T, port));
		numericQueryRNQMetricC3.start();

		BenchmarkReportData.queryRNQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryRNQMetricC3.stop();

		Thread numericQueryRNQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_CLOUD_8T, port));
		numericQueryRNQMetricC4.start();

		BenchmarkReportData.queryRNQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryRNQMetricC4.stop();
		
		
		
		
		Thread numericQueryLNQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_CLOUD_2T, port));
		numericQueryLNQMetricC1.start();

		BenchmarkReportData.queryLNQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryLNQMetricC1.stop();

		Thread numericQueryLNQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_CLOUD_4T, port));
		numericQueryLNQMetricC2.start();

		BenchmarkReportData.queryLNQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryLNQMetricC2.stop();

		Thread numericQueryLNQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_CLOUD_6T, port));
		numericQueryLNQMetricC3.start();

		BenchmarkReportData.queryLNQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryLNQMetricC3.stop();

		Thread numericQueryLNQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_CLOUD_8T, port));
		numericQueryLNQMetricC4.start();

		BenchmarkReportData.queryLNQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryLNQMetricC4.stop();
		
		
		
		
		Thread numericQueryGNQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_CLOUD_2T, port));
		numericQueryGNQMetricC1.start();

		BenchmarkReportData.queryGNQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryGNQMetricC1.stop();

		Thread numericQueryGNQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_CLOUD_4T, port));
		numericQueryGNQMetricC2.start();

		BenchmarkReportData.queryGNQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryGNQMetricC2.stop();

		Thread numericQueryGNQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_CLOUD_6T, port));
		numericQueryGNQMetricC3.start();

		BenchmarkReportData.queryGNQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryGNQMetricC3.stop();

		Thread numericQueryGNQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_CLOUD_8T, port));
		numericQueryGNQMetricC4.start();

		BenchmarkReportData.queryGNQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryGNQMetricC4.stop();
		
		
		
		
		Thread numericQueryANQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_CLOUD_2T, port));
		numericQueryANQMetricC1.start();

		BenchmarkReportData.queryANQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryANQMetricC1.stop();

		Thread numericQueryANQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_CLOUD_4T, port));
		numericQueryANQMetricC2.start();

		BenchmarkReportData.queryANQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryANQMetricC2.stop();

		Thread numericQueryANQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_CLOUD_6T, port));
		numericQueryANQMetricC3.start();

		BenchmarkReportData.queryANQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryANQMetricC3.stop();

		Thread numericQueryANQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_CLOUD_8T, port));
		numericQueryANQMetricC4.start();

		BenchmarkReportData.queryANQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryANQMetricC4.stop();
		
		
		
		
		Thread numericQueryONQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_CLOUD_2T, port));
		numericQueryONQMetricC1.start();

		BenchmarkReportData.queryONQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryONQMetricC1.stop();

		Thread numericQueryONQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_CLOUD_4T, port));
		numericQueryONQMetricC2.start();

		BenchmarkReportData.queryONQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryONQMetricC2.stop();

		Thread numericQueryONQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_CLOUD_6T, port));
		numericQueryONQMetricC3.start();

		BenchmarkReportData.queryONQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryONQMetricC3.stop();

		Thread numericQueryONQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_CLOUD_8T, port));
		numericQueryONQMetricC4.start();

		BenchmarkReportData.queryONQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryONQMetricC4.stop();
		
		
		
		
		Thread numericQuerySNQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_CLOUD_2T, port));
		numericQuerySNQMetricC1.start();

		BenchmarkReportData.querySNQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySNQMetricC1.stop();

		Thread numericQuerySNQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_CLOUD_4T, port));
		numericQuerySNQMetricC2.start();

		BenchmarkReportData.querySNQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySNQMetricC2.stop();

		Thread numericQuerySNQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_CLOUD_6T, port));
		numericQuerySNQMetricC3.start();

		BenchmarkReportData.querySNQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySNQMetricC3.stop();

		Thread numericQuerySNQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_CLOUD_8T, port));
		numericQuerySNQMetricC4.start();

		BenchmarkReportData.querySNQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySNQMetricC4.stop();
		
		
		
		
		Thread numericQueryTTQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_CLOUD_2T, port));
		numericQueryTTQMetricC1.start();

		BenchmarkReportData.queryTTQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryTTQMetricC1.stop();

		Thread numericQueryTTQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_CLOUD_4T, port));
		numericQueryTTQMetricC2.start();

		BenchmarkReportData.queryTTQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryTTQMetricC2.stop();

		Thread numericQueryTTQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_CLOUD_6T, port));
		numericQueryTTQMetricC3.start();

		BenchmarkReportData.queryTTQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryTTQMetricC3.stop();

		Thread numericQueryTTQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_CLOUD_8T, port));
		numericQueryTTQMetricC4.start();

		BenchmarkReportData.queryTTQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryTTQMetricC4.stop();
		
		
		
		
		Thread numericQueryPTQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_CLOUD_2T, port));
		numericQueryPTQMetricC1.start();

		BenchmarkReportData.queryPTQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryPTQMetricC1.stop();

		Thread numericQueryPTQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_CLOUD_4T, port));
		numericQueryPTQMetricC2.start();

		BenchmarkReportData.queryPTQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryPTQMetricC2.stop();

		Thread numericQueryPTQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_CLOUD_6T, port));
		numericQueryPTQMetricC3.start();

		BenchmarkReportData.queryPTQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryPTQMetricC3.stop();

		Thread numericQueryPTQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_CLOUD_8T, port));
		numericQueryPTQMetricC4.start();

		BenchmarkReportData.queryPTQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryPTQMetricC4.stop();
		
		
		
		
		Thread numericQuerySTQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_CLOUD_2T, port));
		numericQuerySTQMetricC1.start();

		BenchmarkReportData.querySTQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySTQMetricC1.stop();

		Thread numericQuerySTQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_CLOUD_4T, port));
		numericQuerySTQMetricC2.start();

		BenchmarkReportData.querySTQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySTQMetricC2.stop();

		Thread numericQuerySTQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_CLOUD_6T, port));
		numericQuerySTQMetricC3.start();

		BenchmarkReportData.querySTQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySTQMetricC3.stop();

		Thread numericQuerySTQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_CLOUD_8T, port));
		numericQuerySTQMetricC4.start();

		BenchmarkReportData.querySTQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQuerySTQMetricC4.stop();
		
		
		
		
		Thread numericQueryHTQMetricC1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_CLOUD_2T, port));
		numericQueryHTQMetricC1.start();

		BenchmarkReportData.queryHTQMetricC_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryHTQMetricC1.stop();

		Thread numericQueryHTQMetricC2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_CLOUD_4T, port));
		numericQueryHTQMetricC2.start();

		BenchmarkReportData.queryHTQMetricC_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryHTQMetricC2.stop();

		Thread numericQueryHTQMetricC3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_CLOUD_6T, port));
		numericQueryHTQMetricC3.start();

		BenchmarkReportData.queryHTQMetricC_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryHTQMetricC3.stop();

		Thread numericQueryHTQMetricC4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_CLOUD_8T, port));
		numericQueryHTQMetricC4.start();

		BenchmarkReportData.queryHTQMetricC_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.cloud.getuRL(),
				Tests.cloud.collectionName);
		
		numericQueryHTQMetricC4.stop();

		
		
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

		Thread numericQueryTNQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQueryTNQMetricS1.start();

		BenchmarkReportData.queryTNQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryTNQMetricS1.stop();

		Thread numericQueryTNQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQueryTNQMetricS2.start();

		BenchmarkReportData.queryTNQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryTNQMetricS2.stop();

		Thread numericQueryTNQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQueryTNQMetricS3.start();
		
		BenchmarkReportData.queryTNQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);

		numericQueryTNQMetricS3.stop();

		Thread numericQueryTNQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TERM_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQueryTNQMetricS4.start();
		
		BenchmarkReportData.queryTNQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
	
		numericQueryTNQMetricS4.stop();
		
		
		
		
		Thread numericQueryRNQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQueryRNQMetricS1.start();

		BenchmarkReportData.queryRNQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryRNQMetricS1.stop();

		Thread numericQueryRNQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQueryRNQMetricS2.start();

		BenchmarkReportData.queryRNQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryRNQMetricS2.stop();

		Thread numericQueryRNQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQueryRNQMetricS3.start();

		BenchmarkReportData.queryRNQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryRNQMetricS3.stop();

		Thread numericQueryRNQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.RANGE_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQueryRNQMetricS4.start();

		BenchmarkReportData.queryRNQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryRNQMetricS4.stop();
		
		
		
		
		Thread numericQueryLNQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQueryLNQMetricS1.start();

		BenchmarkReportData.queryLNQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryLNQMetricS1.stop();

		Thread numericQueryLNQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQueryLNQMetricS2.start();

		BenchmarkReportData.queryLNQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryLNQMetricS2.stop();

		Thread numericQueryLNQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQueryLNQMetricS3.start();

		BenchmarkReportData.queryLNQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryLNQMetricS3.stop();

		Thread numericQueryLNQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.LT_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQueryLNQMetricS4.start();

		BenchmarkReportData.queryLNQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryLNQMetricS4.stop();
		
		
		
		
		Thread numericQueryGNQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQueryGNQMetricS1.start();

		BenchmarkReportData.queryGNQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryGNQMetricS1.stop();

		Thread numericQueryGNQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQueryGNQMetricS2.start();

		BenchmarkReportData.queryGNQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryGNQMetricS2.stop();

		Thread numericQueryGNQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQueryGNQMetricS3.start();

		BenchmarkReportData.queryGNQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryGNQMetricS3.stop();

		Thread numericQueryGNQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.GT_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQueryGNQMetricS4.start();

		BenchmarkReportData.queryGNQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryGNQMetricS4.stop();
		
		
		
		
		Thread numericQueryANQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQueryANQMetricS1.start();

		BenchmarkReportData.queryANQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryANQMetricS1.stop();

		Thread numericQueryANQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQueryANQMetricS2.start();

		BenchmarkReportData.queryANQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryANQMetricS2.stop();

		Thread numericQueryANQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQueryANQMetricS3.start();

		BenchmarkReportData.queryANQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryANQMetricS3.stop();

		Thread numericQueryANQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.AND_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQueryANQMetricS4.start();

		BenchmarkReportData.queryANQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.AND_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryANQMetricS4.stop();
		
		
		
		
		Thread numericQueryONQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQueryONQMetricS1.start();

		BenchmarkReportData.queryONQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryONQMetricS1.stop();

		Thread numericQueryONQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQueryONQMetricS2.start();

		BenchmarkReportData.queryONQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryONQMetricS2.stop();

		Thread numericQueryONQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQueryONQMetricS3.start();

		BenchmarkReportData.queryONQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryONQMetricS3.stop();

		Thread numericQueryONQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.OR_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQueryONQMetricS4.start();

		BenchmarkReportData.queryONQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.OR_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryONQMetricS4.stop();
		
		
		
		
		Thread numericQuerySNQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE_2T, port));
		numericQuerySNQMetricS1.start();

		BenchmarkReportData.querySNQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySNQMetricS1.stop();

		Thread numericQuerySNQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE_4T, port));
		numericQuerySNQMetricS2.start();

		BenchmarkReportData.querySNQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySNQMetricS2.stop();

		Thread numericQuerySNQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE_6T, port));
		numericQuerySNQMetricS3.start();

		BenchmarkReportData.querySNQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySNQMetricS3.stop();

		Thread numericQuerySNQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_NUMERIC_QUERY_STANDALONE_8T, port));
		numericQuerySNQMetricS4.start();

		BenchmarkReportData.querySNQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_NUMERIC_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySNQMetricS4.stop();
		
		
		
		Thread numericQueryTTQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_STANDALONE_2T, port));
		numericQueryTTQMetricS1.start();

		BenchmarkReportData.queryTTQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryTTQMetricS1.stop();

		Thread numericQueryTTQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_STANDALONE_4T, port));
		numericQueryTTQMetricS2.start();

		BenchmarkReportData.queryTTQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryTTQMetricS2.stop();

		Thread numericQueryTTQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_STANDALONE_6T, port));
		numericQueryTTQMetricS3.start();

		BenchmarkReportData.queryTTQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryTTQMetricS3.stop();

		Thread numericQueryTTQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_TERM_QUERY_STANDALONE_8T, port));
		numericQueryTTQMetricS4.start();

		BenchmarkReportData.queryTTQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_TERM_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryTTQMetricS4.stop();
		
		
		
		
		Thread numericQueryPTQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_STANDALONE_2T, port));
		numericQueryPTQMetricS1.start();

		BenchmarkReportData.queryPTQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryPTQMetricS1.stop();

		Thread numericQueryPTQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_STANDALONE_4T, port));
		numericQueryPTQMetricS2.start();

		BenchmarkReportData.queryPTQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryPTQMetricS2.stop();

		Thread numericQueryPTQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_STANDALONE_6T, port));
		numericQueryPTQMetricS3.start();

		BenchmarkReportData.queryPTQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryPTQMetricS3.stop();

		Thread numericQueryPTQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.TEXT_PHRASE_QUERY_STANDALONE_8T, port));
		numericQueryPTQMetricS4.start();

		BenchmarkReportData.queryPTQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TEXT_PHRASE_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryPTQMetricS4.stop();
		
		
		
		
		Thread numericQuerySTQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_STANDALONE_2T, port));
		numericQuerySTQMetricS1.start();

		BenchmarkReportData.querySTQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySTQMetricS1.stop();

		Thread numericQuerySTQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_STANDALONE_4T, port));
		numericQuerySTQMetricS2.start();

		BenchmarkReportData.querySTQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySTQMetricS2.stop();

		Thread numericQuerySTQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_STANDALONE_6T, port));
		numericQuerySTQMetricS3.start();

		BenchmarkReportData.querySTQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySTQMetricS3.stop();

		Thread numericQuerySTQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.SORTED_TEXT_QUERY_STANDALONE_8T, port));
		numericQuerySTQMetricS4.start();

		BenchmarkReportData.querySTQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.SORTED_TEXT_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQuerySTQMetricS4.stop();
		
		
		
		
		Thread numericQueryHTQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_STANDALONE_2T, port));
		numericQueryHTQMetricS1.start();

		BenchmarkReportData.queryHTQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryHTQMetricS1.stop();

		Thread numericQueryHTQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_STANDALONE_4T, port));
		numericQueryHTQMetricS2.start();

		BenchmarkReportData.queryHTQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryHTQMetricS2.stop();

		Thread numericQueryHTQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_STANDALONE_6T, port));
		numericQueryHTQMetricS3.start();

		BenchmarkReportData.queryHTQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryHTQMetricS3.stop();

		Thread numericQueryHTQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.HIGHLIGHTING_QUERY_STANDALONE_8T, port));
		numericQueryHTQMetricS4.start();

		BenchmarkReportData.queryHTQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.HIGHLIGHT_QUERY, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);
		
		numericQueryHTQMetricS4.stop();

		
		Thread numericQueryCTFQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_TERM_FACETING_QUERY_STANDALONE_2T, port));
		numericQueryCTFQMetricS1.start();		
		
		BenchmarkReportData.queryCTFQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_TERM_FACETING, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCTFQMetricS1.stop();
		

		Thread numericQueryCTFQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_TERM_FACETING_QUERY_STANDALONE_4T, port));
		numericQueryCTFQMetricS2.start();		
		
		BenchmarkReportData.queryCTFQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_TERM_FACETING, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCTFQMetricS2.stop();

		
		Thread numericQueryCTFQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_TERM_FACETING_QUERY_STANDALONE_6T, port));
		numericQueryCTFQMetricS3.start();		
		
		BenchmarkReportData.queryCTFQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_TERM_FACETING, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCTFQMetricS3.stop();
		
		
		Thread numericQueryCTFQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_TERM_FACETING_QUERY_STANDALONE_8T, port));
		numericQueryCTFQMetricS4.start();		
		
		BenchmarkReportData.queryCTFQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_TERM_FACETING, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCTFQMetricS4.stop();		

		
		
		Thread numericQueryCRFQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_RANGE_FACETING_QUERY_STANDALONE_2T, port));
		numericQueryCRFQMetricS1.start();		
		
		BenchmarkReportData.queryCRFQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_RANGE_FACETING, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCRFQMetricS1.stop();
		

		Thread numericQueryCRFQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_RANGE_FACETING_QUERY_STANDALONE_4T, port));
		numericQueryCRFQMetricS2.start();		
		
		BenchmarkReportData.queryCRFQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_RANGE_FACETING, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCRFQMetricS2.stop();

		
		Thread numericQueryCRFQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_RANGE_FACETING_QUERY_STANDALONE_6T, port));
		numericQueryCRFQMetricS3.start();		
		
		BenchmarkReportData.queryCRFQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_RANGE_FACETING, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCRFQMetricS3.stop();
		
		
		Thread numericQueryCRFQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.CLASSIC_RANGE_FACETING_QUERY_STANDALONE_8T, port));
		numericQueryCRFQMetricS4.start();		
		
		BenchmarkReportData.queryCRFQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.CLASSIC_RANGE_FACETING, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryCRFQMetricS4.stop();		

		
		
		
		
		
		Thread numericQueryJTFQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_TERM_FACETING_QUERY_STANDALONE_2T, port));
		numericQueryJTFQMetricS1.start();		
		
		BenchmarkReportData.queryJTFQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_TERM_FACETING, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJTFQMetricS1.stop();
		

		Thread numericQueryJTFQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_TERM_FACETING_QUERY_STANDALONE_4T, port));
		numericQueryJTFQMetricS2.start();		
		
		BenchmarkReportData.queryJTFQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_TERM_FACETING, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJTFQMetricS2.stop();

		
		Thread numericQueryJTFQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_TERM_FACETING_QUERY_STANDALONE_6T, port));
		numericQueryJTFQMetricS3.start();		
		
		BenchmarkReportData.queryJTFQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_TERM_FACETING, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJTFQMetricS3.stop();
		
		
		Thread numericQueryJTFQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_TERM_FACETING_QUERY_STANDALONE_8T, port));
		numericQueryJTFQMetricS4.start();		
		
		BenchmarkReportData.queryJTFQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_TERM_FACETING, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJTFQMetricS4.stop();		
		
		
		
		
		
		Thread numericQueryJRFQMetricS1 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_RANGE_FACETING_QUERY_STANDALONE_2T, port));
		numericQueryJRFQMetricS1.start();		
		
		BenchmarkReportData.queryJRFQMetricS_T1 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_RANGE_FACETING, Util.QUERY_THREAD_COUNT_FIRST, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJRFQMetricS1.stop();
		

		Thread numericQueryJRFQMetricS2 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_RANGE_FACETING_QUERY_STANDALONE_4T, port));
		numericQueryJRFQMetricS2.start();		
		
		BenchmarkReportData.queryJRFQMetricS_T2 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_RANGE_FACETING, Util.QUERY_THREAD_COUNT_SECOND, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJRFQMetricS2.stop();

		
		Thread numericQueryJRFQMetricS3 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_RANGE_FACETING_QUERY_STANDALONE_6T, port));
		numericQueryJRFQMetricS3.start();		
		
		BenchmarkReportData.queryJRFQMetricS_T3 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_RANGE_FACETING, Util.QUERY_THREAD_COUNT_THIRD, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJRFQMetricS3.stop();
		
		
		Thread numericQueryJRFQMetricS4 = new Thread(
				new MetricCollector(Util.COMMIT_ID, TestType.JSON_RANGE_FACETING_QUERY_STANDALONE_8T, port));
		numericQueryJRFQMetricS4.start();		
		
		BenchmarkReportData.queryJRFQMetricS_T4 = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.JSON_RANGE_FACETING, Util.QUERY_THREAD_COUNT_FOURTH, 120, 30, Tests.node.getBaseUrl(),
				Tests.node.collectionName);		
		
		numericQueryJRFQMetricS4.stop();		
		
		Tests.shutDownStandalone();
	}
}