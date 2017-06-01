package org.apache.solr.tests.nightlybenchmarks;

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

public class Tests {
	
	public static SolrCloud cloud; 	

	public static boolean indexingTests(String commitID, int numDocuments) {
	
		try {
		
		SolrNode node = new SolrNode(commitID, "", "", false);

		node.doAction(SolrNodeAction.NODE_START);			
		Util.getEnvironmentInformationFromMetricAPI(commitID, node.port);
		BenchmarkReportData.returnStandaloneCreateCollectionMap = node.createCollection("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());	
		
		SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, node.collectionName, commitID);
		BenchmarkReportData.metricMapStandalone = client.indexAmazonFoodData(numDocuments, node.getBaseUrl() + node.collectionName);
		
		node.doAction(SolrNodeAction.NODE_STOP);		
		node.cleanup();	
		
		SolrCloud cloud = new SolrCloud(5, "2", "2", commitID, null, "localhost", true);
		Tests.cloud = cloud;		
		SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, cloud.collectionName, commitID);
		BenchmarkReportData.metricMapCloudSerial = cloudClient.indexAmazonFoodData(numDocuments, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName);
		
		BenchmarkReportData.metricMapCloudConcurrent2 = cloudClient.indexAmazonFoodData(numDocuments, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 2);
		BenchmarkReportData.metricMapCloudConcurrent4 = cloudClient.indexAmazonFoodData(numDocuments, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 4);
		BenchmarkReportData.metricMapCloudConcurrent6 = cloudClient.indexAmazonFoodData(numDocuments, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 6);
		BenchmarkReportData.metricMapCloudConcurrent8 = cloudClient.indexAmazonFoodData(numDocuments, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 8);
		BenchmarkReportData.metricMapCloudConcurrent10 = cloudClient.indexAmazonFoodData(numDocuments, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 10);

		cloud.shutdown();
		BenchmarkReportData.returnCloudCreateCollectionMap = cloud.returnMapCreateCollection;
		
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
		return true;
	}
	
	public static Map<String, String> numericQueryTests(String commitID, QueryType queryType, int numberOfThreads, int secondsToWait, long delayEstimationBySeconds) {
		
		try {
			
			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			LinkedList<QueryClient> list = new LinkedList<QueryClient>();
	
			for (int i = 0 ; i < numberOfThreads ; i++) {
				QueryClient client = new QueryClient(cloud.getBaseURL(), 10000, 10, cloud.collectionName, queryType, numberOfThreads, delayEstimationBySeconds);
				list.add(client);
			}

			QueryClient.running = true;
			
			for (int i = 0 ; i < numberOfThreads ; i++) {
				executorService.execute(list.get(i));
			}
		
			Thread.sleep(secondsToWait * 1000);
			
			QueryClient.running = false;
			
			executorService.shutdownNow();	
			
			Thread.sleep(5000);	
			
			Map<String, String> returnMap = new HashMap<String, String>();
			
	        Date dNow = new Date( );
	   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");

	   	    returnMap.put("TimeStamp", ft.format(dNow));
	   	    returnMap.put("CommitID", commitID);
			returnMap.put("TotalQueriesExecuted", "" + QueryClient.queryCount);
			returnMap.put("QueriesPerSecond", "" + (double)(QueryClient.queryCount/(secondsToWait-delayEstimationBySeconds)));
			returnMap.put("MinQTime", "" + QueryClient.minQtime);
			returnMap.put("MaxQTime", "" + QueryClient.maxQtime);
			returnMap.put("QueryFailureCount", "" + QueryClient.queryFailureCount);
			
			Util.postMessage(returnMap.toString(), MessageType.RED_TEXT, false);
			QueryClient.reset();
			
			return returnMap;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	public static void setUpCloudForFeatureTests(String commitID, int documentCount, int solrNodes, String shards, String replicas) {

		SolrCloud cloud = new SolrCloud(solrNodes, shards, replicas, commitID, null, "localhost", true);
		Tests.cloud = cloud;		
		SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, cloud.collectionName, commitID);
		cloudClient.indexAmazonFoodData(documentCount, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 10);

	}
	
	public static void shutDown() throws IOException, InterruptedException {
		cloud.shutdown();
	}

}