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

import org.apache.solr.tests.nightlybenchmarks.ThreadedNumericQueryClient.NumericQueryType;

public class Tests {
	
	public static SolrCloud cloud; 	

	public static boolean indexingTests(String commitID, int numDocuments) {
	
		try {
		
		SolrNode node = new SolrNode(commitID, "", "", false);

		node.start();			
		Util.getEnvironmentInformationFromMetricAPI(commitID, node.port);
		BenchmarkReportData.returnStandaloneCreateCollectionMap = node.createCore("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());	
		
		SolrIndexingClient client = new SolrIndexingClient("localhost", node.port, node.collectionName, commitID);
		BenchmarkReportData.metricMapStandalone = client.indexAmazonFoodData(numDocuments, node.getBaseUrl() + node.collectionName);
		
		node.stop();		
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
	
	public static Map<String, String> numericQueryTests(String commitID, NumericQueryType queryType, int numberOfThreads, int secondsToWait) {
		
		try {
			
			ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
			LinkedList<ThreadedNumericQueryClient> list = new LinkedList<ThreadedNumericQueryClient>();
	
			for (int i = 0 ; i < numberOfThreads ; i++) {
				ThreadedNumericQueryClient client = new ThreadedNumericQueryClient(cloud.getBaseURL(), 10000, 10, cloud.collectionName, queryType);
				list.add(client);
			}
			
			ThreadedNumericQueryClient.running = true;
	
			for (int i = 0 ; i < numberOfThreads ; i++) {
				executorService.execute(list.get(i));
			}
			
			Thread.sleep(secondsToWait * 1000);
			
			ThreadedNumericQueryClient.running = false;
			
			executorService.shutdown();		
			
			Thread.sleep(5000);	
			
			Map<String, String> returnMap = new HashMap<String, String>();
			
	        Date dNow = new Date( );
	   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");

	   	    returnMap.put("TimeStamp", ft.format(dNow));
	   	    returnMap.put("CommitID", commitID);
			returnMap.put("TotalQueriesExecuted", "" + ThreadedNumericQueryClient.queryCount);
			returnMap.put("QueriesPerSecond", "" + (double)(ThreadedNumericQueryClient.queryCount/secondsToWait));
			returnMap.put("MinQTime", "" + ThreadedNumericQueryClient.minQtime);
			returnMap.put("MaxQTime", "" + ThreadedNumericQueryClient.maxQtime);
			
			return returnMap;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		return null;
	}
	
	public static void setUpCloudForFeatureTests(String commitID, int documentCount) {

		SolrCloud cloud = new SolrCloud(5, "2", "2", commitID, null, "localhost", true);
		Tests.cloud = cloud;		
		SolrIndexingClient cloudClient = new SolrIndexingClient("localhost", cloud.port, cloud.collectionName, commitID);
		cloudClient.indexAmazonFoodData(documentCount, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 10);

	}
	
	public static void shutDown() throws IOException, InterruptedException {
		cloud.shutdown();
	}

}