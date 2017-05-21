package org.apache.solr.tests.nightlybenchmarks;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SolrNightlyBenchmarks {

	public static void main(String[] args) throws IOException, InterruptedException {

	
		Map<String, String> argM = new HashMap<String, String>();
		for (int i = 0; i < args.length; i += 2) {
			argM.put(args[i], args[i + 1]);
		}
		
		Util.getProperties();		
		Util.checkWebAppFiles();
		Util.checkBaseAndTempDir();
		Util.getSystemEnvironmentInformation();

		File statusFile = new File(BenchmarkAppConnector.benchmarkAppDirectory + "iamalive.txt");
		if (!statusFile.exists()) {
			statusFile.createNewFile();			
		}
		
		try {
			
			String commitID = "";
			if(argM.containsKey("-commitID")) {
				commitID = argM.get("-commitID");
			} else {
				commitID = Util.getLatestCommitID();	
				Util.postMessage("The latest commit ID is: " + commitID, MessageType.RESULT_ERRROR, false);
			}
			Util.commitId = commitID;
			
			
			// Sample Test
		
			SolrNode node = new SolrNode(commitID, "", "", false);
			node.start();			
			Util.getEnvironmentInformationFromMetricAPI(commitID, node.port);
			node.createCore("Core-" + UUID.randomUUID(), "Collection-" + UUID.randomUUID());	
			
			SolrClient client = new SolrClient("localhost", node.port, node.collectionName, commitID);
			client.indexAmazonFoodData(10000, node.getBaseUrl() + node.collectionName);
			
			node.stop();		
			node.cleanup();	
			
			SolrCloud cloud = new SolrCloud(5, "2", "2", commitID, null, "localhost", true);
			SolrClient cloudClient = new SolrClient("localhost", cloud.port, cloud.collectionName, commitID);
			cloudClient.indexAmazonFoodData(10000, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName);
			cloudClient.indexAmazonFoodData(10000, cloud.getuRL(), cloud.zookeeperIp, cloud.zookeeperPort, cloud.collectionName, 10000, 10);
			cloud.shutdown();
			
			// End Sample Test
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		statusFile = new File(BenchmarkAppConnector.benchmarkAppDirectory + "iamalive.txt");
		if (statusFile.exists()) {
			statusFile.delete();			
		}
		
		if(argM.containsKey("-Housekeeping")) {
			Util.cleanUpSrcDirs();
		}
	}
}