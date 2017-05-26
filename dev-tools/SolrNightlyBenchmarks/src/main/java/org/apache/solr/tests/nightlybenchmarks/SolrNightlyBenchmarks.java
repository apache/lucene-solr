package org.apache.solr.tests.nightlybenchmarks;
import java.io.IOException;
import java.util.Map;

import org.apache.solr.tests.nightlybenchmarks.ThreadedNumericQueryClient.NumericQueryType;

public class SolrNightlyBenchmarks {

	public static void main(String[] args) throws IOException, InterruptedException {

		Util.getProperties();		
		Util.checkWebAppFiles();
		Util.checkBaseAndTempDir();

		Util.setAliveFlag();
		Map<String, String> argM = Util.getArgs(args);
		Util.getSystemEnvironmentInformation();
			
		String commitID = "";
		if(argM.containsKey("-commitID")) {
			commitID = argM.get("-commitID");
		} else {
			commitID = Util.getLatestCommitID();	
			Util.postMessage("The latest commit ID is: " + commitID, MessageType.RESULT_ERRROR, false);
		}
		Util.commitId = commitID;
		
		Tests.indexingTests(commitID, 10000);
		
		Tests.setUpCloudForFeatureTests(commitID, 50000);
		
		BenchmarkReportData.numericQueryTNQMetric = Tests.numericQueryTests(commitID, NumericQueryType.TERM_NUMERIC_QUERY, 1000, 60);
		Thread.sleep(5000);
		BenchmarkReportData.numericQueryRNQMetric =	Tests.numericQueryTests(commitID, NumericQueryType.RANGE_NUMERIC_QUERY, 1000, 60);
		Thread.sleep(5000);
		BenchmarkReportData.numericQueryLNQMetric =	Tests.numericQueryTests(commitID, NumericQueryType.LESS_THAN_NUMERIC_QUERY, 1000, 60);
		Thread.sleep(5000);
		BenchmarkReportData.numericQueryGNQMetric =	Tests.numericQueryTests(commitID, NumericQueryType.GREATER_THAN_NUMERIC_QUERY, 1000, 60);
		
		Tests.shutDown();
		
		OutPort.publishDataForWebApp();
		
		if(argM.containsKey("-Housekeeping")) {
			Util.cleanUpSrcDirs();
		}
		
		Util.setDeadFlag();
	}
}