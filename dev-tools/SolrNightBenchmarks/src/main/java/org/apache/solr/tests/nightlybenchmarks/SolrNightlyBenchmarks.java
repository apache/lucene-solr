package org.apache.solr.tests.nightlybenchmarks;
import java.io.IOException;

public class SolrNightlyBenchmarks {

	public static void main(String[] args) throws IOException, InterruptedException {

		Util.init(args);
		
		//Tests.indexingTests(Util.COMMIT_ID, 10000);
		
		Tests.setUpCloudForFeatureTests(Util.COMMIT_ID, 50000, 1, "2", "2");
//		BenchmarkReportData.numericQueryTNQMetric = Tests.numericQueryTests(Util.COMMIT_ID, QueryType.TERM_NUMERIC_QUERY, 1, 180, 120);
//		BenchmarkReportData.numericQueryRNQMetric =	Tests.numericQueryTests(Util.COMMIT_ID, QueryType.RANGE_NUMERIC_QUERY, 1, 180, 120);
//		BenchmarkReportData.numericQueryLNQMetric =	Tests.numericQueryTests(Util.COMMIT_ID, QueryType.LESS_THAN_NUMERIC_QUERY, 1, 180, 120);
//		BenchmarkReportData.numericQueryGNQMetric =	Tests.numericQueryTests(Util.COMMIT_ID, QueryType.GREATER_THAN_NUMERIC_QUERY, 1, 180, 120);
		Tests.shutDown();
//		BenchmarkAppConnector.publishDataForWebApp();

		
		Util.destroy();
	}
}