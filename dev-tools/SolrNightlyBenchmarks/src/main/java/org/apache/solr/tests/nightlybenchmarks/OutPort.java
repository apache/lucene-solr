package org.apache.solr.tests.nightlybenchmarks;

import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;

public class OutPort {
	
	public static void publishDataForWebApp() {
		
		Util.postMessage("** Publishing data for webapp ..", MessageType.ACTION, false);
		
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_standalone_regular.csv", BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + BenchmarkReportData.metricMapStandalone.get("IndexingTime") + ", " + BenchmarkReportData.metricMapStandalone.get("CommitID"), false, FileType.STANDALONE_INDEXING_MAIN);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv", BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + BenchmarkReportData.metricMapStandalone.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapStandalone.get("CommitID"), false, FileType.STANDALONE_INDEXING_THROUGHPUT);	

		
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_standalone_regular.csv", BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + BenchmarkReportData.metricMapStandalone.get("IndexingTime") + ", " + BenchmarkReportData.metricMapStandalone.get("CommitID"), false, FileType.STANDALONE_INDEXING_MAIN);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv", BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + BenchmarkReportData.metricMapStandalone.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapStandalone.get("CommitID"), false, FileType.STANDALONE_INDEXING_THROUGHPUT);	


		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_cloud_regular.csv", BenchmarkReportData.metricMapCloudSerial.get("TimeStamp") + ", " + BenchmarkReportData.metricMapCloudSerial.get("IndexingTime") + ", " + BenchmarkReportData.metricMapCloudSerial.get("CommitID"), false, FileType.CLOUD_INDEXING_SERIAL);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial.csv", BenchmarkReportData.metricMapCloudSerial.get("TimeStamp") + ", " + BenchmarkReportData.metricMapCloudSerial.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapCloudSerial.get("CommitID"), false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);	

		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_cloud_concurrent.csv", BenchmarkReportData.metricMapCloudConcurrent2.get("TimeStamp") + ", " + BenchmarkReportData.metricMapCloudConcurrent2.get("CommitID") + ", " + BenchmarkReportData.metricMapCloudConcurrent2.get("IndexingTime") + ", "  + BenchmarkReportData.metricMapCloudConcurrent4.get("IndexingTime") + ", "  + BenchmarkReportData.metricMapCloudConcurrent6.get("IndexingTime") + ", "  + BenchmarkReportData.metricMapCloudConcurrent8.get("IndexingTime") + ", "  + BenchmarkReportData.metricMapCloudConcurrent10.get("IndexingTime"), false, FileType.CLOUD_INDEXING_CONCURRENT);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent.csv", BenchmarkReportData.metricMapCloudConcurrent2.get("TimeStamp") + ", " + BenchmarkReportData.metricMapCloudConcurrent2.get("CommitID") + ", " + BenchmarkReportData.metricMapCloudConcurrent2.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapCloudConcurrent4.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapCloudConcurrent6.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapCloudConcurrent8.get("IndexingThroughput") + ", " + BenchmarkReportData.metricMapCloudConcurrent10.get("IndexingThroughput"), false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);	


		BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_cloud_regular.csv", BenchmarkReportData.returnCloudCreateCollectionMap.get("TimeStamp") + ", " + BenchmarkReportData.returnCloudCreateCollectionMap.get("CreateCollectionTime") + ", " + BenchmarkReportData.returnCloudCreateCollectionMap.get("CommitID"), false, FileType.CLOUD_CREATE_COLLECTION_MAIN);	
		BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_standalone_regular.csv", BenchmarkReportData.returnStandaloneCreateCollectionMap.get("TimeStamp") + ", " +  BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CreateCollectionTime") + ", " +  BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CommitID"), false, FileType.STANDALONE_CREATE_COLLECTION_MAIN);	

		BenchmarkAppConnector.writeToWebAppDataFile("numeric_query_benchmark_cloud.csv", BenchmarkReportData.numericQueryTNQMetric.get("TimeStamp") + ", " +  BenchmarkReportData.numericQueryTNQMetric.get("CommitID") + ", " +  BenchmarkReportData.numericQueryTNQMetric.get("QueriesPerSecond") + ", " +  BenchmarkReportData.numericQueryTNQMetric.get("MinQTime") + ", " +  BenchmarkReportData.numericQueryTNQMetric.get("MaxQTime") + ", " +  BenchmarkReportData.numericQueryRNQMetric.get("QueriesPerSecond") + ", " +  BenchmarkReportData.numericQueryRNQMetric.get("MinQTime") + ", " +  BenchmarkReportData.numericQueryRNQMetric.get("MaxQTime") + ", " +  BenchmarkReportData.numericQueryLNQMetric.get("QueriesPerSecond") + ", " +  BenchmarkReportData.numericQueryLNQMetric.get("MinQTime") + ", " +  BenchmarkReportData.numericQueryLNQMetric.get("MaxQTime") + ", " +  BenchmarkReportData.numericQueryGNQMetric.get("QueriesPerSecond") + ", " +  BenchmarkReportData.numericQueryGNQMetric.get("MinQTime") + ", " +  BenchmarkReportData.numericQueryGNQMetric.get("MaxQTime"), false, FileType.NUMERIC_QUERY_CLOUD);	
		
		Util.postMessage("** Publishing data for webapp [COMPLETE] ..", MessageType.ACTION, false);
		
	}

	
}
