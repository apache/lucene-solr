package org.apache.solr.tests.nightlybenchmarks;

import java.util.Map;

public class BenchmarkReportData {
	
	public static Map<String, String> returnStandaloneCreateCollectionMap;
	public static Map<String, String> returnCloudCreateCollectionMap;
	
	public static Map<String, String> metricMapStandalone;
	
	public static Map<String, String> metricMapCloudSerial;
	
	public static Map<String, String> metricMapCloudConcurrent2;
	public static Map<String, String> metricMapCloudConcurrent4;
	public static Map<String, String> metricMapCloudConcurrent6;
	public static Map<String, String> metricMapCloudConcurrent8;
	public static Map<String, String> metricMapCloudConcurrent10;
	
	public static Map<String, String> numericQueryTNQMetric;
	public static Map<String, String> numericQueryRNQMetric;
	public static Map<String, String> numericQueryLNQMetric;
	public static Map<String, String> numericQueryGNQMetric;
	

}
