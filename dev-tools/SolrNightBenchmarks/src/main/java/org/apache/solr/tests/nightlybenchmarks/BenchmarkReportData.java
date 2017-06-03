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

	public static Map<String, String> metricMapStandaloneConcurrent2;
	public static Map<String, String> metricMapStandaloneConcurrent4;
	public static Map<String, String> metricMapStandaloneConcurrent6;
	public static Map<String, String> metricMapStandaloneConcurrent8;
	public static Map<String, String> metricMapStandaloneConcurrent10;
	
	public static Map<String, String> numericQueryTNQMetricC;
	public static Map<String, String> numericQueryRNQMetricC;
	public static Map<String, String> numericQueryLNQMetricC;
	public static Map<String, String> numericQueryGNQMetricC;

	public static Map<String, String> numericQueryTNQMetricS;
	public static Map<String, String> numericQueryRNQMetricS;
	public static Map<String, String> numericQueryLNQMetricS;
	public static Map<String, String> numericQueryGNQMetricS;

}
