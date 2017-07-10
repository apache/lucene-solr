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

/**
 * @author Vivek Narang
 */
public class BenchmarkReportData {

	public static Map<String, String> returnStandaloneCreateCollectionMap;
	public static Map<String, String> returnCloudCreateCollectionMap;

	public static Map<String, String> metricMapIndexingStandalone;
	public static Map<String, String> metricMapPartialUpdateStandalone;

	public static Map<String, String> metricMapCloudSerial_2N1S2R;
	public static Map<String, String> metricMapCloudSerial_2N2S1R;
	public static Map<String, String> metricMapCloudSerial_3N1S3R;
	public static Map<String, String> metricMapCloudSerial_4N2S2R;

	public static Map<String, String> metricMapCloudConcurrent1_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent1_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent1_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent1_4N2S2R;

	public static Map<String, String> metricMapCloudConcurrent2_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent2_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent2_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent2_4N2S2R;

	public static Map<String, String> metricMapCloudConcurrent3_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent3_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent3_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent3_4N2S2R;

	public static Map<String, String> metricMapStandaloneIndexingConcurrent1;
	public static Map<String, String> metricMapStandaloneIndexingConcurrent2;
	public static Map<String, String> metricMapStandaloneIndexingConcurrent3;
	
	public static Map<String, String> metricMapStandalonePartialUpdateConcurrent1;
	public static Map<String, String> metricMapStandalonePartialUpdateConcurrent2;
	public static Map<String, String> metricMapStandalonePartialUpdateConcurrent3;

	public static Map<String, String> queryTNQMetricC;
	public static Map<String, String> queryRNQMetricC;
	public static Map<String, String> queryLNQMetricC;
	public static Map<String, String> queryGNQMetricC;
	public static Map<String, String> queryANQMetricC;
	public static Map<String, String> queryONQMetricC;
	public static Map<String, String> querySNQMetricC;
	public static Map<String, String> queryTTQMetricC;
	public static Map<String, String> queryPTQMetricC;
	public static Map<String, String> querySTQMetricC;
	public static Map<String, String> queryHTQMetricC;

	public static Map<String, String> queryTNQMetricS;
	public static Map<String, String> queryRNQMetricS;
	public static Map<String, String> queryLNQMetricS;
	public static Map<String, String> queryGNQMetricS;
	public static Map<String, String> queryANQMetricS;
	public static Map<String, String> queryONQMetricS;
	public static Map<String, String> querySNQMetricS;
	public static Map<String, String> queryTTQMetricS;
	public static Map<String, String> queryPTQMetricS;
	public static Map<String, String> querySTQMetricS;
	public static Map<String, String> queryHTQMetricS;

	/**
	 * A method used to refresh the data structures when a cycle completes.
	 */
	public static void reset() {

		metricMapCloudConcurrent1_2N1S2R = null;
		metricMapCloudConcurrent1_2N2S1R = null;
		metricMapCloudConcurrent1_3N1S3R = null;
		metricMapCloudConcurrent1_4N2S2R = null;

		metricMapCloudConcurrent2_2N1S2R = null;
		metricMapCloudConcurrent2_2N2S1R = null;
		metricMapCloudConcurrent2_3N1S3R = null;
		metricMapCloudConcurrent2_4N2S2R = null;

		metricMapCloudConcurrent3_2N1S2R = null;
		metricMapCloudConcurrent3_2N2S1R = null;
		metricMapCloudConcurrent3_3N1S3R = null;
		metricMapCloudConcurrent3_4N2S2R = null;

		metricMapStandaloneIndexingConcurrent1 = null;
		metricMapStandaloneIndexingConcurrent2 = null;
		metricMapStandaloneIndexingConcurrent3 = null;
		
		metricMapStandalonePartialUpdateConcurrent1 = null;
		metricMapStandalonePartialUpdateConcurrent2 = null;
		metricMapStandalonePartialUpdateConcurrent3 = null;

		metricMapCloudSerial_2N1S2R = null;
		metricMapCloudSerial_2N2S1R = null;
		metricMapCloudSerial_3N1S3R = null;
		metricMapCloudSerial_4N2S2R = null;

		returnStandaloneCreateCollectionMap = null;
		returnCloudCreateCollectionMap = null;

		metricMapIndexingStandalone = null;

		queryTNQMetricC = null;
		queryRNQMetricC = null;
		queryLNQMetricC = null;
		queryGNQMetricC = null;
		queryANQMetricC = null;
		queryONQMetricC = null;
		querySNQMetricC = null;
		queryTTQMetricC = null;
		queryPTQMetricC = null;
		querySTQMetricC = null;
		queryHTQMetricC = null;
		
		queryTNQMetricS = null;
		queryRNQMetricS = null;
		queryLNQMetricS = null;
		queryGNQMetricS = null;
		queryANQMetricS = null;
		queryONQMetricS = null;
		querySNQMetricS = null;
		queryTTQMetricS = null;
		queryPTQMetricS = null;
		querySTQMetricS = null;
		queryHTQMetricS = null;

	}

}