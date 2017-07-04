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

	public static Map<String, String> metricMapStandaloneConcurrent1;
	public static Map<String, String> metricMapStandaloneConcurrent2;
	public static Map<String, String> metricMapStandaloneConcurrent3;

	public static Map<String, String> numericQueryTNQMetricC;
	public static Map<String, String> numericQueryRNQMetricC;
	public static Map<String, String> numericQueryLNQMetricC;
	public static Map<String, String> numericQueryGNQMetricC;
	public static Map<String, String> numericQueryANQMetricC;
	public static Map<String, String> numericQueryONQMetricC;
	public static Map<String, String> numericQuerySNQMetricC;
	public static Map<String, String> numericQueryTTQMetricC;
	public static Map<String, String> numericQueryPTQMetricC;
	public static Map<String, String> numericQuerySTQMetricC;

	public static Map<String, String> numericQueryTNQMetricS;
	public static Map<String, String> numericQueryRNQMetricS;
	public static Map<String, String> numericQueryLNQMetricS;
	public static Map<String, String> numericQueryGNQMetricS;
	public static Map<String, String> numericQueryANQMetricS;
	public static Map<String, String> numericQueryONQMetricS;
	public static Map<String, String> numericQuerySNQMetricS;
	public static Map<String, String> numericQueryTTQMetricS;
	public static Map<String, String> numericQueryPTQMetricS;
	public static Map<String, String> numericQuerySTQMetricS;
	

	public static void reset() {

		returnStandaloneCreateCollectionMap = null;
		returnCloudCreateCollectionMap = null;

		metricMapStandalone = null;

		metricMapCloudSerial_2N1S2R = null;
		metricMapCloudSerial_2N2S1R = null;
		metricMapCloudSerial_3N1S3R = null;
		metricMapCloudSerial_4N2S2R = null;

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

		metricMapStandaloneConcurrent1 = null;
		metricMapStandaloneConcurrent2 = null;
		metricMapStandaloneConcurrent3 = null;

		numericQueryTNQMetricC = null;
		numericQueryRNQMetricC = null;
		numericQueryLNQMetricC = null;
		numericQueryGNQMetricC = null;
		numericQueryANQMetricC = null;
		numericQueryONQMetricC = null;
		numericQuerySNQMetricC = null;
		numericQueryTTQMetricC = null;
		numericQueryPTQMetricC = null;
		numericQuerySTQMetricC = null;

		numericQueryTNQMetricS = null;
		numericQueryRNQMetricS = null;
		numericQueryLNQMetricS = null;
		numericQueryGNQMetricS = null;
		numericQueryANQMetricS = null;
		numericQueryONQMetricS = null;
		numericQuerySNQMetricS = null;
		numericQueryTTQMetricS = null;
		numericQueryPTQMetricS = null;
		numericQuerySTQMetricS = null;

	}

}
