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
	
	
	public static Map<String, String> metricMapCloudConcurrent2_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent2_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent2_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent2_4N2S2R;
	
	public static Map<String, String> metricMapCloudConcurrent4_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent4_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent4_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent4_4N2S2R;

	public static Map<String, String> metricMapCloudConcurrent6_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent6_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent6_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent6_4N2S2R;

	public static Map<String, String> metricMapCloudConcurrent8_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent8_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent8_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent8_4N2S2R;

	public static Map<String, String> metricMapCloudConcurrent10_2N1S2R;
	public static Map<String, String> metricMapCloudConcurrent10_2N2S1R;
	public static Map<String, String> metricMapCloudConcurrent10_3N1S3R;
	public static Map<String, String> metricMapCloudConcurrent10_4N2S2R;

	public static Map<String, String> metricMapStandaloneConcurrent2;
	public static Map<String, String> metricMapStandaloneConcurrent4;
	public static Map<String, String> metricMapStandaloneConcurrent6;
	public static Map<String, String> metricMapStandaloneConcurrent8;
	public static Map<String, String> metricMapStandaloneConcurrent10;
	
	public static Map<String, String> numericQueryTNQMetricC;
	public static Map<String, String> numericQueryRNQMetricC;
	public static Map<String, String> numericQueryLNQMetricC;
	public static Map<String, String> numericQueryGNQMetricC;
	public static Map<String, String> numericQueryANQMetricC;
	public static Map<String, String> numericQueryONQMetricC;

	public static Map<String, String> numericQueryTNQMetricS;
	public static Map<String, String> numericQueryRNQMetricS;
	public static Map<String, String> numericQueryLNQMetricS;
	public static Map<String, String> numericQueryGNQMetricS;
	public static Map<String, String> numericQueryANQMetricS;
	public static Map<String, String> numericQueryONQMetricS;
	
	public static void reset() {
		
		returnStandaloneCreateCollectionMap = null;
		returnCloudCreateCollectionMap = null;
		
		metricMapStandalone = null;

		metricMapCloudSerial_2N1S2R = null;
		metricMapCloudSerial_2N2S1R = null;
		metricMapCloudSerial_3N1S3R = null;
		metricMapCloudSerial_4N2S2R = null;
		
		metricMapCloudConcurrent2_2N1S2R = null;
		metricMapCloudConcurrent2_2N2S1R = null;
		metricMapCloudConcurrent2_3N1S3R = null;
		metricMapCloudConcurrent2_4N2S2R = null;
		
		metricMapCloudConcurrent4_2N1S2R = null;
		metricMapCloudConcurrent4_2N2S1R = null;
		metricMapCloudConcurrent4_3N1S3R = null;
		metricMapCloudConcurrent4_4N2S2R = null;

		metricMapCloudConcurrent6_2N1S2R = null;
		metricMapCloudConcurrent6_2N2S1R = null;
		metricMapCloudConcurrent6_3N1S3R = null;
		metricMapCloudConcurrent6_4N2S2R = null;

		metricMapCloudConcurrent8_2N1S2R = null;
		metricMapCloudConcurrent8_2N2S1R = null;
		metricMapCloudConcurrent8_3N1S3R = null;
		metricMapCloudConcurrent8_4N2S2R = null;

		metricMapCloudConcurrent10_2N1S2R = null;
		metricMapCloudConcurrent10_2N2S1R = null;
		metricMapCloudConcurrent10_3N1S3R = null;
		metricMapCloudConcurrent10_4N2S2R = null;
		
		metricMapStandaloneConcurrent2 = null;
		metricMapStandaloneConcurrent4 = null;
		metricMapStandaloneConcurrent6 = null;
		metricMapStandaloneConcurrent8 = null;
		metricMapStandaloneConcurrent10 = null;
		
		numericQueryTNQMetricC = null;
		numericQueryRNQMetricC = null;
		numericQueryLNQMetricC = null;
		numericQueryGNQMetricC = null;
		numericQueryANQMetricC = null;
		numericQueryONQMetricC = null;

		numericQueryTNQMetricS = null;
		numericQueryRNQMetricS = null;
		numericQueryLNQMetricS = null;
		numericQueryGNQMetricS = null;
		numericQueryANQMetricS = null;
		numericQueryONQMetricS = null;
		
	}

}
