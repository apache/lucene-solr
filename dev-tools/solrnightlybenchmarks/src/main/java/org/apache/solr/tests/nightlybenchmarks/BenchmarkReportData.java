/*
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

package org.apache.solr.tests.nightlybenchmarks;

import java.util.Map;

/**
 * This POJO class provides implementation for data object. 
 * @author Vivek Narang
 *
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

	public static Map<String, String> queryTNQMetricC_T1;
	public static Map<String, String> queryTNQMetricC_T2;
	public static Map<String, String> queryTNQMetricC_T3;
	public static Map<String, String> queryTNQMetricC_T4;
	
	public static Map<String, String> queryRNQMetricC_T1;
	public static Map<String, String> queryRNQMetricC_T2;
	public static Map<String, String> queryRNQMetricC_T3;
	public static Map<String, String> queryRNQMetricC_T4;

	public static Map<String, String> queryLNQMetricC_T1;
	public static Map<String, String> queryLNQMetricC_T2;
	public static Map<String, String> queryLNQMetricC_T3;
	public static Map<String, String> queryLNQMetricC_T4;

	public static Map<String, String> queryGNQMetricC_T1;
	public static Map<String, String> queryGNQMetricC_T2;
	public static Map<String, String> queryGNQMetricC_T3;
	public static Map<String, String> queryGNQMetricC_T4;

	public static Map<String, String> queryANQMetricC_T1;
	public static Map<String, String> queryANQMetricC_T2;
	public static Map<String, String> queryANQMetricC_T3;
	public static Map<String, String> queryANQMetricC_T4;

	public static Map<String, String> queryONQMetricC_T1;
	public static Map<String, String> queryONQMetricC_T2;
	public static Map<String, String> queryONQMetricC_T3;
	public static Map<String, String> queryONQMetricC_T4;

	public static Map<String, String> querySNQMetricC_T1;
	public static Map<String, String> querySNQMetricC_T2;
	public static Map<String, String> querySNQMetricC_T3;
	public static Map<String, String> querySNQMetricC_T4;

	public static Map<String, String> queryTTQMetricC_T1;
	public static Map<String, String> queryTTQMetricC_T2;
	public static Map<String, String> queryTTQMetricC_T3;
	public static Map<String, String> queryTTQMetricC_T4;

	public static Map<String, String> queryPTQMetricC_T1;
	public static Map<String, String> queryPTQMetricC_T2;
	public static Map<String, String> queryPTQMetricC_T3;
	public static Map<String, String> queryPTQMetricC_T4;

	public static Map<String, String> querySTQMetricC_T1;
	public static Map<String, String> querySTQMetricC_T2;
	public static Map<String, String> querySTQMetricC_T3;
	public static Map<String, String> querySTQMetricC_T4;

	public static Map<String, String> queryHTQMetricC_T1;
	public static Map<String, String> queryHTQMetricC_T2;
	public static Map<String, String> queryHTQMetricC_T3;
	public static Map<String, String> queryHTQMetricC_T4;

	public static Map<String, String> queryTNQMetricS_T1;
	public static Map<String, String> queryTNQMetricS_T2;
	public static Map<String, String> queryTNQMetricS_T3;
	public static Map<String, String> queryTNQMetricS_T4;
	
	public static Map<String, String> queryRNQMetricS_T1;
	public static Map<String, String> queryRNQMetricS_T2;
	public static Map<String, String> queryRNQMetricS_T3;
	public static Map<String, String> queryRNQMetricS_T4;
	
	public static Map<String, String> queryLNQMetricS_T1;
	public static Map<String, String> queryLNQMetricS_T2;
	public static Map<String, String> queryLNQMetricS_T3;
	public static Map<String, String> queryLNQMetricS_T4;
	
	public static Map<String, String> queryGNQMetricS_T1;
	public static Map<String, String> queryGNQMetricS_T2;
	public static Map<String, String> queryGNQMetricS_T3;
	public static Map<String, String> queryGNQMetricS_T4;
	
	public static Map<String, String> queryANQMetricS_T1;
	public static Map<String, String> queryANQMetricS_T2;
	public static Map<String, String> queryANQMetricS_T3;
	public static Map<String, String> queryANQMetricS_T4;
	
	public static Map<String, String> queryONQMetricS_T1;
	public static Map<String, String> queryONQMetricS_T2;
	public static Map<String, String> queryONQMetricS_T3;
	public static Map<String, String> queryONQMetricS_T4;
	
	public static Map<String, String> querySNQMetricS_T1;
	public static Map<String, String> querySNQMetricS_T2;
	public static Map<String, String> querySNQMetricS_T3;
	public static Map<String, String> querySNQMetricS_T4;
	
	public static Map<String, String> queryTTQMetricS_T1;
	public static Map<String, String> queryTTQMetricS_T2;
	public static Map<String, String> queryTTQMetricS_T3;
	public static Map<String, String> queryTTQMetricS_T4;
	
	public static Map<String, String> queryPTQMetricS_T1;
	public static Map<String, String> queryPTQMetricS_T2;
	public static Map<String, String> queryPTQMetricS_T3;
	public static Map<String, String> queryPTQMetricS_T4;
	
	public static Map<String, String> querySTQMetricS_T1;
	public static Map<String, String> querySTQMetricS_T2;
	public static Map<String, String> querySTQMetricS_T3;
	public static Map<String, String> querySTQMetricS_T4;
	
	public static Map<String, String> queryHTQMetricS_T1;
	public static Map<String, String> queryHTQMetricS_T2;
	public static Map<String, String> queryHTQMetricS_T3;
	public static Map<String, String> queryHTQMetricS_T4;
	
	public static Map<String, String> queryCTFQMetricS_T1;
	public static Map<String, String> queryCTFQMetricS_T2;
	public static Map<String, String> queryCTFQMetricS_T3;
	public static Map<String, String> queryCTFQMetricS_T4;
	
	public static Map<String, String> queryCRFQMetricS_T1;
	public static Map<String, String> queryCRFQMetricS_T2;
	public static Map<String, String> queryCRFQMetricS_T3;
	public static Map<String, String> queryCRFQMetricS_T4;

	public static Map<String, String> queryJTFQMetricS_T1;
	public static Map<String, String> queryJTFQMetricS_T2;
	public static Map<String, String> queryJTFQMetricS_T3;
	public static Map<String, String> queryJTFQMetricS_T4;
	
	public static Map<String, String> queryJRFQMetricS_T1;
	public static Map<String, String> queryJRFQMetricS_T2;
	public static Map<String, String> queryJRFQMetricS_T3;
	public static Map<String, String> queryJRFQMetricS_T4;
	
	public static Map<String, String> queryCTFQMetricC_T1;
	public static Map<String, String> queryCTFQMetricC_T2;
	public static Map<String, String> queryCTFQMetricC_T3;
	public static Map<String, String> queryCTFQMetricC_T4;
	
	public static Map<String, String> queryCRFQMetricC_T1;
	public static Map<String, String> queryCRFQMetricC_T2;
	public static Map<String, String> queryCRFQMetricC_T3;
	public static Map<String, String> queryCRFQMetricC_T4;

	public static Map<String, String> queryJTFQMetricC_T1;
	public static Map<String, String> queryJTFQMetricC_T2;
	public static Map<String, String> queryJTFQMetricC_T3;
	public static Map<String, String> queryJTFQMetricC_T4;
	
	public static Map<String, String> queryJRFQMetricC_T1;
	public static Map<String, String> queryJRFQMetricC_T2;
	public static Map<String, String> queryJRFQMetricC_T3;
	public static Map<String, String> queryJRFQMetricC_T4;
	

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
		
		queryTNQMetricC_T1 = null;
		queryTNQMetricC_T2 = null;
		queryTNQMetricC_T3 = null;
		queryTNQMetricC_T4 = null;
		
		queryRNQMetricC_T1 = null;
		queryRNQMetricC_T2 = null;
		queryRNQMetricC_T3 = null;
		queryRNQMetricC_T4 = null;

		queryLNQMetricC_T1 = null;
		queryLNQMetricC_T2 = null;
		queryLNQMetricC_T3 = null;
		queryLNQMetricC_T4 = null;

		queryGNQMetricC_T1 = null;
		queryGNQMetricC_T2 = null;
		queryGNQMetricC_T3 = null;
		queryGNQMetricC_T4 = null;

		queryANQMetricC_T1 = null;
		queryANQMetricC_T2 = null;
		queryANQMetricC_T3 = null;
		queryANQMetricC_T4 = null;

		queryONQMetricC_T1 = null;
		queryONQMetricC_T2 = null;
		queryONQMetricC_T3 = null;
		queryONQMetricC_T4 = null;

		querySNQMetricC_T1 = null;
		querySNQMetricC_T2 = null;
		querySNQMetricC_T3 = null;
		querySNQMetricC_T4 = null;

		queryTTQMetricC_T1 = null;
		queryTTQMetricC_T2 = null;
		queryTTQMetricC_T3 = null;
		queryTTQMetricC_T4 = null;

		queryPTQMetricC_T1 = null;
		queryPTQMetricC_T2 = null;
		queryPTQMetricC_T3 = null;
		queryPTQMetricC_T4 = null;

		querySTQMetricC_T1 = null;
		querySTQMetricC_T2 = null;
		querySTQMetricC_T3 = null;
		querySTQMetricC_T4 = null;

		queryHTQMetricC_T1 = null;
		queryHTQMetricC_T2 = null;
		queryHTQMetricC_T3 = null;
		queryHTQMetricC_T4 = null;
		
		queryTNQMetricS_T1 = null;
		queryTNQMetricS_T2 = null;
		queryTNQMetricS_T3 = null;
		queryTNQMetricS_T4 = null;
		
		queryRNQMetricS_T1 = null;
		queryRNQMetricS_T2 = null;
		queryRNQMetricS_T3 = null;
		queryRNQMetricS_T4 = null;
		
		queryLNQMetricS_T1 = null;
		queryLNQMetricS_T2 = null;
		queryLNQMetricS_T3 = null;
		queryLNQMetricS_T4 = null;
		
		queryGNQMetricS_T1 = null;
		queryGNQMetricS_T2 = null;
		queryGNQMetricS_T3 = null;
		queryGNQMetricS_T4 = null;
		
		queryANQMetricS_T1 = null;
		queryANQMetricS_T2 = null;
		queryANQMetricS_T3 = null;
		queryANQMetricS_T4 = null;
		
		queryONQMetricS_T1 = null;
		queryONQMetricS_T2 = null;
		queryONQMetricS_T3 = null;
		queryONQMetricS_T4 = null;
		
		querySNQMetricS_T1 = null;
		querySNQMetricS_T2 = null;
		querySNQMetricS_T3 = null;
		querySNQMetricS_T4 = null;
		
		queryTTQMetricS_T1 = null;
		queryTTQMetricS_T2 = null;
		queryTTQMetricS_T3 = null;
		queryTTQMetricS_T4 = null;
		
		queryPTQMetricS_T1 = null;
		queryPTQMetricS_T2 = null;
		queryPTQMetricS_T3 = null;
		queryPTQMetricS_T4 = null;
		
		querySTQMetricS_T1 = null;
		querySTQMetricS_T2 = null;
		querySTQMetricS_T3 = null;
		querySTQMetricS_T4 = null;
		
		queryHTQMetricS_T1 = null;
		queryHTQMetricS_T2 = null;
		queryHTQMetricS_T3 = null;
		queryHTQMetricS_T4 = null;
		
		queryCTFQMetricS_T1 = null;
		queryCTFQMetricS_T2 = null;
		queryCTFQMetricS_T3 = null;
		queryCTFQMetricS_T4 = null;
		
		queryCRFQMetricS_T1 = null;
		queryCRFQMetricS_T2 = null;
		queryCRFQMetricS_T3 = null;
		queryCRFQMetricS_T4 = null;
		
		queryJTFQMetricS_T1 = null;
		queryJTFQMetricS_T2 = null;
		queryJTFQMetricS_T3 = null;
		queryJTFQMetricS_T4 = null;
		
		queryJRFQMetricS_T1 = null;
		queryJRFQMetricS_T2 = null;
		queryJRFQMetricS_T3 = null;
		queryJRFQMetricS_T4 = null;
		
		queryCTFQMetricC_T1 = null;
		queryCTFQMetricC_T2 = null;
		queryCTFQMetricC_T3 = null;
		queryCTFQMetricC_T4 = null;
		
		queryCRFQMetricC_T1 = null;
		queryCRFQMetricC_T2 = null;
		queryCRFQMetricC_T3 = null;
		queryCRFQMetricC_T4 = null;
	
		queryJTFQMetricC_T1 = null;
		queryJTFQMetricC_T2 = null;
		queryJTFQMetricC_T3 = null;
		queryJTFQMetricC_T4 = null;
		
		queryJRFQMetricC_T1 = null;
		queryJRFQMetricC_T2 = null;
		queryJRFQMetricC_T3 = null;
		queryJRFQMetricC_T4 = null;
		
	}
}