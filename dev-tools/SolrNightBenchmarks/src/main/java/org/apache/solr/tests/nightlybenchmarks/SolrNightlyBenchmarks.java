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

import java.io.IOException;

import org.apache.solr.tests.nightlybenchmarks.QueryClient.QueryType;

public class SolrNightlyBenchmarks {

	public static void main(String[] args) throws IOException, InterruptedException {

		Util.init(args);

		Tests.indexingTests(Util.COMMIT_ID, 10000);

		Tests.setUpCloudForFeatureTests(Util.COMMIT_ID, 50000, 1, "2", "2");
		BenchmarkReportData.numericQueryTNQMetric = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.TERM_NUMERIC_QUERY, 1, 180, 120);
		BenchmarkReportData.numericQueryRNQMetric = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.RANGE_NUMERIC_QUERY, 1, 180, 120);
		BenchmarkReportData.numericQueryLNQMetric = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.LESS_THAN_NUMERIC_QUERY, 1, 180, 120);
		BenchmarkReportData.numericQueryGNQMetric = Tests.numericQueryTests(Util.COMMIT_ID,
				QueryType.GREATER_THAN_NUMERIC_QUERY, 1, 180, 120);
		Tests.shutDown();
		BenchmarkAppConnector.publishDataForWebApp();

		Util.destroy();
	}
}