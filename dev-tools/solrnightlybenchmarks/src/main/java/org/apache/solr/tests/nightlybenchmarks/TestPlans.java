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

import org.apache.log4j.Logger;

/**
 * This class provides the Test Plan for Solr Standalone and Solr Cloud.
 * 
 * @author Vivek Narang
 *
 */
public class TestPlans {

	public final static Logger logger = Logger.getLogger(TestPlans.class);

	public enum BenchmarkTestType {
		PROD_TEST, DEV_TEST
	}

	/**
	 * A method describing a test plan for benchmarking.
	 * 
	 * @throws Exception
	 */
	public static void execute() throws Exception {

		logger.info("Executing the benchmark test plan ...");

		Tests.indexingTestsStandalone(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, ActionType.INDEX);
		Tests.createCollectionTestStandalone(Util.COMMIT_ID);
		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, ActionType.INDEX);

		Tests.indexingTestsStandalone(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, ActionType.PARTIAL_UPDATE);
		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS,
				ActionType.PARTIAL_UPDATE);

		Tests.queryTestsStandalone(Util.TEST_WITH_NUMBER_OF_DOCUMENTS, BenchmarkTestType.PROD_TEST,
				Util.NUMBER_OF_QUERIES_TO_RUN);

		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "1", "2");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "2", "1");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 3, "1", "3");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 4, "2", "2");

		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "1", "2",
				BenchmarkTestType.PROD_TEST);
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "2", "1",
				BenchmarkTestType.PROD_TEST);
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 3, "1", "3",
				BenchmarkTestType.PROD_TEST);
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 4, "2", "2",
				BenchmarkTestType.PROD_TEST);

		Tests.queryTestsCloud(Util.TEST_WITH_NUMBER_OF_DOCUMENTS, BenchmarkTestType.PROD_TEST,
				Util.NUMBER_OF_QUERIES_TO_RUN);

		logger.info("Executing the benchmark test plan [COMPLETE] ...");

	}

	/**
	 * A test plan for use in the dev mode. 
	 * @param load
	 * @throws Exception
	 */
	public static void sampleTest(double load) throws Exception {

		logger.info("Executing the benchmark sanity test plan: load " + load + " number of documents: "
				+ (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load));

		Tests.indexingTestsStandalone(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load),
				ActionType.INDEX);
		Tests.createCollectionTestStandalone(Util.COMMIT_ID);
		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load),
				ActionType.INDEX);

		Tests.indexingTestsStandalone(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load),
				ActionType.PARTIAL_UPDATE);
		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load),
				ActionType.PARTIAL_UPDATE);

		Tests.queryTestsStandalone((long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), BenchmarkTestType.DEV_TEST,
				(long) (Util.NUMBER_OF_QUERIES_TO_RUN * load));

		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 2, "1", "2");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 2, "2", "1");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 3, "1", "3");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, (long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 4, "2", "2");

		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID,
				(long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 2, "1", "2", BenchmarkTestType.DEV_TEST);
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID,
				(long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 2, "2", "1", BenchmarkTestType.DEV_TEST);
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID,
				(long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 3, "1", "3", BenchmarkTestType.DEV_TEST);
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID,
				(long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), 4, "2", "2", BenchmarkTestType.DEV_TEST);

		Tests.queryTestsCloud((long) (Util.TEST_WITH_NUMBER_OF_DOCUMENTS * load), BenchmarkTestType.DEV_TEST,
				(long) (Util.NUMBER_OF_QUERIES_TO_RUN * load));

		logger.info("Executing the benchmark sanity test plan [COMPLETE] ...");

	}
}