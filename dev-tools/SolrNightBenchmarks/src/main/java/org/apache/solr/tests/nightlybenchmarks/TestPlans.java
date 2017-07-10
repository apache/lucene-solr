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

/**
 * 
 * @author Vivek Narang
 *
 */
public class TestPlans {

	/**
	 * A method describing a test plan for benchmarking.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void execute() throws IOException, InterruptedException {

		Util.postMessage("** Executing the benchmark test plan ...", MessageType.BLUE_TEXT, false);

		Tests.indexingTestsStandalone(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, ActionType.PARTIAL_UPDATE);
/*		Tests.createCollectionTestStandalone(Util.COMMIT_ID);
		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS);
		
		Tests.queryTestsStandalone(Util.TEST_WITH_NUMBER_OF_DOCUMENTS);

		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "1", "2");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "2", "1");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 3, "1", "3");
		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 4, "2", "2");

		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "1", "2");
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 2, "2", "1");
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 3, "1", "3");
		Tests.indexingTestsCloudConcurrentCustomClient(Util.COMMIT_ID, Util.TEST_WITH_NUMBER_OF_DOCUMENTS, 4, "2", "2");

		Tests.queryTestsCloud(Util.TEST_WITH_NUMBER_OF_DOCUMENTS);
*/
		Util.postMessage("** Executing the benchmark test plan [COMPLETE]...", MessageType.BLUE_TEXT, false);

	}

}
