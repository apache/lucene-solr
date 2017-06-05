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

public class SolrNightlyBenchmarks {

	public static void main(String[] args) throws IOException, InterruptedException {

		
		Util.init(args);

//		Tests.indexingTestsStandalone(Util.COMMIT_ID, 10000);
		
//		Tests.createCollectionTestStandalone(Util.COMMIT_ID);

//		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, 10000);

//		Tests.runNumericQueryTestsStandalone();

//		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, 10000);

//		Tests.indexingTestsCloudConcurrent(Util.COMMIT_ID, 10000);
		
//		Tests.runNumericTestsCloud();
		
		Tests.runNumericQueryTestsStandalone();
		
		BenchmarkAppConnector.publishDataForWebApp();
		
		Util.destroy();
	}
	
}