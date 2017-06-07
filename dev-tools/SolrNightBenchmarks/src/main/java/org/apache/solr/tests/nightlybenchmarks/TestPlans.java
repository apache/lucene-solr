package org.apache.solr.tests.nightlybenchmarks;

import java.io.IOException;

public class TestPlans {
	
	public static void execute(String commitID) throws IOException, InterruptedException {

		Util.postMessage("** Executing the benchmark test plan ...", MessageType.BLUE_TEXT, false);
		
		Util.COMMIT_ID = commitID;
		
		Tests.indexingTestsStandalone(Util.COMMIT_ID, 10000);
		
		Tests.createCollectionTestStandalone(Util.COMMIT_ID);

		Tests.indexingTestsStandaloneConcurrent(Util.COMMIT_ID, 10000);

		Tests.runNumericQueryTestsStandalone();

		Tests.indexingTestsCloudSerial(Util.COMMIT_ID, 10000);

		Tests.indexingTestsCloudConcurrent(Util.COMMIT_ID, 10000);
		
		Tests.runNumericTestsCloud();
		
		Util.postMessage("** Executing the benchmark test plan [COMPLETE]...", MessageType.BLUE_TEXT, false);
		
	}

}
