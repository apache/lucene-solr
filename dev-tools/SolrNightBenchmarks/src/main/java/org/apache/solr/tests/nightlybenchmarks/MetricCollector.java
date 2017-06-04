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

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.MediaType;

import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

enum TestType {

	STANDALONE_CREATE_COLLECTION, STANDALONE_INDEXING, CLOUD_CREATE_COLLECTION, CLOUD_INDEXING_REGULAR, CLOUD_INDEXING_CONCURRENT

}

public class MetricCollector extends Thread {

	public static String metricsURL;

	public MetricCollector(String commitID, TestType testType, String port) {
		this.testType = testType;
		this.commitID = commitID;
		this.port = port;
	}

	public enum MetricType {
		MEM_ESTIMATION, CPU_ESTIMATION
	}

	public enum MetricSubType {
		MEMORY_HEAP_USED, PROCESS_CPU_LOAD
	}

	public TestType testType;
	public String commitID;
	public String port;

	public void run() {

			while (true) {
				try {

					String response = Util.getResponse(
							"http://localhost:" + this.port + "/solr/admin/metrics?wt=json&group=jvm",
							MediaType.APPLICATION_JSON);
					JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

					Date dNow = new Date();
					SimpleDateFormat ft = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					BenchmarkAppConnector.writeToWebAppDataFile(
							Util.TEST_ID + "_" + this.commitID + "_" + MetricType.MEM_ESTIMATION + "_" + testType
									+ "_dump.csv",
							ft.format(dNow) + ", " + Util.TEST_ID + ", "
									+ (Double.parseDouble(
											((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm"))
													.get("memory.heap.used").toString())
											/ (1024 * 1024)),
							false, FileType.MEMORY_HEAP_USED);
					BenchmarkAppConnector.writeToWebAppDataFile(
							Util.TEST_ID + "_" + this.commitID + "_" + MetricType.CPU_ESTIMATION + "_" + testType
									+ "_dump.csv",
							ft.format(dNow) + ", " + Util.TEST_ID + ", "
									+ (Double.parseDouble(
											((JSONObject) ((JSONObject) jsonObject.get("metrics")).get("solr.jvm"))
													.get("os.processCpuLoad").toString())
											* 100),
							false, FileType.PROCESS_CPU_LOAD);

					Thread.sleep(Integer.parseInt(Util.METRIC_ESTIMATION_PERIOD));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

	}
}
