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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class BenchmarkAppConnector {

	public static String benchmarkAppDirectory;

	public BenchmarkAppConnector() {
		super();
	}

	public enum FileType {

		MEMORY_HEAP_USED, PROCESS_CPU_LOAD, TEST_ENV_FILE, STANDALONE_INDEXING_MAIN, STANDALONE_CREATE_COLLECTION_MAIN, STANDALONE_INDEXING_THROUGHPUT, CLOUD_CREATE_COLLECTION_MAIN, CLOUD_SERIAL_INDEXING_THROUGHPUT, CLOUD_CONCURRENT_INDEXING_THROUGHPUT, CLOUD_INDEXING_SERIAL, CLOUD_INDEXING_CONCURRENT, NUMERIC_QUERY_STANDALONE, NUMERIC_QUERY_CLOUD, LAST_RUN_COMMIT, IS_RUNNING_FILE

	}

	public static String getLastRunCommitID() {
		File dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
		return dataDir.listFiles()[0].getName().trim();
	}

	public static boolean isRunningFolderEmpty() {
		return new File(benchmarkAppDirectory + "data" + File.separator + "running" + File.separator)
				.listFiles().length == 0 ? true : false;
	}

	public static void deleteFile(FileType type) {

		if (type == FileType.LAST_RUN_COMMIT) {
			Util.execute(
					"rm -r -f " + BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "lastrun"
							+ File.separator + "*",
					BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
		} else if (type == FileType.IS_RUNNING_FILE) {
			Util.execute(
					"rm -r -f " + BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "running"
							+ File.separator + "*",
					BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);
		}
	}

	public static void writeToWebAppDataFile(String fileName, String data, boolean createNewFile, FileType type) {

		BufferedWriter bw = null;
		FileWriter fw = null;
		File file = null;

		try {

			File dataDir = null;

			if (type == FileType.IS_RUNNING_FILE) {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);
			} else if (type == FileType.LAST_RUN_COMMIT) {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
			} else {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator);
			}

			if (!dataDir.exists()) {
				dataDir.mkdir();
			}

			if (type == FileType.IS_RUNNING_FILE) {
				file = new File(
						benchmarkAppDirectory + "data" + File.separator + "running" + File.separator + fileName);
			} else if (type == FileType.LAST_RUN_COMMIT) {
				file = new File(
						benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator + fileName);
			} else {
				file = new File(benchmarkAppDirectory + "data" + File.separator + fileName);
			}
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);

			if (file.exists() && createNewFile) {
				file.delete();
				file.createNewFile();
			}

			if (!file.exists()) {
				file.createNewFile();
			}

			if (file.length() == 0) {

				file.setReadable(true);
				file.setWritable(true);
				if (type == FileType.MEMORY_HEAP_USED) {
					bw.write("Date, Test_ID, Heap Space Used (MB)\n");
				} else if (type == FileType.PROCESS_CPU_LOAD) {
					bw.write("Date, Test_ID, Process CPU Load (%)\n");
				} else if (type == FileType.STANDALONE_CREATE_COLLECTION_MAIN
						|| type == FileType.CLOUD_CREATE_COLLECTION_MAIN) {
					bw.write("Date, Test_ID, Milliseconds, CommitID\n");
				} else if (type == FileType.STANDALONE_INDEXING_MAIN || type == FileType.CLOUD_INDEXING_SERIAL) {
					bw.write("Date, Test_ID, Seconds, CommitID\n");
				} else if (type == FileType.TEST_ENV_FILE) {
					// Don't add any header
				} else if (type == FileType.STANDALONE_INDEXING_THROUGHPUT
						|| type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT) {
					bw.write("Date, Test_ID, Throughput (doc/sec), CommitID\n");
				} else if (type == FileType.CLOUD_INDEXING_CONCURRENT) {
					bw.write(
							"Date, Test_ID, CommitID, Seconds (2 Threads), Seconds (4 Threads), Seconds (6 Threads), Seconds (8 Threads), Seconds (10 Threads)\n");
				} else if (type == FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT) {
					bw.write(
							"Date, Test_ID, CommitID, Throughput (2 Threads), Throughput (4 Threads), Throughput (6 Threads), Throughput (8 Threads), Throughput (10 Threads) \n");
				} else if (type == FileType.NUMERIC_QUERY_CLOUD || type == FileType.NUMERIC_QUERY_STANDALONE) {
					bw.write(
							"Date, Test_ID, CommitID, QPS(Term), QTime-Min(Term), QTime-Max(Term), QPS(Range), QTime-Min(Range), QTime-Max(Range), QPS(Less Than), QTime-Min(Less Than), QTime-Max(Less Than), QPS(Greater Than), QTime-Min(Greater Than), QTime-Max(Greater Than)\n");
				}
			}

			bw.write(data + "\n");

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}

	public static void publishDataForWebApp() {

		Util.postMessage("** Publishing data for webapp ..", MessageType.WHITE_TEXT, false);

		if (BenchmarkReportData.metricMapStandalone != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_standalone_regular.csv",
					BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandalone.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapStandalone.get("CommitID"),
					false, FileType.STANDALONE_INDEXING_MAIN);
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv",
					BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandalone.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandalone.get("CommitID"),
					false, FileType.STANDALONE_INDEXING_THROUGHPUT);

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_standalone_regular.csv",
					BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandalone.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapStandalone.get("CommitID"),
					false, FileType.STANDALONE_INDEXING_MAIN);
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv",
					BenchmarkReportData.metricMapStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandalone.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandalone.get("CommitID"),
					false, FileType.STANDALONE_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapCloudSerial != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_cloud_regular.csv",
					BenchmarkReportData.metricMapCloudSerial.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapCloudSerial.get("CommitID"),
					false, FileType.CLOUD_INDEXING_SERIAL);
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial.csv",
					BenchmarkReportData.metricMapCloudSerial.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapCloudConcurrent2 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_cloud_concurrent.csv",
					BenchmarkReportData.metricMapCloudConcurrent2.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent4.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent6.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent8.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent10.get("IndexingTime"),
					false, FileType.CLOUD_INDEXING_CONCURRENT);
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent.csv",
					BenchmarkReportData.metricMapCloudConcurrent2.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent4.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent6.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent8.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent10.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.returnStandaloneCreateCollectionMap != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_standalone_regular.csv",
					BenchmarkReportData.returnStandaloneCreateCollectionMap.get("TimeStamp") + ", " + Util.TEST_ID
							+ ", " + BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CreateCollectionTime")
							+ ", " + BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CommitID"),
					false, FileType.STANDALONE_CREATE_COLLECTION_MAIN);
		}

		if (BenchmarkReportData.returnCloudCreateCollectionMap != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_cloud_regular.csv",
					BenchmarkReportData.returnCloudCreateCollectionMap.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.returnCloudCreateCollectionMap.get("CreateCollectionTime") + ", "
							+ BenchmarkReportData.returnCloudCreateCollectionMap.get("CommitID"),
					false, FileType.CLOUD_CREATE_COLLECTION_MAIN);
		}

		if (BenchmarkReportData.numericQueryTNQMetricC != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.numericQueryTNQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.numericQueryTNQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.numericQueryTNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryTNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryTNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.numericQueryRNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryRNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryRNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.numericQueryLNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryLNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryLNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.numericQueryGNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryGNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryGNQMetricC.get("MaxQTime"),
					false, FileType.NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.numericQueryTNQMetricS != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.numericQueryTNQMetricS.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.numericQueryTNQMetricS.get("CommitID") + ", "
							+ BenchmarkReportData.numericQueryTNQMetricS.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryTNQMetricS.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryTNQMetricS.get("MaxQTime") + ", "
							+ BenchmarkReportData.numericQueryRNQMetricS.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryRNQMetricS.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryRNQMetricS.get("MaxQTime") + ", "
							+ BenchmarkReportData.numericQueryLNQMetricS.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryLNQMetricS.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryLNQMetricS.get("MaxQTime") + ", "
							+ BenchmarkReportData.numericQueryGNQMetricS.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.numericQueryGNQMetricS.get("MinQTime") + ", "
							+ BenchmarkReportData.numericQueryGNQMetricS.get("MaxQTime"),
					false, FileType.NUMERIC_QUERY_STANDALONE);
		}

		Util.postMessage("** Publishing data for webapp [COMPLETE] ..", MessageType.WHITE_TEXT, false);

	}

}