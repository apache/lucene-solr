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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.comparator.LastModifiedFileComparator;

/**
 * 
 * @author Vivek Narang
 *
 */
public class BenchmarkAppConnector {

	public static String benchmarkAppDirectory;

	public BenchmarkAppConnector() {
		super();
	}

	/**
	 * An enum defining various file types.
	 */
	public enum FileType {

		MEMORY_HEAP_USED, 
		PROCESS_CPU_LOAD, 
		TEST_ENV_FILE, 
		STANDALONE_INDEXING_MAIN, 
		STANDALONE_CREATE_COLLECTION_MAIN, 
		STANDALONE_INDEXING_THROUGHPUT, 
		CLOUD_CREATE_COLLECTION_MAIN, 
		CLOUD_SERIAL_INDEXING_THROUGHPUT, 
		CLOUD_CONCURRENT_INDEXING_THROUGHPUT, 
		CLOUD_INDEXING_SERIAL, 
		CLOUD_INDEXING_CONCURRENT, 
		TERM_NUMERIC_QUERY_STANDALONE, 
		TERM_NUMERIC_QUERY_CLOUD, 
		RANGE_NUMERIC_QUERY_STANDALONE, 
		RANGE_NUMERIC_QUERY_CLOUD, 
		GT_NUMERIC_QUERY_STANDALONE, 
		GT_NUMERIC_QUERY_CLOUD, 
		LT_NUMERIC_QUERY_STANDALONE, 
		LT_NUMERIC_QUERY_CLOUD, 
		AND_NUMERIC_QUERY_STANDALONE, 
		AND_NUMERIC_QUERY_CLOUD, 
		OR_NUMERIC_QUERY_STANDALONE, 
		OR_NUMERIC_QUERY_CLOUD,		
		SORTING_NUMERIC_QUERY_STANDALONE, 
		SORTING_NUMERIC_QUERY_CLOUD, 
		LAST_RUN_COMMIT, 
		IS_RUNNING_FILE, 
		COMMIT_INFORMATION_FILE, 
		IS_CLONING_FILE, 
		COMMIT_QUEUE, 
		TEXT_TERM_QUERY_CLOUD, 
		TEXT_TERM_QUERY_STANDALONE, 
		TEXT_PHRASE_QUERY_CLOUD, 
		TEXT_PHRASE_QUERY_STANDALONE, 
		SORTING_TEXT_QUERY_STANDALONE, 
		SORTING_TEXT_QUERY_CLOUD,
		HIGHLIGHTING_QUERY_STANDALONE,
		HIGHLIGHTING_QUERY_CLOUD,
		PARTIAL_UPDATE_HTTP_STANDALONE,
		PARTIAL_UPDATE_CONCURRENT_STANDALONE,
		PARTIAL_UPDATE_CLOUD

	}

	/**
	 * A method used for getting the last registered commit.
	 * 
	 * @return String
	 */
	public static String getLastRunCommitID() {

		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);

		if (!dir.exists()) {
			dir.mkdirs();
		}

		File dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
		if (dataDir.listFiles().length != 0) {
			return dataDir.listFiles()[0].getName().trim();
		} else {
			return null;
		}
	}

	/**
	 * A method used for checking if the 'running' folder is empty.
	 * 
	 * @return boolean
	 */
	public static boolean isRunningFolderEmpty() {

		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);

		if (!dir.exists()) {
			dir.mkdirs();
		}

		return dir.listFiles().length == 0 ? true : false;
	}

	/**
	 * A method used to check if the 'cloning' folder is empty.
	 * 
	 * @return boolean
	 */
	public static boolean isCloningFolderEmpty() {

		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "cloning" + File.separator);

		if (!dir.exists()) {
			dir.mkdirs();
		}

		return dir.listFiles().length == 0 ? true : false;
	}

	/**
	 * A method used to check if the commit queue is empty.
	 * 
	 * @return boolean
	 */
	public static boolean isCommitQueueEmpty() {

		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);

		if (!dir.exists()) {
			dir.mkdirs();
		}

		return dir.listFiles().length == 0 ? true : false;
	}

	/**
	 * A method used to delete a specific commit from the queue.
	 * 
	 * @param commit
	 * @return boolean
	 */
	public static boolean deleteCommitFromQueue(String commit) {

		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);

		if (!dir.exists()) {
			dir.mkdirs();
		}

		Util.postMessage("** Deleting registered commit " + commit + " from the queue ...", MessageType.RED_TEXT,
				false);
		File file = new File(
				benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator + commit);
		return file.delete();

	}

	/**
	 * A method used to get the oldest commit from the queue.
	 * 
	 * @return String
	 */
	public static String getOldestCommitFromQueue() {

		File directory = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);

		if (!directory.exists()) {
			directory.mkdirs();
		}

		File[] files = directory.listFiles();

		if (files.length == 0) {
			return null;
		}

		Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);
		return files[0].getName();
	}

	/**
	 * A method used to get the array of registered commits from the queue.
	 * 
	 * @return File Array
	 */
	public static File[] getRegisteredCommitsFromQueue() {

		File directory = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);

		if (!directory.exists()) {
			directory.mkdirs();
		}

		File[] files = directory.listFiles();

		Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);
		Util.postMessage("** Number of registered commits in the queue: " + files.length, MessageType.RED_TEXT, false);

		return files;
	}

	/**
	 * A method used to check if a particular commit is in the queue.
	 * 
	 * @param commit
	 * @return boolean
	 */
	public static boolean isCommitInQueue(String commit) {

		File file = new File(
				benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator + commit);

		if (!file.exists()) {
			file.mkdirs();
		}

		return file.exists();

	}

	/**
	 * A method used to delete a specific folder.
	 * 
	 * @param type
	 */
	public static void deleteFolder(FileType type) {

		if (type == FileType.LAST_RUN_COMMIT) {
			File dir = new File(
					BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);

			if (!dir.exists()) {
				dir.mkdirs();
			} else {
				for (File file : dir.listFiles()) {
					if (!file.isDirectory())
						file.delete();
				}
			}
		} else if (type == FileType.IS_RUNNING_FILE) {
			File dir = new File(
					BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);

			if (!dir.exists()) {
				dir.mkdirs();
			} else {
				for (File file : dir.listFiles()) {
					if (!file.isDirectory())
						file.delete();
				}
			}
		} else if (type == FileType.IS_CLONING_FILE) {
			File dir = new File(
					BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "cloning" + File.separator);

			if (!dir.exists()) {
				dir.mkdirs();
			} else {
				for (File file : dir.listFiles()) {
					if (!file.isDirectory())
						file.delete();
				}
			}
		} else if (type == FileType.COMMIT_QUEUE) {
			File dir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "commit_queue"
					+ File.separator);

			if (!dir.exists()) {
				dir.mkdirs();
			} else {
				for (File file : dir.listFiles()) {
					if (!file.isDirectory())
						file.delete();
				}
			}
		}
	}

	/**
	 * A method used to write new files or append to a file for the WebApp.
	 * 
	 * @param fileName
	 * @param data
	 * @param createNewFile
	 * @param type
	 */
	public static void writeToWebAppDataFile(String fileName, String data, boolean createNewFile, FileType type) {

		File dataDir = null;
		File file = null;
		FileWriter fw = null;

		try {

			if (type == FileType.IS_RUNNING_FILE) {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);
			} else if (type == FileType.LAST_RUN_COMMIT) {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
			} else if (type == FileType.IS_CLONING_FILE) {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "cloning" + File.separator);
			} else if (type == FileType.COMMIT_QUEUE) {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);
			} else {
				dataDir = new File(benchmarkAppDirectory + "data" + File.separator);
			}

			if (!dataDir.exists()) {
				dataDir.mkdirs();
			}

			if (type == FileType.IS_RUNNING_FILE) {
				file = new File(
						benchmarkAppDirectory + "data" + File.separator + "running" + File.separator + fileName);
			} else if (type == FileType.LAST_RUN_COMMIT) {
				file = new File(
						benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator + fileName);
			} else if (type == FileType.IS_CLONING_FILE) {
				file = new File(
						benchmarkAppDirectory + "data" + File.separator + "cloning" + File.separator + fileName);
			} else if (type == FileType.COMMIT_QUEUE) {
				file = new File(
						benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator + fileName);
			} else {
				file = new File(benchmarkAppDirectory + "data" + File.separator + fileName);
			}

			if (file.exists() && createNewFile) {
				file.delete();
				file.createNewFile();
			}

			if (!file.exists()) {
				file.createNewFile();
			}

			fw = new FileWriter(file, true);

			if (file.length() == 0) {

				file.setReadable(true);
				file.setWritable(true);
				if (type == FileType.MEMORY_HEAP_USED) {
					fw.write("Date, Test_ID, Heap Space Used (MB)\n");
				} else if (type == FileType.PROCESS_CPU_LOAD) {
					fw.write("Date, Test_ID, Process CPU Load (%)\n");
				} else if (type == FileType.STANDALONE_CREATE_COLLECTION_MAIN
						|| type == FileType.CLOUD_CREATE_COLLECTION_MAIN) {
					fw.write("Date, Test_ID, Seconds, CommitID\n");
				} else if (type == FileType.STANDALONE_INDEXING_MAIN || type == FileType.CLOUD_INDEXING_SERIAL) {
					fw.write("Date, Test_ID, Seconds, CommitID\n");
				} else if (type == FileType.TEST_ENV_FILE || type == FileType.COMMIT_INFORMATION_FILE
						|| type == FileType.COMMIT_QUEUE) {
					// Don't add any header
				} else if (type == FileType.STANDALONE_INDEXING_THROUGHPUT
						|| type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT 
						|| type == FileType.PARTIAL_UPDATE_HTTP_STANDALONE
						|| type == FileType.PARTIAL_UPDATE_CLOUD) {
					fw.write("Date, Test_ID, Throughput (doc/sec), CommitID\n");
				} else if (type == FileType.CLOUD_INDEXING_CONCURRENT) {
					fw.write(
							"Date, Test_ID, CommitID, Seconds (1 Threads), Seconds (2 Threads), Seconds (3 Threads)\n");
				} else if (type == FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT || type == FileType.PARTIAL_UPDATE_CONCURRENT_STANDALONE) {
					fw.write(
							"Date, Test_ID, CommitID, Throughput (1 Threads), Throughput (2 Threads), Throughput (3 Threads)\n");
				} else if (type == FileType.TERM_NUMERIC_QUERY_CLOUD || type == FileType.TERM_NUMERIC_QUERY_STANDALONE
						|| type == FileType.RANGE_NUMERIC_QUERY_CLOUD || type == FileType.RANGE_NUMERIC_QUERY_STANDALONE
						|| type == FileType.GT_NUMERIC_QUERY_CLOUD || type == FileType.GT_NUMERIC_QUERY_STANDALONE
						|| type == FileType.LT_NUMERIC_QUERY_CLOUD || type == FileType.LT_NUMERIC_QUERY_STANDALONE
						|| type == FileType.AND_NUMERIC_QUERY_CLOUD || type == FileType.AND_NUMERIC_QUERY_STANDALONE
						|| type == FileType.OR_NUMERIC_QUERY_CLOUD || type == FileType.OR_NUMERIC_QUERY_STANDALONE) {
					fw.write(
							"Date, Test_ID, CommitID, QPS(2 Threads), QTime-Min(2 Threads), QTime-Max(2 Threads), QTime-75th-Percentile(2 Threads), QTime-95th-Percentile(2 Threads), QTime-99th-Percentile(2 Threads), QTime-99.9th-Percentile(2 Threads), QPS(4 Threads), QTime-Min(4 Threads), QTime-Max(4 Threads), QTime-75th-Percentile(4 Threads), QTime-95th-Percentile(4 Threads), QTime-99th-Percentile(4 Threads), QTime-99.9th-Percentile(4 Threads), QPS(6 Threads), QTime-Min(6 Threads), QTime-Max(6 Threads), QTime-75th-Percentile(6 Threads), QTime-95th-Percentile(6 Threads), QTime-99th-Percentile(6 Threads), QTime-99.9th-Percentile(6 Threads), QPS(8 Threads), QTime-Min(8 Threads), QTime-Max(8 Threads), QTime-75th-Percentile(8 Threads), QTime-95th-Percentile(8 Threads), QTime-99th-Percentile(8 Threads), QTime-99.9th-Percentile(8 Threads)\n");
				} else if (type == FileType.SORTING_NUMERIC_QUERY_STANDALONE
						|| type == FileType.SORTING_NUMERIC_QUERY_CLOUD || type == FileType.TEXT_PHRASE_QUERY_CLOUD
						|| type == FileType.TEXT_PHRASE_QUERY_STANDALONE || type == FileType.TEXT_TERM_QUERY_CLOUD
						|| type == FileType.TEXT_TERM_QUERY_STANDALONE || type == FileType.HIGHLIGHTING_QUERY_CLOUD
						|| type == FileType.HIGHLIGHTING_QUERY_STANDALONE) {
					fw.write(
							"Date, Test_ID, CommitID, QPS(2 Threads), QTime-Min(2 Threads), QTime-Max(2 Threads), QTime-75th-Percentile(2 Threads), QTime-95th-Percentile(2 Threads), QTime-99th-Percentile(2 Threads), QTime-99.9th-Percentile(2 Threads), QPS(4 Threads), QTime-Min(4 Threads), QTime-Max(4 Threads), QTime-75th-Percentile(4 Threads), QTime-95th-Percentile(4 Threads), QTime-99th-Percentile(4 Threads), QTime-99.9th-Percentile(4 Threads), QPS(6 Threads), QTime-Min(6 Threads), QTime-Max(6 Threads), QTime-75th-Percentile(6 Threads), QTime-95th-Percentile(6 Threads), QTime-99th-Percentile(6 Threads), QTime-99.9th-Percentile(6 Threads), QPS(8 Threads), QTime-Min(8 Threads), QTime-Max(8 Threads), QTime-75th-Percentile(8 Threads), QTime-95th-Percentile(8 Threads), QTime-99th-Percentile(8 Threads), QTime-99.9th-Percentile(8 Threads)\n");
				}
			}

			fw.write(data + "\n");

		} catch (IOException ioe) {
			Util.postMessage(ioe.getMessage(), MessageType.RED_TEXT, false);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * A method which publishes data to the WebApp once the cycle completes.
	 */
	public static void publishDataForWebApp() {

		Util.postMessage("** Publishing data for webapp ..", MessageType.CYAN_TEXT, false);

		if (BenchmarkReportData.metricMapIndexingStandalone != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv",
					BenchmarkReportData.metricMapIndexingStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapIndexingStandalone.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapIndexingStandalone.get("CommitID"),
					false, FileType.STANDALONE_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapCloudSerial_2N1S2R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_2n1s2r.csv",
					BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudSerial_2N2S1R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_2n2s1r.csv",
					BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudSerial_3N1S3R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_3n1s3r.csv",
					BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudSerial_4N2S2R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_4n2s2r.csv",
					BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_2n1s2r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_2n2s1r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_3n1s3r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_4n2s2r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapStandaloneIndexingConcurrent1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_concurrent.csv",
					BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent2.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent3.get("IndexingThroughput"),
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

		if (BenchmarkReportData.queryTNQMetricC != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryTNQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC.get("99.9thQtime"),

					false, FileType.TERM_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryTNQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("term_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.queryTNQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricS_T4.get("99.9thQtime"),
					false, FileType.TERM_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryRNQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("range_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.queryRNQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricS_T4.get("99.9thQtime"),
					false, FileType.RANGE_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryLNQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("lt_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.queryLNQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricS_T4.get("99.9thQtime"),
					false, FileType.LT_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryGNQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("gt_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.queryGNQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricS_T4.get("99.9thQtime"),
					false, FileType.GT_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryANQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("and_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.queryANQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricS_T4.get("99.9thQtime"),
					false, FileType.AND_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryONQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("or_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.queryONQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricS_T4.get("99.9thQtime"),
					false, FileType.OR_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.querySNQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("sorting_numeric_query_benchmark_standalone.csv",
					BenchmarkReportData.querySNQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricS_T4.get("99.9thQtime"),
					false, FileType.SORTING_NUMERIC_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryTTQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("term_text_query_benchmark_standalone.csv",
					BenchmarkReportData.queryTTQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricS_T4.get("99.9thQtime"),
					false, FileType.TEXT_TERM_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryPTQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("phrase_text_query_benchmark_standalone.csv",
					BenchmarkReportData.queryPTQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricS_T4.get("99.9thQtime"),
					false, FileType.TEXT_PHRASE_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.querySTQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("sorting_text_query_benchmark_standalone.csv",
					BenchmarkReportData.querySTQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricS_T4.get("99.9thQtime"),
					false, FileType.SORTING_TEXT_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.queryHTQMetricS_T1 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("highlighting_text_query_benchmark_standalone.csv",
					BenchmarkReportData.queryHTQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricS_T4.get("99.9thQtime"),
					false, FileType.HIGHLIGHTING_QUERY_STANDALONE);
		}

		if (BenchmarkReportData.querySNQMetricC != null) {

			BenchmarkAppConnector.writeToWebAppDataFile("sorting_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.querySNQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.querySNQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC.get("99.9thQtime"),

					false, FileType.SORTING_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryTTQMetricC != null) {

			BenchmarkAppConnector.writeToWebAppDataFile("text_term_query_cloud.csv",
					BenchmarkReportData.queryTTQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC.get("99.9thQtime"),

					false, FileType.TEXT_TERM_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryPTQMetricC != null) {

			BenchmarkAppConnector.writeToWebAppDataFile("text_phrase_query_cloud.csv",
					BenchmarkReportData.queryPTQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC.get("99.9thQtime"),

					false, FileType.TEXT_PHRASE_QUERY_CLOUD);
		}

		if (BenchmarkReportData.querySTQMetricC != null) {

			BenchmarkAppConnector.writeToWebAppDataFile("text_sorting_query_cloud.csv",
					BenchmarkReportData.querySTQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.querySTQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC.get("99.9thQtime"),

					false, FileType.TEXT_PHRASE_QUERY_CLOUD);
		}
		
		if (BenchmarkReportData.queryHTQMetricC != null) {

			BenchmarkAppConnector.writeToWebAppDataFile("highlighting_query_cloud.csv",
					BenchmarkReportData.queryHTQMetricC.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("CommitID") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC.get("99.9thQtime"),

					false, FileType.HIGHLIGHTING_QUERY_CLOUD);
		}
		
		if (BenchmarkReportData.metricMapPartialUpdateStandalone != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_partial_update.csv",
					BenchmarkReportData.metricMapPartialUpdateStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapPartialUpdateStandalone.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapPartialUpdateStandalone.get("CommitID"),
					false, FileType.PARTIAL_UPDATE_HTTP_STANDALONE);
		}
		
		if (BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1 != null 
				&& BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2 != null
				&& BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("partial_update_throughput_data_standalone_concurrent.csv",
					BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3.get("IndexingThroughput"),
					false, FileType.PARTIAL_UPDATE_CONCURRENT_STANDALONE);
		}

		Util.getAndPublishCommitInformation();

		Util.postMessage("** Publishing data for webapp [COMPLETE] ..", MessageType.GREEN_TEXT, false);
	}
}