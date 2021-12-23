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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * This class provides implementation for connecting class for webapp.
 * @author Vivek Narang
 *
 */
public class BenchmarkAppConnector {

	public final static Logger logger = Logger.getLogger(BenchmarkAppConnector.class);
	public static String benchmarkAppDirectory;

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
		PARTIAL_UPDATE_CLOUD,
		CLASSIC_TERM_FACETING_STANDALONE,
		CLASSIC_RANGE_FACETING_STANDALONE,
		JSON_TERM_FACETING_STANDALONE,
		JSON_RANGE_FACETING_STANDALONE,
		CLASSIC_TERM_FACETING_CLOUD,
		CLASSIC_RANGE_FACETING_CLOUD,
		JSON_TERM_FACETING_CLOUD,
		JSON_RANGE_FACETING_CLOUD
	}

	/**
	 * Constructor.
	 */
	public BenchmarkAppConnector() {
		super();
	}

	/**
	 * A method used for getting the last registered commit.
	 * 
	 * @return String
	 */
	public static String getLastRunCommitID() {

		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);

		if (dir != null && !dir.exists()) {
			dir.mkdirs();
		}

		File dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
		if (dataDir != null && dataDir.listFiles().length != 0) {
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

		if (dir != null && !dir.exists()) {
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

		if (dir != null && !dir.exists()) {
			dir.mkdirs();
		}

		return dir.listFiles().length == 0 ? true : false;
	}

	/**
	 * A method used to delete a specific folder.
	 * 
	 * @param type
	 * @throws Exception 
	 */
	public static void deleteFolder(FileType type) throws Exception {
		
		if (type == null) {
			throw new Exception("File type is undefined!");
		}

		if (type == FileType.LAST_RUN_COMMIT) {
			File dir = new File(
					BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);

			if (dir != null && !dir.exists()) {
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

			if (dir != null && !dir.exists()) {
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

			if (dir != null && !dir.exists()) {
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

			if (dir != null && !dir.exists()) {
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
	 * @throws Exception 
	 */
	public static void writeToWebAppDataFile(String fileName, String data, boolean createNewFile, FileType type) throws Exception {
		
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
						|| type == FileType.HIGHLIGHTING_QUERY_STANDALONE || type == FileType.SORTING_TEXT_QUERY_CLOUD
						|| type == FileType.SORTING_TEXT_QUERY_STANDALONE || type == FileType.CLASSIC_TERM_FACETING_STANDALONE
						|| type == FileType.CLASSIC_RANGE_FACETING_STANDALONE || type == FileType.CLASSIC_TERM_FACETING_CLOUD
						|| type == FileType.CLASSIC_RANGE_FACETING_CLOUD || type == FileType.JSON_TERM_FACETING_STANDALONE
						|| type == FileType.JSON_RANGE_FACETING_STANDALONE || type == FileType.JSON_TERM_FACETING_CLOUD
						|| type == FileType.JSON_RANGE_FACETING_CLOUD ) {
					fw.write(
							"Date, Test_ID, CommitID, QPS(2 Threads), QTime-Min(2 Threads), QTime-Max(2 Threads), QTime-75th-Percentile(2 Threads), QTime-95th-Percentile(2 Threads), QTime-99th-Percentile(2 Threads), QTime-99.9th-Percentile(2 Threads), QPS(4 Threads), QTime-Min(4 Threads), QTime-Max(4 Threads), QTime-75th-Percentile(4 Threads), QTime-95th-Percentile(4 Threads), QTime-99th-Percentile(4 Threads), QTime-99.9th-Percentile(4 Threads), QPS(6 Threads), QTime-Min(6 Threads), QTime-Max(6 Threads), QTime-75th-Percentile(6 Threads), QTime-95th-Percentile(6 Threads), QTime-99th-Percentile(6 Threads), QTime-99.9th-Percentile(6 Threads), QPS(8 Threads), QTime-Min(8 Threads), QTime-Max(8 Threads), QTime-75th-Percentile(8 Threads), QTime-95th-Percentile(8 Threads), QTime-99th-Percentile(8 Threads), QTime-99.9th-Percentile(8 Threads)\n");
				}
			}

			fw.write(data + "\n");

		} catch (IOException ioe) {
			logger.error("BenchmarkAppConnector: " + ioe.getMessage());
			throw new IOException(ioe.getMessage());
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					logger.error("BenchmarkAppConnector: " + e.getMessage());
					throw new IOException(e.getMessage());
				}
			}
		}

	}

	/**
	 * A method which publishes data to the WebApp once the cycle completes.
	 * @throws Exception 
	 */
	public static void publishDataForWebApp() throws Exception {

		logger.info("Publishing data for webapp ..");

		if (BenchmarkReportData.metricMapIndexingStandalone != null) {
			
			if (BenchmarkReportData.metricMapIndexingStandalone.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapIndexingStandalone.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapIndexingStandalone.get("CommitID") == null) {
				logger.error(BenchmarkReportData.metricMapIndexingStandalone.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv",
					BenchmarkReportData.metricMapIndexingStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapIndexingStandalone.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapIndexingStandalone.get("CommitID"),
					false, FileType.STANDALONE_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapCloudSerial_2N1S2R != null) {

			if (BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("CommitID") == null) {
				logger.error(BenchmarkReportData.metricMapCloudSerial_2N1S2R.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_2n1s2r.csv",
					BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N1S2R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudSerial_2N2S1R != null) {
			
			if (BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("CommitID") == null) {
				logger.error(BenchmarkReportData.metricMapCloudSerial_2N2S1R.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_2n2s1r.csv",
					BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_2N2S1R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudSerial_3N1S3R != null) {

			if (BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("CommitID") == null) {
				logger.error(BenchmarkReportData.metricMapCloudSerial_3N1S3R.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_3n1s3r.csv",
					BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_3N1S3R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudSerial_4N2S2R != null) {
			
			if (BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("CommitID") == null) {
				logger.error(BenchmarkReportData.metricMapCloudSerial_4N2S2R.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial_4n2s2r.csv",
					BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudSerial_4N2S2R.get("CommitID"),
					false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R != null 
				&& BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R != null
				&& BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R != null) {

			if (BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("CommitID") == null
					|| BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R.get("IndexingThroughput") == null) {
				logger.error(BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R.toString());

				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_2n1s2r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N1S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_2N1S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_2N1S2R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R != null
				&& BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R != null
				&& BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R != null) {

			if (BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("CommitID") == null
					|| BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R.get("IndexingThroughput") == null) {
				logger.error(BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R.toString());

				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_2n2s1r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_2N2S1R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_2N2S1R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_2N2S1R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R != null
				&& BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R != null
				&& BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R != null) {

			if (BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("CommitID") == null
					|| BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R.get("IndexingThroughput") == null) {
				logger.error(BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R.toString());

				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_3n1s3r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_3N1S3R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_3N1S3R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_3N1S3R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}
		if (BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R != null
				&& BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R != null
				&& BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R != null) {

			if (BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("CommitID") == null
					|| BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R.get("IndexingThroughput") == null) {
				logger.error(BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R.toString());
				logger.error(BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R.toString());

				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent_4n2s2r.csv",
					BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent1_4N2S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent2_4N2S2R.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapCloudConcurrent3_4N2S2R.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.metricMapStandaloneIndexingConcurrent1 != null
				&& BenchmarkReportData.metricMapStandaloneIndexingConcurrent2 != null
				&& BenchmarkReportData.metricMapStandaloneIndexingConcurrent3 != null) {

			if (BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("TimeStamp") == null 
					|| BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("CommitID") == null
					|| BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapStandaloneIndexingConcurrent2.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapStandaloneIndexingConcurrent3.get("IndexingThroughput") == null) {
				logger.error(BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.toString());
				logger.error(BenchmarkReportData.metricMapStandaloneIndexingConcurrent2.toString());
				logger.error(BenchmarkReportData.metricMapStandaloneIndexingConcurrent3.toString());

				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_concurrent.csv",
					BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent1.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent2.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneIndexingConcurrent3.get("IndexingThroughput"),
					false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);
		}

		if (BenchmarkReportData.returnStandaloneCreateCollectionMap != null) {
	
			if (BenchmarkReportData.returnStandaloneCreateCollectionMap.get("TimeStamp") == null 
					|| BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CommitID") == null
					|| BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CreateCollectionTime") == null) {
				logger.error(BenchmarkReportData.returnStandaloneCreateCollectionMap.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_standalone_regular.csv",
					BenchmarkReportData.returnStandaloneCreateCollectionMap.get("TimeStamp") + ", " + Util.TEST_ID
							+ ", " + BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CreateCollectionTime")
							+ ", " + BenchmarkReportData.returnStandaloneCreateCollectionMap.get("CommitID"),
					false, FileType.STANDALONE_CREATE_COLLECTION_MAIN);
		}

		if (BenchmarkReportData.returnCloudCreateCollectionMap != null) {
			
			if (BenchmarkReportData.returnCloudCreateCollectionMap.get("TimeStamp") == null 
					|| BenchmarkReportData.returnCloudCreateCollectionMap.get("CommitID") == null
					|| BenchmarkReportData.returnCloudCreateCollectionMap.get("CreateCollectionTime") == null) {
				logger.error(BenchmarkReportData.returnCloudCreateCollectionMap.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("create_collection_data_cloud_regular.csv",
					BenchmarkReportData.returnCloudCreateCollectionMap.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.returnCloudCreateCollectionMap.get("CreateCollectionTime") + ", "
							+ BenchmarkReportData.returnCloudCreateCollectionMap.get("CommitID"),
					false, FileType.CLOUD_CREATE_COLLECTION_MAIN);
		}


		if (BenchmarkReportData.queryTNQMetricS_T1 != null && BenchmarkReportData.queryTNQMetricS_T2 != null
				&& BenchmarkReportData.queryTNQMetricS_T3 != null && BenchmarkReportData.queryTNQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryTNQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTNQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTNQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryTNQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryTNQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryTNQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryTNQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

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

		if (BenchmarkReportData.queryRNQMetricS_T1 != null && BenchmarkReportData.queryRNQMetricS_T2 != null
				&& BenchmarkReportData.queryRNQMetricS_T3 != null && BenchmarkReportData.queryRNQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryRNQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryRNQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryRNQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryRNQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryRNQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryRNQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryRNQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
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

		if (BenchmarkReportData.queryLNQMetricS_T1 != null && BenchmarkReportData.queryLNQMetricS_T2 != null
				&& BenchmarkReportData.queryLNQMetricS_T3 != null && BenchmarkReportData.queryLNQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryLNQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryLNQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryLNQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryLNQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryLNQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryLNQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryLNQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
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

		if (BenchmarkReportData.queryGNQMetricS_T1 != null && BenchmarkReportData.queryGNQMetricS_T2 != null
				&& BenchmarkReportData.queryGNQMetricS_T3 != null && BenchmarkReportData.queryGNQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryGNQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryGNQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryGNQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryGNQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryGNQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryGNQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryGNQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
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

		if (BenchmarkReportData.queryANQMetricS_T1 != null && BenchmarkReportData.queryANQMetricS_T2 != null
				&& BenchmarkReportData.queryANQMetricS_T3 != null && BenchmarkReportData.queryANQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryANQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryANQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryANQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryANQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryANQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryANQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryANQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

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

		if (BenchmarkReportData.queryONQMetricS_T1 != null && BenchmarkReportData.queryONQMetricS_T2 != null
				&& BenchmarkReportData.queryONQMetricS_T3 != null && BenchmarkReportData.queryONQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryONQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryONQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryONQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryONQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryONQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryONQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryONQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
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

		if (BenchmarkReportData.querySNQMetricS_T1 != null && BenchmarkReportData.querySNQMetricS_T2 != null
				&& BenchmarkReportData.querySNQMetricS_T3 != null && BenchmarkReportData.querySNQMetricS_T4 != null) {
			
			if (BenchmarkReportData.querySNQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySNQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySNQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.querySNQMetricS_T1.toString());
				logger.error(BenchmarkReportData.querySNQMetricS_T2.toString());
				logger.error(BenchmarkReportData.querySNQMetricS_T3.toString());
				logger.error(BenchmarkReportData.querySNQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

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

		if (BenchmarkReportData.queryTTQMetricS_T1 != null && BenchmarkReportData.queryTTQMetricS_T2 != null
				&& BenchmarkReportData.queryTTQMetricS_T3 != null && BenchmarkReportData.queryTTQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryTTQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTTQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTTQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryTTQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryTTQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryTTQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryTTQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

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

		if (BenchmarkReportData.queryPTQMetricS_T1 != null && BenchmarkReportData.queryPTQMetricS_T2 != null
				&& BenchmarkReportData.queryPTQMetricS_T3 != null && BenchmarkReportData.queryPTQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryPTQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryPTQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryPTQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryPTQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryPTQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryPTQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryPTQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

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

		if (BenchmarkReportData.querySTQMetricS_T1 != null && BenchmarkReportData.querySTQMetricS_T2 != null
				&& BenchmarkReportData.querySTQMetricS_T3 != null && BenchmarkReportData.querySTQMetricS_T4 != null) {
			
			if (BenchmarkReportData.querySTQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySTQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySTQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.querySTQMetricS_T1.toString());
				logger.error(BenchmarkReportData.querySTQMetricS_T2.toString());
				logger.error(BenchmarkReportData.querySTQMetricS_T3.toString());
				logger.error(BenchmarkReportData.querySTQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
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

		if (BenchmarkReportData.queryHTQMetricS_T1 != null && BenchmarkReportData.queryHTQMetricS_T2 != null
				&& BenchmarkReportData.queryHTQMetricS_T3 != null && BenchmarkReportData.queryHTQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryHTQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryHTQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryHTQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryHTQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryHTQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryHTQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryHTQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

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


		if (BenchmarkReportData.queryTNQMetricC_T1 != null && BenchmarkReportData.queryTNQMetricC_T2 != null
				&& BenchmarkReportData.queryTNQMetricC_T3 != null && BenchmarkReportData.queryTNQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryTNQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTNQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTNQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTNQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryTNQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryTNQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryTNQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryTNQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryTNQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryTNQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("term_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryTNQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTNQMetricC_T4.get("99.9thQtime"),
					false, FileType.TERM_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryRNQMetricC_T1 != null && BenchmarkReportData.queryRNQMetricC_T2 != null
				&& BenchmarkReportData.queryRNQMetricC_T3 != null && BenchmarkReportData.queryRNQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryRNQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryRNQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryRNQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryRNQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryRNQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryRNQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryRNQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryRNQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryRNQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryRNQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("range_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryRNQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryRNQMetricC_T4.get("99.9thQtime"),
					false, FileType.RANGE_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryLNQMetricC_T1 != null && BenchmarkReportData.queryLNQMetricC_T2 != null
				&& BenchmarkReportData.queryLNQMetricC_T3 != null && BenchmarkReportData.queryLNQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryLNQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryLNQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryLNQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryLNQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryLNQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryLNQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryLNQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryLNQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryLNQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryLNQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("lt_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryLNQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryLNQMetricC_T4.get("99.9thQtime"),
					false, FileType.LT_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryGNQMetricC_T1 != null && BenchmarkReportData.queryGNQMetricC_T2 != null
				&& BenchmarkReportData.queryGNQMetricC_T3 != null && BenchmarkReportData.queryGNQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryGNQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryGNQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryGNQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryGNQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryGNQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryGNQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryGNQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryGNQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryGNQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryGNQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("gt_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryGNQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryGNQMetricC_T4.get("99.9thQtime"),
					false, FileType.GT_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryANQMetricC_T1 != null && BenchmarkReportData.queryANQMetricC_T2 != null
				&& BenchmarkReportData.queryANQMetricC_T3 != null && BenchmarkReportData.queryANQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryANQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryANQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryANQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryANQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryANQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryANQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryANQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryANQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryANQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryANQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("and_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryANQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryANQMetricC_T4.get("99.9thQtime"),
					false, FileType.AND_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryONQMetricC_T1 != null && BenchmarkReportData.queryONQMetricC_T2 != null
				&& BenchmarkReportData.queryONQMetricC_T3 != null && BenchmarkReportData.queryONQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryONQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryONQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryONQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryONQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryONQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryONQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryONQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryONQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryONQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryONQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("or_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.queryONQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryONQMetricC_T4.get("99.9thQtime"),
					false, FileType.OR_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.querySNQMetricC_T1 != null && BenchmarkReportData.querySNQMetricC_T2 != null
				&& BenchmarkReportData.querySNQMetricC_T3 != null && BenchmarkReportData.querySNQMetricC_T4 != null) {
			
			if (BenchmarkReportData.querySNQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySNQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySNQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySNQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.querySNQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.querySNQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.querySNQMetricC_T1.toString());
				logger.error(BenchmarkReportData.querySNQMetricC_T2.toString());
				logger.error(BenchmarkReportData.querySNQMetricC_T3.toString());
				logger.error(BenchmarkReportData.querySNQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("sorting_numeric_query_benchmark_cloud.csv",
					BenchmarkReportData.querySNQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.querySNQMetricC_T4.get("99.9thQtime"),
					false, FileType.SORTING_NUMERIC_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryTTQMetricC_T1 != null && BenchmarkReportData.queryTTQMetricC_T2 != null
				&& BenchmarkReportData.queryTTQMetricC_T3 != null && BenchmarkReportData.queryTTQMetricC_T4 != null) {
			
			
			if (BenchmarkReportData.queryTTQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTTQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryTTQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryTTQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryTTQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryTTQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryTTQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryTTQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryTTQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryTTQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("term_text_query_benchmark_cloud.csv",
					BenchmarkReportData.queryTTQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryTTQMetricC_T4.get("99.9thQtime"),
					false, FileType.TEXT_TERM_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryPTQMetricC_T1 != null && BenchmarkReportData.queryPTQMetricC_T2 != null
				&& BenchmarkReportData.queryPTQMetricC_T3 != null && BenchmarkReportData.queryPTQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryPTQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryPTQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryPTQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryPTQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryPTQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryPTQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryPTQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryPTQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryPTQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryPTQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("phrase_text_query_benchmark_cloud.csv",
					BenchmarkReportData.queryPTQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryPTQMetricC_T4.get("99.9thQtime"),
					false, FileType.TEXT_PHRASE_QUERY_CLOUD);
		}

		if (BenchmarkReportData.querySTQMetricC_T1 != null && BenchmarkReportData.querySTQMetricC_T2 != null
				&& BenchmarkReportData.querySTQMetricC_T3 != null && BenchmarkReportData.querySTQMetricC_T4 != null) {
			
			if (BenchmarkReportData.querySTQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySTQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.querySTQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.querySTQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.querySTQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.querySTQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.querySTQMetricC_T1.toString());
				logger.error(BenchmarkReportData.querySTQMetricC_T2.toString());
				logger.error(BenchmarkReportData.querySTQMetricC_T3.toString());
				logger.error(BenchmarkReportData.querySTQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("sorting_text_query_benchmark_cloud.csv",
					BenchmarkReportData.querySTQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.querySTQMetricC_T4.get("99.9thQtime"),
					false, FileType.SORTING_TEXT_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryHTQMetricC_T1 != null && BenchmarkReportData.queryHTQMetricC_T2 != null
				&& BenchmarkReportData.queryHTQMetricC_T3 != null && BenchmarkReportData.queryHTQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryHTQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryHTQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryHTQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryHTQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryHTQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryHTQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryHTQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryHTQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryHTQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryHTQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("highlighting_text_query_benchmark_cloud.csv",
					BenchmarkReportData.queryHTQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryHTQMetricC_T4.get("99.9thQtime"),
					false, FileType.HIGHLIGHTING_QUERY_CLOUD);
		}

		if (BenchmarkReportData.queryCTFQMetricS_T1 != null && BenchmarkReportData.queryCTFQMetricS_T2 != null
				&& BenchmarkReportData.queryCTFQMetricS_T3 != null && BenchmarkReportData.queryCTFQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryCTFQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryCTFQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryCTFQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryCTFQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryCTFQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("classic_term_faceting_query_benchmark_standalone.csv",
					BenchmarkReportData.queryCTFQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricS_T4.get("99.9thQtime"),
					false, FileType.CLASSIC_TERM_FACETING_STANDALONE);
		}

		if (BenchmarkReportData.queryCRFQMetricS_T1 != null && BenchmarkReportData.queryCRFQMetricS_T2 != null
				&& BenchmarkReportData.queryCRFQMetricS_T3 != null && BenchmarkReportData.queryCRFQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryCRFQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryCRFQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryCRFQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryCRFQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryCRFQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("classic_range_faceting_query_benchmark_standalone.csv",
					BenchmarkReportData.queryCRFQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricS_T4.get("99.9thQtime"),
					false, FileType.CLASSIC_RANGE_FACETING_STANDALONE);
		}

		if (BenchmarkReportData.queryJTFQMetricS_T1 != null && BenchmarkReportData.queryJTFQMetricS_T2 != null
				&& BenchmarkReportData.queryJTFQMetricS_T3 != null && BenchmarkReportData.queryJTFQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryJTFQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryJTFQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryJTFQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryJTFQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryJTFQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("json_term_faceting_query_benchmark_standalone.csv",
					BenchmarkReportData.queryJTFQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricS_T4.get("99.9thQtime"),
					false, FileType.JSON_TERM_FACETING_STANDALONE);
		}

		if (BenchmarkReportData.queryJRFQMetricS_T1 != null && BenchmarkReportData.queryJRFQMetricS_T2 != null
				&& BenchmarkReportData.queryJRFQMetricS_T3 != null && BenchmarkReportData.queryJRFQMetricS_T4 != null) {
			
			if (BenchmarkReportData.queryJRFQMetricS_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("CommitID") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricS_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryJRFQMetricS_T1.toString());
				logger.error(BenchmarkReportData.queryJRFQMetricS_T2.toString());
				logger.error(BenchmarkReportData.queryJRFQMetricS_T3.toString());
				logger.error(BenchmarkReportData.queryJRFQMetricS_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("json_range_faceting_query_benchmark_standalone.csv",
					BenchmarkReportData.queryJRFQMetricS_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricS_T4.get("99.9thQtime"),
					false, FileType.JSON_TERM_FACETING_STANDALONE);
		}
		
		
		if (BenchmarkReportData.queryCTFQMetricC_T1 != null && BenchmarkReportData.queryCTFQMetricC_T2 != null
				&& BenchmarkReportData.queryCTFQMetricC_T3 != null && BenchmarkReportData.queryCTFQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryCTFQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryCTFQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryCTFQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryCTFQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryCTFQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryCTFQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("classic_term_faceting_query_benchmark_cloud.csv",
					BenchmarkReportData.queryCTFQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCTFQMetricC_T4.get("99.9thQtime"),
					false, FileType.CLASSIC_TERM_FACETING_CLOUD);
		}

		if (BenchmarkReportData.queryCRFQMetricC_T1 != null && BenchmarkReportData.queryCRFQMetricC_T2 != null
				&& BenchmarkReportData.queryCRFQMetricC_T3 != null && BenchmarkReportData.queryCRFQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryCRFQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryCRFQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryCRFQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryCRFQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryCRFQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryCRFQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("classic_range_faceting_query_benchmark_cloud.csv",
					BenchmarkReportData.queryCRFQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryCRFQMetricC_T4.get("99.9thQtime"),
					false, FileType.CLASSIC_RANGE_FACETING_CLOUD);
		}

		if (BenchmarkReportData.queryJTFQMetricC_T1 != null && BenchmarkReportData.queryJTFQMetricC_T2 != null
				&& BenchmarkReportData.queryJTFQMetricC_T3 != null && BenchmarkReportData.queryJTFQMetricC_T4 != null) {
			
			if (BenchmarkReportData.queryJTFQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryJTFQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryJTFQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryJTFQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryJTFQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryJTFQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("json_term_faceting_query_benchmark_cloud.csv",
					BenchmarkReportData.queryJTFQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJTFQMetricC_T4.get("99.9thQtime"),
					false, FileType.JSON_TERM_FACETING_CLOUD);
		}

		if (BenchmarkReportData.queryJRFQMetricC_T1 != null && BenchmarkReportData.queryJRFQMetricC_T2 != null
				&& BenchmarkReportData.queryJRFQMetricC_T3 != null && BenchmarkReportData.queryJRFQMetricC_T4 != null) {
			
			
			if (BenchmarkReportData.queryJRFQMetricC_T1.get("TimeStamp") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("CommitID") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T1.get("99.9thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T2.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T3.get("99.9thQtime") == null					
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("QueriesPerSecond") == null
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("MinQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("MaxQTime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("75thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("95thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("99thQtime") == null
					|| BenchmarkReportData.queryJRFQMetricC_T4.get("99.9thQtime") == null) {
				
				logger.error(BenchmarkReportData.queryJRFQMetricC_T1.toString());
				logger.error(BenchmarkReportData.queryJRFQMetricC_T2.toString());
				logger.error(BenchmarkReportData.queryJRFQMetricC_T3.toString());
				logger.error(BenchmarkReportData.queryJRFQMetricC_T4.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}

			BenchmarkAppConnector.writeToWebAppDataFile("json_range_faceting_query_benchmark_cloud.csv",
					BenchmarkReportData.queryJRFQMetricC_T1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("CommitID") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T1.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T2.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T3.get("99.9thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("QueriesPerSecond") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("MinQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("MaxQTime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("75thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("95thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("99thQtime") + ", "
							+ BenchmarkReportData.queryJRFQMetricC_T4.get("99.9thQtime"),
					false, FileType.JSON_TERM_FACETING_CLOUD);
		}
		
		
		if (BenchmarkReportData.metricMapPartialUpdateStandalone != null) {
	
			if (BenchmarkReportData.metricMapPartialUpdateStandalone.get("TimeStamp") == null
					|| BenchmarkReportData.metricMapPartialUpdateStandalone.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapPartialUpdateStandalone.get("CommitID") == null) {
				
				logger.error(BenchmarkReportData.metricMapPartialUpdateStandalone.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_partial_update.csv",
					BenchmarkReportData.metricMapPartialUpdateStandalone.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapPartialUpdateStandalone.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapPartialUpdateStandalone.get("CommitID"),
					false, FileType.PARTIAL_UPDATE_HTTP_STANDALONE);
		}
		
		if (BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1 != null 
				&& BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2 != null
				&& BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3 != null) {
			
			if (BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("TimeStamp") == null
					|| BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("CommitID") == null
					|| BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2.get("IndexingThroughput") == null
					|| BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3.get("IndexingThroughput") == null) {
				
				logger.error(BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.toString());
				logger.error(BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2.toString());
				logger.error(BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3.toString());
				throw new Exception("PublishDataForWebApp: Null Values Observed.");
			}
			
			BenchmarkAppConnector.writeToWebAppDataFile("partial_update_throughput_data_standalone_concurrent.csv",
					BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent1.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent2.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandalonePartialUpdateConcurrent3.get("IndexingThroughput"),
					false, FileType.PARTIAL_UPDATE_CONCURRENT_STANDALONE);
		}

		Util.getAndPublishCommitInformation();

		logger.info("Publishing data for webapp [COMPLETE] ..");
	}
}