package org.apache.solr.tests.nightlybenchmarks;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.comparator.LastModifiedFileComparator;

public class BenchmarkAppConnector {

	public static String benchmarkAppDirectory;

	public BenchmarkAppConnector() {
		super();
	}

	public enum FileType {

		MEMORY_HEAP_USED, PROCESS_CPU_LOAD, TEST_ENV_FILE, STANDALONE_INDEXING_MAIN, STANDALONE_CREATE_COLLECTION_MAIN, STANDALONE_INDEXING_THROUGHPUT, CLOUD_CREATE_COLLECTION_MAIN, CLOUD_SERIAL_INDEXING_THROUGHPUT, CLOUD_CONCURRENT_INDEXING_THROUGHPUT, CLOUD_INDEXING_SERIAL, CLOUD_INDEXING_CONCURRENT, NUMERIC_QUERY_STANDALONE, NUMERIC_QUERY_CLOUD, LAST_RUN_COMMIT, IS_RUNNING_FILE, COMMIT_INFORMATION_FILE, IS_CLONING_FILE, COMMIT_QUEUE

	}

	public static String getLastRunCommitID() {
		File dataDir = new File(benchmarkAppDirectory + "data" + File.separator + "lastrun" + File.separator);
		return dataDir.listFiles()[0].getName().trim();
	}

	public static boolean isRunningFolderEmpty() {
		
		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);
		
		if (!dir.exists()) {
			dir.mkdir();
		}
		
		return dir.listFiles().length == 0 ? true : false;
	}

	public static boolean isCloningFolderEmpty() {
		
		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "cloning" + File.separator);
		
		if (!dir.exists()) {
			dir.mkdir();
		}
		
		return dir.listFiles().length == 0 ? true : false;
	}

	public static boolean isCommitQueueEmpty() {
		
		File dir = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);
		
		if (!dir.exists()) {
			dir.mkdir();
		}
		
		return dir.listFiles().length == 0 ? true : false;
	}

	public static boolean deleteCommitFromQueue(String commit) {
		
		File file = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator + commit);
		return file.delete();
	
	}
	
	public static String getOldestCommitFromQueue() {
		
		File directory = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);

		File[] files = directory.listFiles();
		
		if (files.length == 0) {
			return null;
		}
		
        Arrays.sort(files, LastModifiedFileComparator.LASTMODIFIED_COMPARATOR);
        return files[0].getName();
	}

	public static boolean isCommitInQueue(String commit) {
		
		File file = new File(benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator + commit);
		return file.exists();
	
	}
	
	public static void deleteFolder(FileType type) {

		if (type == FileType.LAST_RUN_COMMIT) {
			 File dir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + type + File.separator);
			 
			 if (!dir.exists()) {
				 dir.mkdir();
			 } else {
						 for (File file: dir.listFiles()) {
						        if (!file.isDirectory()) file.delete();
						    }
			 }
		} else if (type == FileType.IS_RUNNING_FILE) {
			 File dir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "running" + File.separator);
			 
			 if (!dir.exists()) {
				 dir.mkdir();
			 } else {
						 for (File file: dir.listFiles()) {
						        if (!file.isDirectory()) file.delete();
						    }
			 }
		} else if (type == FileType.IS_CLONING_FILE) {
			 File dir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "cloning" + File.separator);
			 
			 if (!dir.exists()) {
				 dir.mkdir();
			 } else {
						 for (File file: dir.listFiles()) {
						        if (!file.isDirectory()) file.delete();
						    }
			 }
		} else if (type == FileType.COMMIT_QUEUE) {
			 File dir = new File(BenchmarkAppConnector.benchmarkAppDirectory + "data" + File.separator + "commit_queue" + File.separator);
			 
			 if (!dir.exists()) {
				 dir.mkdir();
			 } else {
						 for (File file: dir.listFiles()) {
						        if (!file.isDirectory()) file.delete();
						    }
			 }
		}
	}

	public static void writeToWebAppDataFile(String fileName, String data, boolean createNewFile, FileType type) {
		
		File dataDir = null;
		File file = null;
		FileWriter fw = null;
		
		try
		{

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
				dataDir.mkdir();
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
				} else if (type == FileType.TEST_ENV_FILE || type == FileType.COMMIT_INFORMATION_FILE || type == FileType.COMMIT_QUEUE) {
					// Don't add any header
				} else if (type == FileType.STANDALONE_INDEXING_THROUGHPUT
						|| type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT) {
					fw.write("Date, Test_ID, Throughput (doc/sec), CommitID\n");
				} else if (type == FileType.CLOUD_INDEXING_CONCURRENT) {
					fw.write(
							"Date, Test_ID, CommitID, Seconds (2 Threads), Seconds (4 Threads), Seconds (6 Threads), Seconds (8 Threads), Seconds (10 Threads)\n");
				} else if (type == FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT) {
					fw.write(
							"Date, Test_ID, CommitID, Throughput (2 Threads), Throughput (4 Threads), Throughput (6 Threads), Throughput (8 Threads), Throughput (10 Threads) \n");
				} else if (type == FileType.NUMERIC_QUERY_CLOUD || type == FileType.NUMERIC_QUERY_STANDALONE) {
					fw.write(
							"Date, Test_ID, CommitID, QPS(Term), QTime-Min(Term), QTime-Max(Term), QPS(Range), QTime-Min(Range), QTime-Max(Range), QPS(Less Than), QTime-Min(Less Than), QTime-Max(Less Than), QPS(Greater Than), QTime-Min(Greater Than), QTime-Max(Greater Than)\n");
				}
			}
			
		    fw.write(data + "\n");
		    
		}
		catch(IOException ioe)
		{
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

		if (BenchmarkReportData.metricMapStandaloneConcurrent2 != null) {
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_standalone_concurrent.csv",
					BenchmarkReportData.metricMapStandaloneConcurrent2.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent2.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent2.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent4.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent6.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent8.get("IndexingTime") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent10.get("IndexingTime"),
					false, FileType.CLOUD_INDEXING_CONCURRENT);
			BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_concurrent.csv",
					BenchmarkReportData.metricMapStandaloneConcurrent2.get("TimeStamp") + ", " + Util.TEST_ID + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent2.get("CommitID") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent2.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent4.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent6.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent8.get("IndexingThroughput") + ", "
							+ BenchmarkReportData.metricMapStandaloneConcurrent10.get("IndexingThroughput"),
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

		Util.getAndPublishCommitInformation();

		Util.postMessage("** Publishing data for webapp [COMPLETE] ..", MessageType.WHITE_TEXT, false);
	}

}