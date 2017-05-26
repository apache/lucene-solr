package org.apache.solr.tests.nightlybenchmarks;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class BenchmarkAppConnector {

	public static String benchmarkAppDirectory;	
	
	public BenchmarkAppConnector() {
		super();
	}
	
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
		NUMERIC_QUERY_STANDALONE,
		NUMERIC_QUERY_CLOUD
	
	}
	
	public static void writeFileHeader(String fileName, boolean createNewFile, FileType type) {

		BufferedWriter bw = null;
		FileWriter fw = null;
		File file = null;

		try {
			
			File dataDir = new File(benchmarkAppDirectory + File.separator + "data" + File.separator);
			if(!dataDir.exists()) {
				dataDir.mkdir();
			}
			
			file = new File(benchmarkAppDirectory + File.separator + "data" + File.separator + fileName);
			
			if (file.exists() && createNewFile) {
				file.delete();
				file.createNewFile();
			}

			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);

			if (!file.exists()) {
				file.createNewFile();
			}		

			if (file.length() == 0) {
				
				file.setReadable(true);
				file.setWritable(true);
				if (type == FileType.MEMORY_HEAP_USED) {
										bw.write("Date, Heap Space Used (MB)\n");
				} else if (type == FileType.PROCESS_CPU_LOAD) {
										bw.write("Date, Process CPU Load (%)\n");
				}else if (type == FileType.STANDALONE_CREATE_COLLECTION_MAIN || type == FileType.CLOUD_CREATE_COLLECTION_MAIN) {
										bw.write("Date, Milliseconds, CommitID, Average-Milliseconds\n");
				} else if (type == FileType.STANDALONE_INDEXING_MAIN || type == FileType.CLOUD_INDEXING_SERIAL) {
										bw.write("Date, Seconds, CommitID, Average-Seconds\n");
				} else if (type == FileType.TEST_ENV_FILE) {
										// Don't add any header
				} else if (type == FileType.STANDALONE_INDEXING_THROUGHPUT || type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT) {
										bw.write("Date, Throughput (doc/sec), CommitID, Average-Throughput\n");
				}  else if (type == FileType.CLOUD_INDEXING_CONCURRENT) {
					bw.write("Date, CommitID, Seconds (2 Threads), Seconds (4 Threads), Seconds (6 Threads), Seconds (8 Threads), Seconds (10 Threads)\n");
				}  else if (type == FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT) {
					bw.write("Date, CommitID, Throughput (2 Threads), Throughput (4 Threads), Throughput (6 Threads), Throughput (8 Threads), Throughput (10 Threads) \n");
				}  else if (type == FileType.NUMERIC_QUERY_CLOUD || type == FileType.NUMERIC_QUERY_STANDALONE) {
					bw.write("Date, CommitID, QPS(Term), QTime-Min(Term), QTime-Max(Term), QPS(Range), QTime-Min(Range), QTime-Max(Range), QPS(Less Than), QTime-Min(Less Than), QTime-Max(Less Than), QPS(Greater Than), QTime-Min(Greater Than), QTime-Max(Greater Than)\n");
				}	
				
				
			}

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

	public static void writeToWebAppDataFile(String fileName, String data, boolean createNewFile, FileType type) {

		writeFileHeader(fileName, createNewFile, type);
	        
		BufferedWriter bw = null;
		FileWriter fw = null;
		File file = null;

		try {
			
			file = new File(benchmarkAppDirectory + File.separator + "data" + File.separator + fileName);

			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			
			double last_param = 0.0;
			
			if (type == FileType.STANDALONE_CREATE_COLLECTION_MAIN || type == FileType.CLOUD_CREATE_COLLECTION_MAIN || type == FileType.STANDALONE_INDEXING_MAIN || type == FileType.STANDALONE_INDEXING_THROUGHPUT || type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT || type == FileType.CLOUD_INDEXING_SERIAL) {
			
				BufferedReader input = new BufferedReader(new FileReader(benchmarkAppDirectory + File.separator + "data" + File.separator + fileName));
			    String last = "", line = "";
	
			    while ((line = input.readLine()) != null) {
			        last = line;
			    }
			    
			    String[] lastLine = last.trim().split(",");
			    
			    try {
			    	last_param = Double.parseDouble(lastLine[3]);
			    } catch (Exception e) {
			    	last_param = -1;
			    }
			    
			    input.close();
			}
			
			if (type == FileType.STANDALONE_CREATE_COLLECTION_MAIN || type == FileType.CLOUD_CREATE_COLLECTION_MAIN || type == FileType.STANDALONE_INDEXING_MAIN || type == FileType.STANDALONE_INDEXING_THROUGHPUT || type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT || type == FileType.CLOUD_INDEXING_SERIAL) {
						
						String[] arrayString = data.trim().split(",");
						
						double param1 = Double.parseDouble(arrayString[1]);
						double d_average = 0.0;
						
						if (last_param == -1) {
							d_average = param1;
						} else {
							d_average = (param1 + last_param)/2;
						}
				
						bw.write(data + ", " + d_average + "\n");
				} else {
						bw.write(data + "\n");
				}

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
	
}