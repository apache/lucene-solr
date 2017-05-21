package org.apache.solr.tests.nightlybenchmarks;
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
		
		MEMORY_HEAP_USED, 
		PROCESS_CPU_LOAD, 
		TEST_ENV_FILE, 
		STANDALONE_INDEXING_MAIN, 
		STANDALONE_CREATE_COLLECTION_MAIN,
		STANDALONE_INDEXING_THROUGHPUT,
		CLOUD_CREATE_COLLECTION_MAIN,
		CLOUD_SERIAL_INDEXING_THROUGHPUT,
		CLOUD_CONCURRENT_INDEXING_THROUGHPUT
	
	}

	public static void writeToWebAppDataFile(String fileName, String data, boolean createNewFile, FileType type) {
		
	        
		BufferedWriter bw = null;
		FileWriter fw = null;
		File file = null;

		try {
			data += "\n";
			
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
										bw.write("Date, milliseconds, CommitID, Commit_Information\n");
				} else if (type == FileType.STANDALONE_INDEXING_MAIN) {
										bw.write("Date, seconds, CommitID, Commit_Information\n");
				} else if (type == FileType.TEST_ENV_FILE) {
							// Don't add any header
				} else if (type == FileType.STANDALONE_INDEXING_THROUGHPUT || type == FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT || type == FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT) {
										bw.write("Date, Throughput (doc/sec)\n");
				}
				
			}

			bw.write(data);

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