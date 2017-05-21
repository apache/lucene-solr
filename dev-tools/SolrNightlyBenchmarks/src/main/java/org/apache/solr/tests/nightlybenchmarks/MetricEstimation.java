package org.apache.solr.tests.nightlybenchmarks;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.MediaType;

import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class MetricEstimation extends Thread {

	public static String metricsURL;
	
	public MetricEstimation(String commitID, TestType testType, String port) {
		this.testType = testType;
		this.commitID = commitID;
		this.port = port;
	}
	
	public enum MetricType { MEM_ESTIMATION, CPU_ESTIMATION }
	public enum MetricSubType { MEMORY_HEAP_USED, PROCESS_CPU_LOAD }
	
	public TestType testType;
	public String commitID;
	public String port;
	
	public void run(){  

		if(testType == TestType.STANDALONE_INDEXING) {	
		
		
			while(true) { 
					try {									
				
						String response = Util.getResponse("http://localhost:" + this.port + "/solr/admin/metrics?wt=json&group=jvm", MediaType.APPLICATION_JSON);
						JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

						Date dNow = new Date();
				   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.MEM_ESTIMATION + "_" + testType  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.used").toString()) / (1024 * 1024)), false, FileType.MEMORY_HEAP_USED);
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.CPU_ESTIMATION + "_" + testType  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.processCpuLoad").toString()) * 100), false, FileType.PROCESS_CPU_LOAD);

						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		
		}

		if(testType == TestType.STANDALONE_CREATE_COLLECTION) {	
			
			
			while(true) { 
					try {									
				
						String response = Util.getResponse("http://localhost:" + this.port + "/solr/admin/metrics?wt=json&group=jvm", MediaType.APPLICATION_JSON);
						JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

						Date dNow = new Date();
				   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.MEM_ESTIMATION + "_" + TestType.STANDALONE_CREATE_COLLECTION  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.used").toString()) / (1024 * 1024)), false, FileType.MEMORY_HEAP_USED);
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.CPU_ESTIMATION + "_" + TestType.STANDALONE_CREATE_COLLECTION  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.processCpuLoad").toString()) * 100), false, FileType.PROCESS_CPU_LOAD);

						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		
		}
		
		if(testType == TestType.CLOUD_CREATE_COLLECTION) {	
			
			
			while(true) { 
					try {									
				
						String response = Util.getResponse("http://localhost:" + this.port + "/solr/admin/metrics?wt=json&group=jvm", MediaType.APPLICATION_JSON);
						JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

						Date dNow = new Date();
				   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.MEM_ESTIMATION + "_" + TestType.CLOUD_CREATE_COLLECTION  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.used").toString()) / (1024 * 1024)), false, FileType.MEMORY_HEAP_USED);
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.CPU_ESTIMATION + "_" + TestType.CLOUD_CREATE_COLLECTION  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.processCpuLoad").toString()) * 100), false, FileType.PROCESS_CPU_LOAD);

						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		
		}

		if(testType == TestType.CLOUD_INDEXING_REGULAR) {	
			
			
			while(true) { 
					try {									
				
						String response = Util.getResponse("http://localhost:" + this.port + "/solr/admin/metrics?wt=json&group=jvm", MediaType.APPLICATION_JSON);
						JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

						Date dNow = new Date();
				   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.MEM_ESTIMATION + "_" + TestType.CLOUD_INDEXING_REGULAR  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.used").toString()) / (1024 * 1024)), false, FileType.MEMORY_HEAP_USED);
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.CPU_ESTIMATION + "_" + TestType.CLOUD_INDEXING_REGULAR  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.processCpuLoad").toString()) * 100), false, FileType.PROCESS_CPU_LOAD);

						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		
		}

		if(testType == TestType.CLOUD_INDEXING_CONCURRENT) {	
			
			
			while(true) { 
					try {									
				
						String response = Util.getResponse("http://localhost:" + this.port + "/solr/admin/metrics?wt=json&group=jvm", MediaType.APPLICATION_JSON);
						JSONObject jsonObject = (JSONObject) JSONValue.parse(response);

						Date dNow = new Date();
				   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.MEM_ESTIMATION + "_" + TestType.CLOUD_INDEXING_CONCURRENT  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("memory.heap.used").toString()) / (1024 * 1024)), false, FileType.MEMORY_HEAP_USED);
						BenchmarkAppConnector.writeToWebAppDataFile(this.commitID + "_" + MetricType.CPU_ESTIMATION + "_" + TestType.CLOUD_INDEXING_CONCURRENT  +  "_dump.csv", ft.format(dNow) + ", " + (Double.parseDouble(((JSONObject)((JSONObject)jsonObject.get("metrics")).get("solr.jvm")).get("os.processCpuLoad").toString()) * 100), false, FileType.PROCESS_CPU_LOAD);

						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		
		}
		
	}  
}
