package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.tests.nightlybenchmarks.BenchmarkAppConnector.FileType;

public class SolrClient {
	
	public enum SolrClientType { HTTP_SOLR_CLIENT, CLOUD_SOLR_CLIENT, CONCURRENT_UPDATE_SOLR_CLIENT };
	
	@SuppressWarnings("unused")
	private String host;
	private String port;
	@SuppressWarnings("unused")
	private String collection; 	
	public static String solrCommitHistoryData;
	public static String amazonFoodDataLocation;
	private String commitId;

	public SolrClient(String host, String port, String collection, String commitId) {
		super();
		this.host = host;
		this.port = port;
		this.collection = collection;
		this.commitId = commitId;
	}
	
	@SuppressWarnings("deprecation")
	public void indexAmazonFoodData(int numDocuments, String urlString) {
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.ACTION, false);		
		
		HttpSolrClient solrClient = new HttpSolrClient.Builder(urlString).build();	
		
		long start = 0; 
		long end = 0;	
		int numberOfDocuments = 0;		
        String line = "";
        String cvsSplitBy = ",";
        
        Thread thread =new Thread(new MetricEstimation(this.commitId, TestType.STANDALONE_INDEXING, this.port));  
        thread.start();  

        try (BufferedReader br = new BufferedReader(new FileReader(amazonFoodDataLocation))) {

        	start = System.nanoTime();
            while ((line = br.readLine()) != null) {
                
            	String[] foodData = line.split(cvsSplitBy);
        	    SolrInputDocument document = new SolrInputDocument();

        	    document.addField("Id", foodData[0]);
        	    document.addField("ProductId", foodData[1]);
        	    document.addField("UserId", foodData[2]);
        	    document.addField("ProfileName", foodData[3].replaceAll("'", "").replaceAll("\"", ""));
        	    document.addField("HelpfulnessNumerator", foodData[4]);
        	    document.addField("HelpfulnessDenominator", foodData[5]);
        	    document.addField("Score", foodData[6]);
        	    document.addField("Time", foodData[7]);
        	    document.addField("Summary", foodData[8].replaceAll("'", "").replaceAll("\"", ""));
        	    document.addField("Text", foodData[9].replaceAll("'", "").replaceAll("\"", ""));
        	    
				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
								break;
				}
				if(numberOfDocuments%5000 == 0) {
								Util.postMessageOnLine("|");
				}
				Util.postMessageOnLine("" + numberOfDocuments + " ");
            }
    		solrClient.commit();
    		end = System.nanoTime();
    		Util.postMessage("", MessageType.GENERAL, false);
    		Util.postMessage("** Committing the documents ...", MessageType.ACTION, false);

    		Util.postMessage("** Closing the Solr connection ...", MessageType.RESULT_SUCCESS, false);
    		solrClient.close();
    		Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (end-start) + " nanosecond(s)" , MessageType.RESULT_ERRROR, false);

            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
			e.printStackTrace();
		}

        Date dNow = new Date( );
   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_standalone_regular.csv", ft.format(dNow) + ", " + ((end-start)/1000000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.STANDALONE_INDEXING_MAIN);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_standalone_regular.csv", ft.format(dNow) + ", " + numberOfDocuments/((end-start)/1000000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.STANDALONE_INDEXING_THROUGHPUT);	
		
		thread.stop();
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.ACTION, false);		
	}
	
	
	@SuppressWarnings("deprecation")
	public void indexAmazonFoodData(int numDocuments, String urlString, String zookeeperIp, String zookeeperPort, String collectionName) {
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.ACTION, false);		
		
		CloudSolrClient solrClient = new CloudSolrClient(zookeeperIp + ":" + zookeeperPort);
		solrClient.connect();
		solrClient.setDefaultCollection(collectionName);
		
		long start = 0; 
		long end = 0;	
		int numberOfDocuments = 0;		
        String line = "";
        String cvsSplitBy = ",";
        
        Thread thread =new Thread(new MetricEstimation(this.commitId, TestType.CLOUD_INDEXING_REGULAR, this.port));  
        thread.start();  

        try (BufferedReader br = new BufferedReader(new FileReader(amazonFoodDataLocation))) {

        	start = System.nanoTime();
            while ((line = br.readLine()) != null) {
                
            	String[] foodData = line.split(cvsSplitBy);
        	    SolrInputDocument document = new SolrInputDocument();

        	    document.addField("Id", foodData[0]);
        	    document.addField("ProductId", foodData[1]);
        	    document.addField("UserId", foodData[2]);
        	    document.addField("ProfileName", foodData[3].replaceAll("'", "").replaceAll("\"", ""));
        	    document.addField("HelpfulnessNumerator", foodData[4]);
        	    document.addField("HelpfulnessDenominator", foodData[5]);
        	    document.addField("Score", foodData[6]);
        	    document.addField("Time", foodData[7]);
        	    document.addField("Summary", foodData[8].replaceAll("'", "").replaceAll("\"", ""));
        	    document.addField("Text", foodData[9].replaceAll("'", "").replaceAll("\"", ""));
        	    
				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
								break;
				}
				if(numberOfDocuments%5000 == 0) {
								Util.postMessageOnLine("|");
				}
				Util.postMessageOnLine("" + numberOfDocuments + " ");
            }
    		solrClient.commit();
    		end = System.nanoTime();
    		Util.postMessage("", MessageType.GENERAL, false);
    		Util.postMessage("** Committing the documents ...", MessageType.ACTION, false);

    		Util.postMessage("** Closing the Solr connection ...", MessageType.RESULT_SUCCESS, false);
    		solrClient.close();
    		Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (end-start) + " nanosecond(s)" , MessageType.RESULT_ERRROR, false);

            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
			e.printStackTrace();
		}

        Date dNow = new Date( );
   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_cloud_regular.csv", ft.format(dNow) + ", " + ((end-start)/1000000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.STANDALONE_INDEXING_MAIN);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_serial.csv", ft.format(dNow) + ", " + numberOfDocuments/((end-start)/1000000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.CLOUD_SERIAL_INDEXING_THROUGHPUT);	
		
		thread.stop();
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.ACTION, false);		
	}


	@SuppressWarnings("deprecation")
	public void indexAmazonFoodData(int numDocuments, String urlString, String zookeeperIp, String zookeeperPort, String collectionName, int queueSize, int threadCount) {
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.ACTION, false);		
		
		ConcurrentUpdateSolrClient solrClient = new ConcurrentUpdateSolrClient(urlString, queueSize, threadCount);
		solrClient.setConnectionTimeout(1000000);
		
		long start = 0; 
		long end = 0;	
		int numberOfDocuments = 0;		
        String line = "";
        String cvsSplitBy = ",";
        
        Thread thread =new Thread(new MetricEstimation(this.commitId, TestType.CLOUD_INDEXING_CONCURRENT, this.port));  
        thread.start();  

        try (BufferedReader br = new BufferedReader(new FileReader(amazonFoodDataLocation))) {

        	start = System.nanoTime();
            while ((line = br.readLine()) != null) {
                
            	String[] foodData = line.split(cvsSplitBy);
        	    SolrInputDocument document = new SolrInputDocument();

        	    document.addField("Id", foodData[0]);
        	    document.addField("ProductId", foodData[1]);
        	    document.addField("UserId", foodData[2]);
        	    document.addField("ProfileName", foodData[3].replaceAll("'", "").replaceAll("\"", ""));
        	    document.addField("HelpfulnessNumerator", foodData[4]);
        	    document.addField("HelpfulnessDenominator", foodData[5]);
        	    document.addField("Score", foodData[6]);
        	    document.addField("Time", foodData[7]);
        	    document.addField("Summary", foodData[8].replaceAll("'", "").replaceAll("\"", ""));
        	    document.addField("Text", foodData[9].replaceAll("'", "").replaceAll("\"", ""));
        	    
				solrClient.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
								break;
				}
				if(numberOfDocuments%5000 == 0) {
								Util.postMessageOnLine("|");
				}
				Util.postMessageOnLine("" + numberOfDocuments + " ");
            }
    		solrClient.commit();
    		end = System.nanoTime();
    		Util.postMessage("", MessageType.GENERAL, false);
    		Util.postMessage("** Committing the documents ...", MessageType.ACTION, false);

    		Util.postMessage("** Closing the Solr connection ...", MessageType.RESULT_SUCCESS, false);
    		solrClient.close();
    		Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (end-start) + " nanosecond(s)" , MessageType.RESULT_ERRROR, false);

            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
			e.printStackTrace();
		}

        Date dNow = new Date( );
   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_data_cloud_concurrent.csv", ft.format(dNow) + ", " + ((end-start)/1000000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.STANDALONE_INDEXING_MAIN);	
		BenchmarkAppConnector.writeToWebAppDataFile("indexing_throughput_data_cloud_concurrent.csv", ft.format(dNow) + ", " + numberOfDocuments/((end-start)/1000000000) + ", " + this.commitId + ", " + Util.getCommitInformation(), false, FileType.CLOUD_CONCURRENT_INDEXING_THROUGHPUT);	
		
		thread.stop();
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.ACTION, false);		
	}
	
}
