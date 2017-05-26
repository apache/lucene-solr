package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexingClient {
	
	public enum SolrClientType { HTTP_SOLR_CLIENT, CLOUD_SOLR_CLIENT, CONCURRENT_UPDATE_SOLR_CLIENT };
	
	@SuppressWarnings("unused")
	private String host;
	private String port;
	@SuppressWarnings("unused")
	private String collection; 	
	public static String solrCommitHistoryData;
	public static String amazonFoodDataLocation;
	private String commitId;
	Random r = new Random();
	
	
	public static List<Integer> intList = new LinkedList<Integer>();
	public static int documentCount;

	public SolrIndexingClient(String host, String port, String collection, String commitId) {
		super();
		this.host = host;
		this.port = port;
		this.collection = collection;
		this.commitId = commitId;
	}
	
	@SuppressWarnings("deprecation")
	public Map<String, String> indexAmazonFoodData(int numDocuments, String urlString) {
		
		documentCount = numDocuments;
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.ACTION, false);		
		
		HttpSolrClient solrClient = new HttpSolrClient.Builder(urlString).build();	
		
		long start = 0; 
		long end = 0;	
		int numberOfDocuments = 0;		
        String line = "";
        String cvsSplitBy = ",";
        int value;
        
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
        	    document.addField("RandomLongField", r.nextLong());
        	    
        	    value =  r.nextInt(Integer.MAX_VALUE);
        	    document.addField("RandomIntField", value);
        	    intList.add(value);
        	    
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
    		Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double)(end-start) + " nanosecond(s)" , MessageType.RESULT_ERRROR, false);

            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
			e.printStackTrace();
		}

        Date dNow = new Date( );
   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
   	    
   	    Map<String, String> returnMetricMap = new HashMap<String, String>();
   	    
   	    returnMetricMap.put("TimeStamp", ft.format(dNow));
   	    returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
   	    returnMetricMap.put("IndexingTime", "" + (double)((double)(end-start)/(double)1000000000));
   	    returnMetricMap.put("IndexingThroughput", "" + (double)numberOfDocuments/((double)((double)end-start)/(double)1000000000));
   	    returnMetricMap.put("ThroughputUnit", "doc/sec");
   	    returnMetricMap.put("CommitID", this.commitId);
		
		thread.stop();
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.ACTION, false);	
		return returnMetricMap;
	}
	
	
	@SuppressWarnings("deprecation")
	public Map<String, String> indexAmazonFoodData(int numDocuments, String urlString, String zookeeperIp, String zookeeperPort, String collectionName) {
		
		documentCount = numDocuments;
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.ACTION, false);		
		
		CloudSolrClient solrClient = new CloudSolrClient(zookeeperIp + ":" + zookeeperPort);
		solrClient.connect();
		solrClient.setDefaultCollection(collectionName);
		
		long start = 0; 
		long end = 0;	
		int numberOfDocuments = 0;		
        String line = "";
        String cvsSplitBy = ",";
        int value;
        
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
        	    document.addField("RandomLongField", r.nextLong());

        	    value =  r.nextInt(Integer.MAX_VALUE);
        	    document.addField("RandomIntField", value);
        	    intList.add(value);

        	    
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
    		Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double)(end-start) + " nanosecond(s)" , MessageType.RESULT_ERRROR, false);

            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
			e.printStackTrace();
		}

        Date dNow = new Date( );
   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");
		
   	    Map<String, String> returnMetricMap = new HashMap<String, String>();
   	    
   	    returnMetricMap.put("TimeStamp", ft.format(dNow));
   	    returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
   	    returnMetricMap.put("IndexingTime", "" + (double)((double)(end-start)/(double)1000000000));
   	    returnMetricMap.put("IndexingThroughput", "" + (double)numberOfDocuments/((double)((double)end-start)/(double)1000000000));
   	    returnMetricMap.put("ThroughputUnit", "doc/sec");
   	    returnMetricMap.put("CommitID", this.commitId);

   	    
		thread.stop();
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.ACTION, false);		
		return returnMetricMap;
	}


	@SuppressWarnings("deprecation")
	public Map<String, String> indexAmazonFoodData(int numDocuments, String urlString, String zookeeperIp, String zookeeperPort, String collectionName, int queueSize, int threadCount) {
		
		documentCount = numDocuments;

		Util.postMessage("** Indexing documents (Amazon Food Reviews) ...", MessageType.ACTION, false);		
		
		ConcurrentUpdateSolrClient solrClient = new ConcurrentUpdateSolrClient(urlString, queueSize, threadCount);
		solrClient.setConnectionTimeout(1000000);
		
		long start = 0; 
		long end = 0;	
		int numberOfDocuments = 0;		
        String line = "";
        String cvsSplitBy = ",";
        int value;
        
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
        	    document.addField("RandomLongField", r.nextLong());

        	    value =  r.nextInt(Integer.MAX_VALUE);
        	    document.addField("RandomIntField", value);
        	    intList.add(value);
        	    
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
    		Util.postMessage("** Time taken to index " + numberOfDocuments + " documents is: " + (double)(end-start) + " nanosecond(s)" , MessageType.RESULT_ERRROR, false);

            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SolrServerException e) {
			e.printStackTrace();
		}

        Date dNow = new Date( );
   	    SimpleDateFormat ft =  new SimpleDateFormat ("yyyy/MM/dd HH:mm:ss");

   	    Map<String, String> returnMetricMap = new HashMap<String, String>();
   	    
   	    returnMetricMap.put("TimeStamp", ft.format(dNow));
   	    returnMetricMap.put("TimeFormat", "yyyy/MM/dd HH:mm:ss");
   	    returnMetricMap.put("IndexingTime", "" + (double)((double)(end-start)/(double)1000000000));
   	    returnMetricMap.put("IndexingThroughput", "" + (double)numberOfDocuments/((double)((double)end-start)/(double)1000000000));
   	    returnMetricMap.put("ThroughputUnit", "doc/sec");
   	    returnMetricMap.put("CommitID", this.commitId);
		
		thread.stop();
		
		Util.postMessage("** Indexing documents (Amazon Food Reviews Data) COMPLETE ...", MessageType.ACTION, false);		
		return returnMetricMap;
	}

	
}
