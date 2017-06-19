package org.apache.solr.tests.nightlybenchmarks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class StandaloneConcurrentIndexClient implements Runnable {
	
	HttpSolrClient solrClient;
	String collectionName;
	Random r;
	
	static String zookeeperIp;
	static String zookeeperPort;
	static ConcurrentLinkedDeque<SolrInputDocument> documents = new ConcurrentLinkedDeque<SolrInputDocument>();
	static long startTime;
	static long endTime;
	static boolean running;
	static boolean starTimeFlag = false;
	static boolean endTimeFlag = false;
	static long numDocs;
	static double indexThroughput = 0;
	static double indexingTime = 0;
	
	public StandaloneConcurrentIndexClient(String collectionName, String baseSolrUrl) {

		this.collectionName = collectionName;
		
		solrClient = new HttpSolrClient.Builder().withBaseSolrUrl(baseSolrUrl).build();
		
		r = new Random();
		Util.postMessage(this.toString() + "** StandaloneConcurrentIndexClient CREATED ...", MessageType.GREEN_TEXT, false);
	}
	
	public static void prepare(int numDocuments) {
		
		Util.postMessage("** Preparing document queue ...", MessageType.CYAN_TEXT, false);

		numDocs = numDocuments;

		String line;
		String cvsSplitBy = ",";
		documents = new ConcurrentLinkedDeque<SolrInputDocument>();
		int numberOfDocuments = 0;
		
		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {
				
				SolrInputDocument document = new SolrInputDocument();				
				line.trim();
				
				String[] data = line.split(cvsSplitBy);

				document.addField("ID", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text1", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Int1", Integer.parseInt(data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Long1", Long.parseLong(data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Category1", data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Text2", data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				
				
				documents.add(document);
				numberOfDocuments++;
				if (numDocuments == numberOfDocuments) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Util.postMessage("** Queue preparation COMPLETE [READY NOW] ...", MessageType.CYAN_TEXT, false);
	}

	@Override
	public void run() {
		
		while(true) {
		
			if (running) {
				
				if (!starTimeFlag) {
					starTimeFlag = true;
					startTime = System.currentTimeMillis();
					Util.postMessage("Start Time: " + startTime, MessageType.PURPLE_TEXT, false);
				}
				
				if (!documents.isEmpty()) {
					try {	
					
							solrClient.add(this.collectionName, documents.remove());
						
					} catch (SolrServerException | IOException e) {
						e.printStackTrace();
					}
				} else {
					running = false;
				}
				
			} else {
				if (!endTimeFlag) {

					endTimeFlag = true;
					endTime = System.currentTimeMillis();
					indexingTime = endTime - startTime;
					indexThroughput = (numDocs - documents.size())/((endTime-startTime)/1000);
					Util.postMessage("Items remaining in queue:" + documents.size(), MessageType.PURPLE_TEXT, false);
					Util.postMessage("End Time: " + endTime, MessageType.PURPLE_TEXT, false);
					Util.postMessage("Throughput: " + indexThroughput, MessageType.PURPLE_TEXT, false);
					Util.postMessage(this.toString() + "** StandaloneConcurrentIndexClient Exiting critical section ...", MessageType.GREEN_TEXT, false);
					
				}
				break;
			}

		}
		
	}
	
	public static void setDocuments(Set<SolrInputDocument> documents) {
		StandaloneConcurrentIndexClient.documents.addAll(documents);
	}

	public void close() {
		try {
			solrClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void commit() {
		try {
			solrClient.commit(collectionName);
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void reset() {
		zookeeperIp = "";
		zookeeperPort = "";
		documents = new ConcurrentLinkedDeque<SolrInputDocument>();
		startTime = 0;
		endTime = 0;
		running = false;
		starTimeFlag = false;
		endTimeFlag = false;
		numDocs = 0;
		indexThroughput = 0;
		indexingTime = 0;
	}
}
