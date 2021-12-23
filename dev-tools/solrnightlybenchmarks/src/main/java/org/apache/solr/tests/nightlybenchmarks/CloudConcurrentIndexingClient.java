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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;

/**
 * This class provides implementation for indexing client for Solr Cloud
 * @author Vivek Narang
 *
 */
public class CloudConcurrentIndexingClient implements Runnable {

	public final static Logger logger = Logger.getLogger(CloudConcurrentIndexingClient.class);

	public static boolean running;
	public static boolean endLock = false;
	public static long documentCount = 0;
	public static long documentCountLimit = 0;
	public static ConcurrentLinkedQueue<SolrInputDocument> documents = new ConcurrentLinkedQueue<SolrInputDocument>();
	public static long totalTime = 0;

    CountDownLatch latch;
	String urlString;
	String collectionName;
	SolrParams params;
	CloudSolrClient solrClient;
	boolean setThreadReadyFlag = false;
	long numberOfThreads = 0;
	long startTime = 0;
	long endTime = 0;
	long delayEstimationBySeconds = 0;
	Random random = new Random();

	/**
	 * Constructor.
	 * 
	 * @param zookeeperURL
	 * @param collectionName
	 */
	public CloudConcurrentIndexingClient(String zookeeperURL, String collectionName, CountDownLatch latch) {
		super();
		this.collectionName = collectionName;
		this.latch = latch;

		solrClient = new CloudSolrClient.Builder().withZkHost(zookeeperURL).build();
		solrClient.setDefaultCollection(collectionName);
		solrClient.connect();
		logger.debug(this.toString() + " INDEXING CLIENT CREATED ...");
	}

	/**
	 * A method invoked to inject the documents into the queue for indexing
	 * threads to use.
	 * @throws Exception 
	 */
	public static void prepare() throws Exception {

		logger.info("Preparing document queue ...");

		documents = new ConcurrentLinkedQueue<SolrInputDocument>();

		String line = "";
		String cvsSplitBy = ",";
		int documentCountInQueue = 0;

		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {

				SolrInputDocument document = new SolrInputDocument();
				line.trim();
				String[] data = line.split(cvsSplitBy);

				document.addField("id", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Title_t", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Article_t", data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Category_t", data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Int1_i", Integer.parseInt(data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Int2_i", Integer.parseInt(data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Float1_f", Float.parseFloat(data[6].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Long1_l", Long.parseLong(data[7].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Double1_d", Double.parseDouble(data[8].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Text_s", data[9].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				documents.add(document);
				documentCountInQueue++;
				if (documentCountInQueue == CloudConcurrentIndexingClient.documentCountLimit + 1000) {
					break;
				}
			}
			br.close();

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}

		logger.info("Preparing document queue [COMPLETE] ...");
	}

	/**
	 * A method invoked by indexing threads.
	 */
	public void run() {

		startTime = System.currentTimeMillis();
		while (true) {

			if (running == true) {
				// Critical Section ....
					try {
						addDocument(collectionName, documents.poll());
					} catch (SolrServerException | IOException e) {
						logger.error(e.getMessage());
						throw new RuntimeException(e.getMessage());
					}
			} else if (running == false) {
				// Break out from loop ...
				synchronized (this) {
					if (!endLock) {
						endLock = true;
						endTime = System.currentTimeMillis();
						totalTime = endTime - startTime;
					}
				}
				logger.debug("Getting out of critical section ...");
				break;
			}
			
			if (CloudConcurrentIndexingClient.documentCount >= CloudConcurrentIndexingClient.documentCountLimit) {
				CloudConcurrentIndexingClient.running = false;
			}		
		}
        latch.countDown();
	}

	/**
	 * A method called by running threads to index the document.
	 * 
	 * @param collectionName
	 * @param document
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	private synchronized SolrResponse addDocument(String collectionName, SolrInputDocument document)
			throws SolrServerException, IOException {
		documentCount++;
		return solrClient.add(collectionName, document, 5000);
	}

	/**
	 * A method invoked to close the open CloudSolrClients.
	 * @throws Exception 
	 */
	public void closeAndCommit() throws Exception {
		try {
			solrClient.commit(collectionName);
			solrClient.close();
		} catch (SolrServerException | IOException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * A method used to reset the static data variables.
	 */
	public static void reset() {
		documents = new ConcurrentLinkedQueue<SolrInputDocument>();
		running = false;
		endLock = false;
		documentCount = 0;
		documentCountLimit = 0;
	}

}