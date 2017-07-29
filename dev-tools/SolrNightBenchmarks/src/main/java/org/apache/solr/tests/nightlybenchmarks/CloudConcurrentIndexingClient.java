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

	public static boolean running;
	public static boolean endLock = false;
	public static long documentCount = 0;
	public static ConcurrentLinkedQueue<SolrInputDocument> documents = new ConcurrentLinkedQueue<SolrInputDocument>();
	public static long totalTime = 0;

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
	public CloudConcurrentIndexingClient(String zookeeperURL, String collectionName) {
		super();
		this.collectionName = collectionName;
		solrClient = new CloudSolrClient.Builder().withZkHost(zookeeperURL).build();
		solrClient.setDefaultCollection(collectionName);
		solrClient.connect();
		Util.postMessage("\r" + this.toString() + "** QUERY CLIENT CREATED ... Testing Type: ", MessageType.GREEN_TEXT,
				false);
	}

	/**
	 * A method invoked to inject the documents into the queue for indexing
	 * threads to use.
	 */
	public static void prepare() {

		Util.postMessage("** Preparing document queue ...", MessageType.CYAN_TEXT, false);

		documents = new ConcurrentLinkedQueue<SolrInputDocument>();

		String line = "";
		String cvsSplitBy = ",";
		int documentCount = 0;

		try (BufferedReader br = new BufferedReader(new FileReader(Util.TEST_DATA_DIRECTORY + Util.ONEM_TEST_DATA))) {

			while ((line = br.readLine()) != null) {

				SolrInputDocument document = new SolrInputDocument();
				line.trim();
				String[] data = line.split(cvsSplitBy);

				document.addField("id", data[0].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Title_t", data[1].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Article_t", data[2].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Category_t", data[3].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				document.addField("Int1_pi", Integer.parseInt(data[4].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Int2_pi", Integer.parseInt(data[5].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Float1_pf", Float.parseFloat(data[6].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Long1_pl", Long.parseLong(data[7].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Double1_pd", Double.parseDouble(data[8].replaceAll("[^\\sa-zA-Z0-9]", "").trim()));
				document.addField("Text_s", data[9].replaceAll("[^\\sa-zA-Z0-9]", "").trim());
				documentCount++;
				documents.add(document);

				if (documentCount > 25000) {
					break;
				}
			}
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		Util.postMessage("** Preparing document queue [COMPLETE] ..." + documents.size(), MessageType.GREEN_TEXT,
				false);
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
					e.printStackTrace();
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
				Util.postMessage("\r" + this.toString() + "** Getting out of critical section ...",
						MessageType.RED_TEXT, false);
				break;
			}
		}
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
	 */
	public void closeAndCommit() {
		try {
			solrClient.commit(collectionName);
			solrClient.close();
		} catch (SolrServerException | IOException e) {
			e.printStackTrace();
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
	}

}