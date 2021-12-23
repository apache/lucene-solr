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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.eclipse.jgit.api.errors.GitAPIException;

/**
 * This class provides a blueprint for SolrCloud
 * @author Vivek Narang
 *
 */
public class SolrCloud {

	public final static Logger logger = Logger.getLogger(SolrCloud.class);

	public int solrNodes;
	public String shards;
	public String replicas;
	public String port;
	public Zookeeper zookeeperNode;
	public String zookeeperPort;
	public String zookeeperIp;
	public String commitId;
	public String collectionName;
	public String configName;
	public SolrNode masterNode;
	public List<SolrNode> nodes;
	public String url;
	public String host;
	public boolean createADefaultCollection;
	public Map<String, String> returnMapCreateCollection;

	/**
	 * Constructor.
	 * 
	 * @param solrNodes
	 * @param shards
	 * @param replicas
	 * @param commitId
	 * @param configName
	 * @param host
	 * @param creatADefaultCollection
	 * @throws Exception 
	 */
	public SolrCloud(int solrNodes, String shards, String replicas, String commitId, String configName, String host,
			boolean creatADefaultCollection) throws Exception {
		super();
		this.solrNodes = solrNodes;
		this.shards = shards;
		this.replicas = replicas;
		this.commitId = commitId;
		this.configName = configName;
		this.host = host;
		this.collectionName = "Collection_" + UUID.randomUUID();
		this.createADefaultCollection = creatADefaultCollection;
		nodes = new LinkedList<SolrNode>();
		this.init();
	}

	/**
	 * A method used for getting ready to set up the Solr Cloud.
	 * @throws Exception 
	 */
	private void init() throws Exception {

		try {

			zookeeperNode = new Zookeeper();
			int initValue = zookeeperNode.doAction(ZookeeperAction.ZOOKEEPER_START);
			if (initValue == 0) {
				this.zookeeperIp = zookeeperNode.getZookeeperIp();
				this.zookeeperPort = zookeeperNode.getZookeeperPort();
			} else {
				logger.error("Failed to start Zookeeper!");
				throw new RuntimeException("Failed to start Zookeeper!");
			}

			for (int i = 1; i <= solrNodes; i++) {

				SolrNode node = new SolrNode(commitId, this.zookeeperIp, this.zookeeperPort, true);
				node.doAction(SolrNodeAction.NODE_START);
				nodes.add(node);
			}

			if (this.createADefaultCollection) {
				returnMapCreateCollection = nodes.get(0).createCollection(this.collectionName, this.configName,
						this.shards, this.replicas);
			}

			this.port = nodes.get(0).port;
			this.url = "http://" + this.host + ":" + this.port + "/solr/" + this.collectionName;

		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new IOException(e.getMessage());
		} catch (GitAPIException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			throw new InterruptedException(e.getMessage());
		}
	}

	/**
	 * A method used for creating a collection.
	 * 
	 * @param collectionName
	 * @param configName
	 * @param shards
	 * @param replicas
	 * @throws Exception 
	 */
	public void createCollection(String collectionName, String configName, String shards, String replicas) throws Exception {
		try {
			nodes.get(0).createCollection(collectionName, configName, shards, replicas);
		} catch (IOException | InterruptedException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * A method for deleting a collection.
	 * 
	 * @param collectionName
	 * @throws Exception 
	 */
	public void deleteCollection(String collectionName) throws Exception {
		try {
			nodes.get(0).deleteCollection(collectionName);
		} catch (IOException | InterruptedException e) {
			logger.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
	}

	/**
	 * A method used for getting the URL for the solr cloud.
	 * 
	 * @return String
	 */
	public String getuRL() {
		return "http://" + this.host + ":" + this.port + "/solr/";
	}

	/**
	 * A method used to get the zookeeper url for communication with the solr
	 * cloud.
	 * 
	 * @return String
	 */
	public String getZookeeperUrl() {
		return this.zookeeperIp + ":" + this.zookeeperPort;
	}

	/**
	 * A method used for shutting down the solr cloud.
	 * @throws Exception 
	 */
	public void shutdown() throws Exception {
		for (SolrNode node : nodes) {
			node.doAction(SolrNodeAction.NODE_STOP);
			node.cleanup();
		}
		zookeeperNode.doAction(ZookeeperAction.ZOOKEEPER_STOP);
		zookeeperNode.doAction(ZookeeperAction.ZOOKEEPER_CLEAN);
	}
}