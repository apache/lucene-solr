package org.apache.solr.tests.nightlybenchmarks;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.eclipse.jgit.api.errors.GitAPIException;

public class SolrCloud {

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
	
	public SolrCloud(int solrNodes, String shards, String replicas, String commitId, String configName, String host, boolean creatADefaultCollection) {
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
	
	private void init() {
		
		try {

			zookeeperNode = new Zookeeper();
			int initValue = zookeeperNode.start();
			if (initValue == 0) {
						this.zookeeperIp = zookeeperNode.getZookeeperIp();
						this.zookeeperPort = zookeeperNode.getZookeeperPort();
			} else {
				throw new RuntimeException("Failed to start Zookeeper!");
			}
			
			for (int i = 1; i <= solrNodes; i++) {
				
					SolrNode node = new SolrNode(commitId, this.zookeeperIp, this.zookeeperPort, true);
					node.start();					
					nodes.add(node);					
			}
			
			if (this.createADefaultCollection) {
					nodes.get(0).createCollection(this.collectionName, this.configName, this.shards, this.replicas);
			}
			
			this.port = nodes.get(0).port;
			
			this.url = "http://" + this.host + ":" + this.port + "/solr/" + this.collectionName;
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (GitAPIException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	public void createCollection(String collectionName, String configName, String shards, String replicas) {
		try {
			nodes.get(0).createCollection(collectionName, configName, shards, replicas);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public String getuRL() {
		if (createADefaultCollection) {
			return "http://" + this.host + ":" + this.port + "/solr/" + this.collectionName;
		} else {
			return "http://" + this.host + ":" + this.port + "/solr/";
		}
	}
	
	public void shutdown() {
		for (SolrNode node: nodes) {
			node.stop();
			node.cleanup();
		}
		zookeeperNode.stop();
	}	
	
}
