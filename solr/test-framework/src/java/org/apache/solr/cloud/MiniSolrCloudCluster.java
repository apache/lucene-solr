package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniSolrCloudCluster {
  
  private static Logger log = LoggerFactory.getLogger(MiniSolrCloudCluster.class);

  private ZkTestServer zkServer;
  private List<JettySolrRunner> jettys;
  private File testDir;
  private CloudSolrClient solrClient;

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, File baseDir, File solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters) throws Exception {
    this(numServers, hostContext, baseDir, solrXml, extraServlets, extraRequestFilters, null);
  }

  /**
   * "Mini" SolrCloud cluster to be used for testing
   * @param numServers number of Solr servers to start
   * @param hostContext context path of Solr servers used by Jetty
   * @param baseDir base directory that the mini cluster should be run from
   * @param solrXml solr.xml file to be uploaded to ZooKeeper
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   */
  public MiniSolrCloudCluster(int numServers, String hostContext, File baseDir, File solrXml,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters,
      SSLConfig sslConfig) throws Exception {
    testDir = baseDir;

    String zkDir = testDir.getAbsolutePath() + File.separator
      + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(),
        AbstractZkTestCase.TIMEOUT, 45000, null)) {
      zkClient.makePath("/solr/solr.xml", solrXml, false, true);
    }

    // tell solr to look in zookeeper for solr.xml
    System.setProperty("solr.solrxml.location","zookeeper");
    System.setProperty("zkHost", zkServer.getZkAddress());

    jettys = new LinkedList<JettySolrRunner>();
    for (int i = 0; i < numServers; ++i) {
      if (sslConfig == null) {
        startJettySolrRunner(hostContext, extraServlets, extraRequestFilters);
      } else {
        startJettySolrRunner(hostContext, extraServlets, extraRequestFilters, sslConfig);
      }
    }
    
    solrClient = buildSolrClient();
  }

  /**
   * @return ZooKeeper server used by the MiniCluster
   */
  public ZkTestServer getZkServer() {
    return zkServer;
  }

  /**
   * @return Unmodifiable list of all the currently started Solr Jettys.
   */
  public List<JettySolrRunner> getJettySolrRunners() {
    return Collections.unmodifiableList(jettys);
  }

  /**
   * Start a new Solr instance
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters) throws Exception {
    return startJettySolrRunner(hostContext, extraServlets, extraRequestFilters, null);
  }

  /**
   * Start a new Solr instance
   * @param hostContext context path of Solr servers used by Jetty
   * @param extraServlets Extra servlets to be started by Jetty
   * @param extraRequestFilters extra filters to be started by Jetty
   * @param sslConfig SSL configuration
   * @return new Solr instance
   */
  public JettySolrRunner startJettySolrRunner(String hostContext,
      SortedMap<ServletHolder, String> extraServlets,
      SortedMap<Class, String> extraRequestFilters, SSLConfig sslConfig) throws Exception {
    String context = getHostContextSuitableForServletContext(hostContext);
    JettySolrRunner jetty = new JettySolrRunner(testDir.getAbsolutePath(), context,
      0, null, null, true, extraServlets, sslConfig, extraRequestFilters);
    jetty.start();
    jettys.add(jetty);
    return jetty;
  }

  /**
   * Stop a Solr instance
   * @param index the index of node in collection returned by {@link #getJettySolrRunners()}
   * @return the shut down node
   */
  public JettySolrRunner stopJettySolrRunner(int index) throws Exception {
    JettySolrRunner jetty = jettys.get(index);
    jetty.stop();
    jettys.remove(index);
    return jetty;
  }
  
  public void uploadConfigDir(File configDir, String configName) throws IOException, KeeperException, InterruptedException {
    try(SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null)) {
      ZkController.uploadConfigDir(zkClient, configDir, configName);
    }
  }
  
  public NamedList<Object> createCollection(String name, int numShards, int replicationFactor, 
      String configName, Map<String, String> collectionProperties) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CollectionAction.CREATE.name());
    params.set(CoreAdminParams.NAME, name);
    params.set("numShards", numShards);
    params.set("replicationFactor", replicationFactor);
    params.set("collection.configName", configName);
    if(collectionProperties != null) {
      for(Map.Entry<String, String> property : collectionProperties.entrySet()){
        params.set(CoreAdminParams.PROPERTY_PREFIX + property.getKey(), property.getValue());
      }
    }
    
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    
    return solrClient.request(request);
  }

  /**
   * Shut down the cluster, including all Solr nodes and ZooKeeper
   */
  public void shutdown() throws Exception {
    try {
      solrClient.shutdown();
      for (int i = jettys.size() - 1; i >= 0; --i) {
        stopJettySolrRunner(i);
      }
    } finally {
      try {
        zkServer.shutdown();
      } finally {
        System.clearProperty("solr.solrxml.location");
        System.clearProperty("zkHost");
      }
    }
  }
  
  public CloudSolrClient getSolrClient() {
    return solrClient;
  }
  
  protected CloudSolrClient buildSolrClient() {
    return new CloudSolrClient(getZkServer().getZkAddress());
  }

  private static String getHostContextSuitableForServletContext(String ctx) {
    if (ctx == null || "".equals(ctx)) ctx = "/solr";
    if (ctx.endsWith("/")) ctx = ctx.substring(0,ctx.length()-1);;
    if (!ctx.startsWith("/")) ctx = "/" + ctx;
    return ctx;
  }
}
