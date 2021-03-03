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
package org.apache.solr.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.NamedList.NamedListEntry;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.CorePropertiesLocator;
import org.apache.solr.core.CoresLocator;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrPaths;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.metrics.reporters.SolrJmxReporter;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.update.UpdateShardHandlerConfig;

/**
 * This class provides a simple harness that may be useful when
 * writing testcases.
 *
 * <p>
 * This class lives in the tests-framework source tree (and not in the test source
 * tree), so that it will be included with even the most minimal solr
 * distribution, in order to encourage plugin writers to create unit 
 * tests for their plugins.
 *
 *
 */
public class TestHarness extends BaseTestHarness {
  public volatile String coreName;
  protected volatile CoreContainer container;
  public UpdateRequestHandler updater;
  //private Map<String,SolrCore> cores = new ConcurrentHashMap<>();

  /**
   * Creates a SolrConfig object for the specified coreName assuming it 
   * follows the basic conventions of being a relative path in the solrHome 
   * dir. (ie: <code>${solrHome}/${coreName}/conf/${confFile}</code>
   */
  public static SolrConfig createConfig(Path solrHome, String coreName, String confFile, SolrResourceLoader loader) {
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    try {
      return new SolrConfig(loader, confFile);
    } catch (Exception xany) {
      ParWork.propagateInterrupt(xany);
      throw new RuntimeException(xany);
    }
  }
  
  /**
   * Creates a SolrConfig object for the default test core using {@link #createConfig(Path,String,String,SolrResourceLoader)}
   */
  public static SolrConfig createConfig(Path solrHome, String confFile, SolrResourceLoader loader) {
    return createConfig(solrHome, SolrTestCaseJ4.DEFAULT_TEST_CORENAME, confFile, loader);
  }

  /**
   * @param coreName to initialize
   * @param dataDirectory path for index data, will not be cleaned up
   * @param solrConfig solronfig instance
   * @param schemaFile schema filename
   */
     public TestHarness( String coreName,
                         String dataDirectory,
                         SolrConfig solrConfig,
                         String schemaFile) {
    this( coreName, dataDirectory, solrConfig, IndexSchemaFactory.buildIndexSchema(schemaFile, solrConfig));
  }

   /**
    * @param dataDirectory path for index data, will not be cleaned up
    * @param solrConfig solronfig instance
    * @param schemaFile schema filename
    */
      public TestHarness( String dataDirectory,
                          SolrConfig solrConfig,
                          String schemaFile) {
     this( dataDirectory, solrConfig, IndexSchemaFactory.buildIndexSchema(schemaFile, solrConfig));
   }
   /**
    * @param dataDirectory path for index data, will not be cleaned up
    * @param solrConfig solrconfig instance
    * @param indexSchema schema instance
    */
  public TestHarness(String dataDirectory,
                      SolrConfig solrConfig,
                      IndexSchema indexSchema) {
      this(SolrTestCaseJ4.DEFAULT_TEST_CORENAME, dataDirectory, solrConfig, indexSchema);
  }

  /**
   * @param coreName to initialize
   * @param dataDir path for index data, will not be cleaned up
   * @param solrConfig solrconfig resource name
   * @param indexSchema schema resource name
   */
  public TestHarness(String coreName, String dataDir, String solrConfig, String indexSchema) {
    this(coreName, buildTestNodeConfig(SolrPaths.locateSolrHome()),
        new TestCoresLocator(coreName, dataDir, solrConfig, indexSchema));
  }

  public TestHarness(String coreName, String dataDir, SolrConfig solrConfig, IndexSchema indexSchema) {
    this(coreName, dataDir, solrConfig.getResourceName(), indexSchema.getResourceName());
  }

  /**
   * Create a TestHarness using a specific solr home directory and solr xml
   * @param solrHome the solr home directory
   * @param solrXml the text of a solrxml
   */
  public TestHarness(Path solrHome, String solrXml) {
    this(new SolrXmlConfig().fromString(solrHome, solrXml));
  }

  public TestHarness(NodeConfig nodeConfig) {
    this(null, nodeConfig, new CorePropertiesLocator(nodeConfig.getCoreRootDirectory()));
  }

  public TestHarness(NodeConfig config, CoresLocator coresLocator) {
    this(null, config, coresLocator);
  }

  /**
   * Create a TestHarness using a specific config
   * @param config the ConfigSolr to use
   */
  public TestHarness(String coreName, NodeConfig config, CoresLocator coresLocator) {
    super(config.getSolrResourceLoader());
    this.coreName = (coreName == null) ? SolrTestCaseJ4.DEFAULT_TEST_CORENAME : coreName;
    container = new CoreContainer(config, coresLocator, false);
    container.load();
    updater = new UpdateRequestHandler();
    updater.init(null);
  }

  public static NodeConfig buildTestNodeConfig(Path solrHome) {
    CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder(System.getProperty("host"), Integer.getInteger("hostPort", 8983), System.getProperty("hostContext", ""))
        .setZkClientTimeout(Integer.getInteger("zkClientTimeout", 30000)).build();
    if (System.getProperty("zkHost") == null) cloudConfig = null;
    UpdateShardHandlerConfig updateShardHandlerConfig = new UpdateShardHandlerConfig(HttpClientUtil.DEFAULT_MAXCONNECTIONS, HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST, 30000, 30000,
        UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY, UpdateShardHandlerConfig.DEFAULT_MAXRECOVERYTHREADS);

    boolean disableJmxReporter = true;
    String disableJmxReporterProp = System.getProperty("solr.disableDefaultJmxReporter");
    if (disableJmxReporterProp != null) {
      disableJmxReporter = Boolean.parseBoolean(disableJmxReporterProp);
    }
    if (!disableJmxReporter) {
      // universal default metric reporter
      Map<String,Object> attributes = new HashMap<>();
      attributes.put("name", "default");
      attributes.put("class", SolrJmxReporter.class.getName());
      PluginInfo defaultPlugin = new PluginInfo("reporter", attributes);
      MetricsConfig metricsConfig = new MetricsConfig.MetricsConfigBuilder().setMetricReporterPlugins(new PluginInfo[] {defaultPlugin}).build();

      return new NodeConfig.NodeConfigBuilder("testNode", solrHome).setUseSchemaCache(Boolean.getBoolean("shareSchema")).
          setCloudConfig(cloudConfig).setUpdateShardHandlerConfig(updateShardHandlerConfig).setMetricsConfig(metricsConfig).build();
    } else {
      return new NodeConfig.NodeConfigBuilder("testNode", solrHome).setUseSchemaCache(Boolean.getBoolean("shareSchema")).
          setCloudConfig(cloudConfig).setUpdateShardHandlerConfig(updateShardHandlerConfig).build();
    }
  }

  public static class TestCoresLocator extends ReadOnlyCoresLocator {

    final String coreName;
    final String dataDir;
    final String solrConfig;
    final String schema;

    public TestCoresLocator(String coreName, String dataDir, String solrConfig, String schema) {
      this.coreName = coreName == null ? SolrTestCaseJ4.DEFAULT_TEST_CORENAME : coreName;
      this.dataDir = dataDir;
      this.schema = schema;
      this.solrConfig = solrConfig;
    }

    @Override
    public List<CoreDescriptor> discover(CoreContainer cc) {
      return ImmutableList.of(new CoreDescriptor(coreName, cc.getCoreRootDirectory().resolve(coreName), cc,
          CoreDescriptor.CORE_DATADIR, dataDir,
          CoreDescriptor.CORE_CONFIG, solrConfig,
          CoreDescriptor.CORE_SCHEMA, schema,
          CoreDescriptor.CORE_COLLECTION, System.getProperty("collection", "collection1"),
          CoreDescriptor.CORE_SHARD, System.getProperty("shard", "shard1")));
    }

  }
  
  public CoreContainer getCoreContainer() {
    return container;
  }

  public SolrCore getCore() {
    return container.getCore(coreName);
  }

  /** Gets the core with its reference count incremented.
   * You must call core.close() when done!
   */
  public SolrCore getCoreInc() {
    return container.getCore(coreName);
  }

  public void reload() throws Exception {
    container.reload(coreName);
  }

  /**
   * Processes an "update" (add, commit or optimize) and
   * returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  public String update(String xml) {
    try {
      try (DirectSolrConnection connection = new DirectSolrConnection(getCore())) {

        SolrRequestHandler handler = connection.getCore().getRequestHandler("/update");
        // prefer the handler mapped to /update, but use our generic backup handler
        // if that lookup fails
        if (handler == null) {
          handler = updater;
        }
        return connection.request(handler, null, xml);
      }
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }
  
        
  /**
   * Validates a "query" response against an array of XPath test strings
   *
   * @param req the Query to process
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception IOException if there is a problem writing the XML
   * @see LocalSolrQueryRequest
   */
  public String validateQuery(SolrQueryRequest req, String... tests)
    throws Exception {
                
    String res = query(req);
    return validateXPathWithEntities(loader, res, tests);
  }
            
  /**
   * Processes a "query" using a user constructed SolrQueryRequest
   *
   * @param req the Query to process, will be closed.
   * @return The XML response to the query
   * @exception Exception any exception in the response.
   * @exception IOException if there is a problem writing the XML
   * @see LocalSolrQueryRequest
   */
  public String query(SolrQueryRequest req) throws Exception {
    return query(req.getParams().get(CommonParams.QT), req);
  }

  /**
   * Processes a "query" using a user constructed SolrQueryRequest, and closes the request at the end.
   *
   * @param handler the name of the request handler to process the request
   * @param req the Query to process, will be closed.
   * @return The XML response to the query
   * @exception Exception any exception in the response.
   * @exception IOException if there is a problem writing the XML
   * @see LocalSolrQueryRequest
   */
  public String query(String handler, SolrQueryRequest req) throws Exception {
    try {
      SolrCore core = req.getCore();
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      core.execute(core.getRequestHandler(handler),req,rsp);
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
      if (responseWriter instanceof BinaryQueryResponseWriter) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(768);
        BinaryQueryResponseWriter writer = (BinaryQueryResponseWriter) responseWriter;
        writer.write(byteArrayOutputStream, req, rsp);
        return new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
      } else {
        StringWriter sw = new StringWriter(768);
        responseWriter.write(sw,req,rsp);
        return sw.toString();
      }

    } finally {
      SolrRequestInfo.clearRequestInfo();
    }
  }

  /** It is the users responsibility to close the request object when done with it.
   * This method does not set/clear SolrRequestInfo */
  public SolrQueryResponse queryAndResponse(String handler, SolrQueryRequest req) throws Exception {
    try (SolrCore core = getCoreInc()) {
      SolrQueryResponse rsp = new SolrQueryResponse();
      core.execute(core.getRequestHandler(handler), req, rsp);
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      return rsp;
    }
  }

  /**
   * Shuts down and frees any resources
   */
  public void close() {
    if (container != null) {
      try {
        container.shutdown();
      } finally {
        container = null;
      }

    }
  }

  public LocalRequestFactory getRequestFactory(String qtype,
                                               int start,
                                               int limit) {
    LocalRequestFactory f = new LocalRequestFactory();
    f.qtype = qtype;
    f.start = start;
    f.limit = limit;
    return f;
  }
    
  /**
   * 0 and Even numbered args are keys, Odd numbered args are values.
   */
  public LocalRequestFactory getRequestFactory(String qtype,
                                               int start, int limit,
                                               String... args) {
    LocalRequestFactory f = getRequestFactory(qtype, start, limit);
    for (int i = 0; i < args.length; i+=2) {
      f.args.put(args[i], args[i+1]);
    }
    return f;
        
  }
    
  public LocalRequestFactory getRequestFactory(String qtype,
                                               int start, int limit,
                                               Map<String,String> args) {

    LocalRequestFactory f = getRequestFactory(qtype, start, limit);
    f.args.putAll(args);
    return f;
  }
    
  /**
   * A Factory that generates LocalSolrQueryRequest objects using a
   * specified set of default options.
   */
  public class LocalRequestFactory {
    public String qtype = null;
    public int start = 0;
    public int limit = 1000;
    public Map<String,String> args = new HashMap<>();
    public LocalRequestFactory() {
    }
    /**
     * Creates a LocalSolrQueryRequest based on variable args; for
     * historical reasons, this method has some peculiar behavior:
     * <ul>
     *   <li>If there is a single arg, then it is treated as the "q"
     *       param, and the LocalSolrQueryRequest consists of that query
     *       string along with "qt", "start", and "rows" params (based
     *       on the qtype, start, and limit properties of this factory)
     *       along with any other default "args" set on this factory.
     *   </li>
     *   <li>If there are multiple args, then there must be an even number
     *       of them, and each pair of args is used as a key=value param in
     *       the LocalSolrQueryRequest.  <b>NOTE: In this usage, the "qtype",
     *       "start", "limit", and "args" properties of this factory are
     *       ignored.</b>
     *   </li>
     * </ul>
     *
     * TODO: this isn't really safe in the presense of core reloads!
     * Perhaps the best we could do is increment the core reference count
     * and decrement it in the request close() method?
     */
    public LocalSolrQueryRequest makeRequest(String ... q) {
      if (q.length==1) {
        return new LocalSolrQueryRequest(TestHarness.this.getCore(),
                                       q[0], qtype, start, limit, args, true);
      }
      if (q.length%2 != 0) { 
        throw new RuntimeException("The length of the string array (query arguments) needs to be even");
      }
      Map.Entry<String, String> [] entries = new NamedListEntry[q.length / 2];
      for (int i = 0; i < q.length; i += 2) {
        entries[i/2] = new NamedListEntry<>(q[i], q[i+1]);
      }
      NamedList nl = new NamedList(entries);
      if(nl.get("wt" ) == null) nl.add("wt","xml");
      return new LocalSolrQueryRequest(TestHarness.this.getCore(), nl, true);
    }
  }


}
