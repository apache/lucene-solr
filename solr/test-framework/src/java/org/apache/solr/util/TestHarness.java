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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.Config;
import org.apache.solr.core.ConfigSolrXmlOld;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.logging.ListenerConfig;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.jul.JulWatcher;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.common.util.NamedList.NamedListEntry;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;


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
  String coreName;
  protected volatile CoreContainer container;
  public UpdateRequestHandler updater;
 
  /**
   * Creates a SolrConfig object for the specified coreName assuming it 
   * follows the basic conventions of being a relative path in the solrHome 
   * dir. (ie: <code>${solrHome}/${coreName}/conf/${confFile}</code>
   */
  public static SolrConfig createConfig(String solrHome, String coreName, String confFile) {
    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    try {
      return new SolrConfig(solrHome + File.separator + coreName, confFile, null);
    } catch (Exception xany) {
      throw new RuntimeException(xany);
    }
  }
  
  /**
   * Creates a SolrConfig object for the 
   * {@link CoreContainer#DEFAULT_DEFAULT_CORE_NAME} core using {@link #createConfig(String,String,String)}
   */
  public static SolrConfig createConfig(String solrHome, String confFile) {
    return createConfig(solrHome, CoreContainer.DEFAULT_DEFAULT_CORE_NAME, confFile);
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
   * @param coreName to initialize
   * @param dataDirectory path for index data, will not be cleaned up
   * @param solrConfig solrconfig instance
   * @param indexSchema schema instance
   */
    public TestHarness( String coreName,
                        String dataDirectory,
                        SolrConfig solrConfig,
                        IndexSchema indexSchema) {
        this(coreName, new Initializer(coreName, dataDirectory, solrConfig, indexSchema));
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
  public TestHarness( String dataDirectory,
                      SolrConfig solrConfig,
                      IndexSchema indexSchema) {
      this(null, new Initializer(null, dataDirectory, solrConfig, indexSchema));
  }
  
  public TestHarness(String coreName, CoreContainer.Initializer init) {
    try {

      container = init.initialize();
      if (coreName == null)
        coreName = CoreContainer.DEFAULT_DEFAULT_CORE_NAME;

      this.coreName = coreName;

      updater = new UpdateRequestHandler();
      updater.init( null );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Creates a container based on infos needed to create one core
  static class Initializer extends CoreContainer.Initializer {
    String coreName;
    String dataDirectory;
    SolrConfig solrConfig;
    IndexSchema indexSchema;
    public Initializer(String coreName,
                      String dataDirectory,
                      SolrConfig solrConfig,
                      IndexSchema indexSchema) {
      if (coreName == null)
        coreName = CoreContainer.DEFAULT_DEFAULT_CORE_NAME;
      this.coreName = coreName;
      this.dataDirectory = dataDirectory;
      this.solrConfig = solrConfig;
      this.indexSchema = indexSchema;
    }
    public String getCoreName() {
      return coreName;
    }
    @Override
    public CoreContainer initialize() {
      CoreContainer container;
      try {
        String solrHome = SolrResourceLoader.locateSolrHome();
        container = new CoreContainer(new SolrResourceLoader(solrHome)) {
          {
            String hostPort = System.getProperty("hostPort");
            String hostContext = "solr";
            defaultCoreName = CoreContainer.DEFAULT_DEFAULT_CORE_NAME;
            initShardHandler();
            zkSys.initZooKeeper(this, solrHome, System.getProperty("zkHost"), 30000, hostPort, hostContext, null, "30000", 30000, 30000);
            ByteArrayInputStream is = new ByteArrayInputStream(ConfigSolrXmlOld.DEF_SOLR_XML.getBytes("UTF-8"));
            Config config = new Config(loader, null, new InputSource(is), null, false);
            cfg = new ConfigSolrXmlOld(config, this);
          }
        };
      } catch (ParserConfigurationException e) {
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (SAXException e) {
        throw new RuntimeException(e);
      }
      LogWatcher<?> logging = new JulWatcher("test");
      logging.registerListener(new ListenerConfig());
      container.setLogging(logging);
      
      CoreDescriptor dcore = new CoreDescriptor(container, coreName, solrConfig.getResourceLoader().getInstanceDir());
      dcore.setConfigName(solrConfig.getResourceName());
      dcore.setSchemaName(indexSchema.getResourceName());
      SolrCore core = new SolrCore(coreName, dataDirectory, solrConfig, indexSchema, dcore);
      container.register(coreName, core, false);

      // TODO: we should be exercising the *same* core container initialization code, not equivalent code!
      if (container.getZkController() == null && core.getUpdateHandler().getUpdateLog() != null) {
        // always kick off recovery if we are in standalone mode.
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }
      return container;
    }
  }
  
  public CoreContainer getCoreContainer() {
    return container;
  }

  /** Gets a core that does not have it's refcount incremented (i.e. there is no need to
   * close when done).  This is not MT safe in conjunction with reloads!
   */
  public SolrCore getCore() {
    // get the core & decrease its refcount:
    // the container holds the core for the harness lifetime
    SolrCore core = container.getCore(coreName);
    if (core != null)
      core.close();
    return core;
  }

  /** Gets the core with it's reference count incremented.
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
    SolrCore core = getCoreInc();
    DirectSolrConnection connection = new DirectSolrConnection(core);
    SolrRequestHandler handler = core.getRequestHandler("/update");
    // prefer the handler mapped to /update, but use our generic backup handler
    // if that lookup fails
    if (handler == null) {
      handler = updater;
    }
    try {
      return connection.request(handler, null, xml);
    } catch (SolrException e) {
      throw (SolrException)e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    } finally {
      core.close();
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
    return validateXPath(res, tests);
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
    SolrCore core = getCoreInc();
    try {
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      core.execute(core.getRequestHandler(handler),req,rsp);
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      StringWriter sw = new StringWriter(32000);
      QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
      responseWriter.write(sw,req,rsp);

      req.close();

      return sw.toString();
    } finally {
      req.close();
      SolrRequestInfo.clearRequestInfo();
      core.close();
    }
  }

  /** It is the users responsibility to close the request object when done with it.
   * This method does not set/clear SolrRequestInfo */
  public SolrQueryResponse queryAndResponse(String handler, SolrQueryRequest req) throws Exception {
    SolrCore core = getCoreInc();
    try {
      SolrQueryResponse rsp = new SolrQueryResponse();
      core.execute(core.getRequestHandler(handler),req,rsp);
      if (rsp.getException() != null) {
        throw rsp.getException();
      }
      return rsp;
    } finally {
      core.close();
    }
  }

  /**
   * Shuts down and frees any resources
   */
  public void close() {
    if (container != null) {
      for (SolrCore c : container.getCores()) {
        if (c.getOpenCount() > 1)
          throw new RuntimeException("SolrCore.getOpenCount()=="+c.getOpenCount());
      }      
    }

    if (container != null) {
      container.shutdown();
      container = null;
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
    public Map<String,String> args = new HashMap<String,String>();
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
                                       q[0], qtype, start, limit, args);
      }
      if (q.length%2 != 0) { 
        throw new RuntimeException("The length of the string array (query arguments) needs to be even");
      }
      Map.Entry<String, String> [] entries = new NamedListEntry[q.length / 2];
      for (int i = 0; i < q.length; i += 2) {
        entries[i/2] = new NamedListEntry<String>(q[i], q[i+1]);
      }
      return new LocalSolrQueryRequest(TestHarness.this.getCore(), new NamedList(entries));
    }
  }
}
