/**
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
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.JsonUpdateRequestHandler;
import org.apache.solr.handler.XmlUpdateRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.servlet.DirectSolrConnection;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import org.apache.solr.common.util.NamedList.NamedListEntry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;


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
public class TestHarness {
  protected CoreContainer container;
  private SolrCore core;
  private XPath xpath = XPathFactory.newInstance().newXPath();
  private DocumentBuilder builder;
  public XmlUpdateRequestHandler updater;
        
  public static SolrConfig createConfig(String confFile) {
      // set some system properties for use by tests
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");
      try {
      return new SolrConfig(confFile);
      }
      catch(Exception xany) {
        throw new RuntimeException(xany);
      }
  }
        
  /**
   * Assumes "solrconfig.xml" is the config file to use, and
   * "schema.xml" is the schema path to use.
   *
   * @param dataDirectory path for index data, will not be cleaned up
   */
  public TestHarness( String dataDirectory) {
    this( dataDirectory, "schema.xml");
  }
  
  /**
   * Assumes "solrconfig.xml" is the config file to use.
   *
   * @param dataDirectory path for index data, will not be cleaned up
   * @param schemaFile path of schema file
   */
  public TestHarness( String dataDirectory, String schemaFile) {
    this( dataDirectory, "solrconfig.xml", schemaFile);
  }
  /**
   * @param dataDirectory path for index data, will not be cleaned up
   * @param configFile solrconfig filename
   * @param schemaFile schema filename
   */
   public TestHarness( String dataDirectory, String configFile, String schemaFile) {
     this( dataDirectory, createConfig(configFile), schemaFile);
   }
   /**
    * @param dataDirectory path for index data, will not be cleaned up
    * @param solrConfig solronfig instance
    * @param schemaFile schema filename
    */
      public TestHarness( String dataDirectory,
                          SolrConfig solrConfig,
                          String schemaFile) {
     this( dataDirectory, solrConfig, new IndexSchema(solrConfig, schemaFile, null));
   }
   /**
    * @param dataDirectory path for index data, will not be cleaned up
    * @param solrConfig solrconfig instance
    * @param indexSchema schema instance
    */
  public TestHarness( String dataDirectory,
                      SolrConfig solrConfig,
                      IndexSchema indexSchema) {
      this("", new Initializer("", dataDirectory, solrConfig, indexSchema));
  }
  
  public TestHarness(String coreName, CoreContainer.Initializer init) {
    try {
      container = init.initialize();
      if (coreName == null)
        coreName = "";
      // get the core & decrease its refcount:
      // the container holds the core for the harness lifetime
      core = container.getCore(coreName);
      if (core != null)
        core.close();
      builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      
      updater = new XmlUpdateRequestHandler();
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
        coreName = "";
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
      CoreContainer container = new CoreContainer(new SolrResourceLoader(SolrResourceLoader.locateSolrHome())) {
        {
          hostPort = System.getProperty("hostPort");
          hostContext = "solr";
          defaultCoreName = "collection1";
          initZooKeeper(System.getProperty("zkHost"), 10000);
        }
      };
      
      CoreDescriptor dcore = new CoreDescriptor(container, coreName, solrConfig.getResourceLoader().getInstanceDir());
      dcore.setConfigName(solrConfig.getResourceName());
      dcore.setSchemaName(indexSchema.getResourceName());
      SolrCore core = new SolrCore(coreName, dataDirectory, solrConfig, indexSchema, dcore);
      container.register(coreName, core, false);

      return container;
    }
  }
  
  public CoreContainer getCoreContainer() {
    return container;
  }

  public SolrCore getCore() {
    return core;
  }
        
  /**
   * Processes an "update" (add, commit or optimize) and
   * returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  public String update(String xml) {
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
    }
  }
  
        
  /**
   * Validates that an "update" (add, commit or optimize) results in success.
   *
   * :TODO: currently only deals with one add/doc at a time, this will need changed if/when SOLR-2 is resolved
   * 
   * @param xml The XML of the update
   * @return null if successful, otherwise the XML response to the update
   */
  public String validateUpdate(String xml) throws SAXException {
    return checkUpdateStatus(xml, "0");
  }

  /**
   * Validates that an "update" (add, commit or optimize) results in success.
   *
   * :TODO: currently only deals with one add/doc at a time, this will need changed if/when SOLR-2 is resolved
   * 
   * @param xml The XML of the update
   * @return null if successful, otherwise the XML response to the update
   */
  public String validateErrorUpdate(String xml) throws SAXException {
    try {
      return checkUpdateStatus(xml, "1");
    } catch (SolrException e) {
      // return ((SolrException)e).getMessage();
      return null;  // success
    }
  }

  /**
   * Validates that an "update" (add, commit or optimize) results in success.
   *
   * :TODO: currently only deals with one add/doc at a time, this will need changed if/when SOLR-2 is resolved
   * 
   * @param xml The XML of the update
   * @return null if successful, otherwise the XML response to the update
   */
  public String checkUpdateStatus(String xml, String code) throws SAXException {
    try {
      String res = update(xml);
      String valid = validateXPath(res, "//int[@name='status']="+code );
      return (null == valid) ? null : res;
    } catch (XPathExpressionException e) {
      throw new RuntimeException
        ("?!? static xpath has bug?", e);
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
    throws IOException, Exception {
                
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
  public String query(SolrQueryRequest req) throws IOException, Exception {
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
  public String query(String handler, SolrQueryRequest req) throws IOException, Exception {
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
    }
  }

  /** It is the users responsibility to close the request object when done with it.
   * This method does not set/clear SolrRequestInfo */
  public SolrQueryResponse queryAndResponse(String handler, SolrQueryRequest req) throws Exception {
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(core.getRequestHandler(handler),req,rsp);
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
    return rsp;
  }


  /**
   * A helper method which valides a String against an array of XPath test
   * strings.
   *
   * @param xml The xml String to validate
   * @param tests Array of XPath strings to test (in boolean mode) on the xml
   * @return null if all good, otherwise the first test that fails.
   */
  public String validateXPath(String xml, String... tests)
    throws XPathExpressionException, SAXException {
        
    if (tests==null || tests.length == 0) return null;
                
    Document document=null;
    try {
      document = builder.parse(new ByteArrayInputStream
                               (xml.getBytes("UTF-8")));
    } catch (UnsupportedEncodingException e1) {
      throw new RuntimeException("Totally weird UTF-8 exception", e1);
    } catch (IOException e2) {
      throw new RuntimeException("Totally weird io exception", e2);
    }
                
    for (String xp : tests) {
      xp=xp.trim();
      Boolean bool = (Boolean) xpath.evaluate(xp, document,
                                              XPathConstants.BOOLEAN);

      if (!bool) {
        return xp;
      }
    }
    return null;
                
  }

  /**
   * Shuts down and frees any resources
   */
  public void close() {
    if (container != null) {
      for (SolrCore c : container.getCores()) {
        if (c.getOpenCount() > 1)
          throw new RuntimeException("SolrCore.getOpenCount()=="+core.getOpenCount());
      }      
    }

    if (container != null) {
      container.shutdown();
      container = null;
    }
  }

  /**
   * A helper that creates an xml &lt;doc&gt; containing all of the
   * fields and values specified
   *
   * @param fieldsAndValues 0 and Even numbered args are fields names odds are field values.
   */
  public static StringBuffer makeSimpleDoc(String... fieldsAndValues) {

    try {
      StringWriter w = new StringWriter();
      w.append("<doc>");
      for (int i = 0; i < fieldsAndValues.length; i+=2) {
        XML.writeXML(w, "field", fieldsAndValues[i+1], "name",
                     fieldsAndValues[i]);
      }
      w.append("</doc>");
      return w.getBuffer();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }

  /**
   * Generates a delete by query xml string
   * @param q Query that has not already been xml escaped
   */
  public static String deleteByQuery(String q) {
    return delete("query", q);
  }
  /**
   * Generates a delete by id xml string
   * @param id ID that has not already been xml escaped
   */
  public static String deleteById(String id) {
    return delete("id", id);
  }
        
  /**
   * Generates a delete xml string
   * @param val text that has not already been xml escaped
   */
  private static String delete(String deltype, String val) {
    try {
      StringWriter r = new StringWriter();
            
      r.write("<delete>");
      XML.writeXML(r, deltype, val);
      r.write("</delete>");
            
      return r.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }
    
  /**
   * Helper that returns an &lt;optimize&gt; String with
   * optional key/val pairs.
   *
   * @param args 0 and Even numbered args are params, Odd numbered args are values.
   */
  public static String optimize(String... args) {
    return simpleTag("optimize", args);
  }

  private static String simpleTag(String tag, String... args) {
    try {
      StringWriter r = new StringWriter();

      // this is annoying
      if (null == args || 0 == args.length) {
        XML.writeXML(r, tag, null);
      } else {
        XML.writeXML(r, tag, null, (Object[])args);
      }
      return r.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }
    
  /**
   * Helper that returns an &lt;commit&gt; String with
   * optional key/val pairs.
   *
   * @param args 0 and Even numbered args are params, Odd numbered args are values.
   */
  public static String commit(String... args) {
    return simpleTag("commit", args);
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
    public String qtype = "standard";
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
