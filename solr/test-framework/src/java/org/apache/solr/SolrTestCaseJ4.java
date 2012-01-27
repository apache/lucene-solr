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


package org.apache.solr;


import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.noggit.CharArr;
import org.apache.noggit.JSONUtil;
import org.apache.noggit.ObjectBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.JsonUpdateRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.util.TestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;

/**
 * A junit4 Solr test harness that extends LuceneTestCaseJ4.
 * Unlike AbstractSolrTestCase, a new core is not created for each test method.
 *
 */
public abstract class SolrTestCaseJ4 extends LuceneTestCase {

  @BeforeClass
  public static void beforeClassSolrTestCase() throws Exception {
    startTrackingSearchers();
    startTrackingZkClients();
    ignoreException("ignore_exception");
  }

  @AfterClass
  public static void afterClassSolrTestCase() throws Exception {
    deleteCore();
    resetExceptionIgnores();
    endTrackingSearchers();
    endTrackingZkClients();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    log.info("###Starting " + getName());  // returns <unknown>???
  }

  @Override
  public void tearDown() throws Exception {
    log.info("###Ending " + getName());    
    super.tearDown();
  }

  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
  public static void initCore(String config, String schema) throws Exception {
    initCore(config, schema, TEST_HOME());
  }

  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
  public static void initCore(String config, String schema, String solrHome) throws Exception {
    configString = config;
    schemaString = schema;
    if (solrHome != null) {
      System.setProperty("solr.solr.home", solrHome);
    }
    initCore();
  }


  static long numOpens;
  static long numCloses;
  public static void startTrackingSearchers() {
    numOpens = SolrIndexSearcher.numOpens.getAndSet(0);
    numCloses = SolrIndexSearcher.numCloses.getAndSet(0);
    if (numOpens != 0 || numCloses != 0) {
      // NOTE: some other tests don't use this base class and hence won't reset the counts.
      log.warn("startTrackingSearchers: numOpens="+numOpens+" numCloses="+numCloses);
      try {
        throw new RuntimeException();
      } catch (Exception e) {
        log.error("",e);
      }
      numOpens = numCloses = 0;
    }
  }
  static long zkClientNumOpens;
  static long zkClientNumCloses;
  public static void startTrackingZkClients() {
    zkClientNumOpens = SolrZkClient.numOpens.get();
    zkClientNumCloses = SolrZkClient.numCloses.get();
  }

  public static void endTrackingSearchers() {
     long endNumOpens = SolrIndexSearcher.numOpens.get();
     long endNumCloses = SolrIndexSearcher.numCloses.get();

     // wait a bit in case any ending threads have anything to release
     int retries = 0;
     while (endNumOpens - numOpens != endNumCloses - numCloses) {
       if (retries++ > 30) {
         break;
       }
       try {
         Thread.sleep(1000);
       } catch (InterruptedException e) {}
       endNumOpens = SolrIndexSearcher.numOpens.get();
       endNumCloses = SolrIndexSearcher.numCloses.get();
     }

     SolrIndexSearcher.numOpens.getAndSet(0);
     SolrIndexSearcher.numCloses.getAndSet(0);

     if (endNumOpens-numOpens != endNumCloses-numCloses) {
       String msg = "ERROR: SolrIndexSearcher opens=" + (endNumOpens-numOpens) + " closes=" + (endNumCloses-numCloses);
       log.error(msg);
       testsFailed = true;
       fail(msg);
     }
  }
  
  public static void endTrackingZkClients() {
    long endNumOpens = SolrZkClient.numOpens.get();
    long endNumCloses = SolrZkClient.numCloses.get();

    SolrZkClient.numOpens.getAndSet(0);
    SolrZkClient.numCloses.getAndSet(0);

    
    if (endNumOpens-zkClientNumOpens != endNumCloses-zkClientNumCloses) {
      String msg = "ERROR: SolrZkClient opens=" + (endNumOpens-zkClientNumOpens) + " closes=" + (endNumCloses-zkClientNumCloses);
      log.error(msg);
      testsFailed = true;
      fail(msg);
    }
 }
  
  /** Causes an exception matching the regex pattern to not be logged. */
  public static void ignoreException(String pattern) {
    if (SolrException.ignorePatterns == null)
      SolrException.ignorePatterns = new HashSet<String>();
    SolrException.ignorePatterns.add(pattern);
  }

  public static void resetExceptionIgnores() {
    SolrException.ignorePatterns = null;
    ignoreException("ignore_exception");  // always ignore "ignore_exception"    
  }

  protected static String getClassName() {
    StackTraceElement[] stack = new RuntimeException("WhoAmI").fillInStackTrace().getStackTrace();
    for (int i = stack.length-1; i>=0; i--) {
      StackTraceElement ste = stack[i];
      String cname = ste.getClassName();
      if (cname.indexOf(".lucene.")>=0 || cname.indexOf(".solr.")>=0) {
        return cname;
      }
    }
    return SolrTestCaseJ4.class.getName();
  }

  protected static String getSimpleClassName() {
    String cname = getClassName();
    return cname.substring(cname.lastIndexOf('.')+1);
  }

  protected static String configString;
  protected static String schemaString;

  protected static SolrConfig solrConfig;
  /**
   * Harness initialized by initTestHarness.
   *
   * <p>
   * For use in test methods as needed.
   * </p>
   */
  protected static TestHarness h;
  /**
   * LocalRequestFactory initialized by initTestHarness using sensible
   * defaults.
   *
   * <p>
   * For use in test methods as needed.
   * </p>
   */
  protected static TestHarness.LocalRequestFactory lrf;


  /**
   * Subclasses must define this method to return the name of the
   * schema.xml they wish to use.
   */
  public static  String getSchemaFile() {
    return schemaString;
  };

  /**
   * Subclasses must define this method to return the name of the
   * solrconfig.xml they wish to use.
   */
  public static  String getSolrConfigFile() {
    return configString;
  };

  /**
   * The directory used to story the index managed by the TestHarness h
   */
  protected static File dataDir;

  /**
   * Initializes things your test might need
   *
   * <ul>
   * <li>Creates a dataDir in the "java.io.tmpdir"</li>
   * <li>initializes the TestHarness h using this data directory, and getSchemaPath()</li>
   * <li>initializes the LocalRequestFactory lrf using sensible defaults.</li>
   * </ul>
   *
   */

  public static Logger log = LoggerFactory.getLogger(SolrTestCaseJ4.class);

  private static String factoryProp;

  public static void createTempDir() {
    String cname = getSimpleClassName();
    dataDir = new File(TEMP_DIR,
            "solrtest-" + cname + "-" + System.currentTimeMillis());
    dataDir.mkdirs();
  }

  public static void initCore() throws Exception {
    log.info("####initCore");

    ignoreException("ignore_exception");
    factoryProp = System.getProperty("solr.directoryFactory");
    if (factoryProp == null) {
      System.setProperty("solr.directoryFactory","solr.RAMDirectoryFactory");
    }
    if (dataDir == null) {
      createTempDir();
    }

    // other  methods like starting a jetty instance need these too
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    String configFile = getSolrConfigFile();
    if (configFile != null) {
      createCore();
    }
    log.info("####initCore end");
  }

  public static void createCore() throws Exception {
    solrConfig = h.createConfig(getSolrConfigFile());
    h = new TestHarness( dataDir.getAbsolutePath(),
            solrConfig,
            getSchemaFile());
    lrf = h.getRequestFactory
            ("standard",0,20,CommonParams.VERSION,"2.2");
  }

  /** Subclasses that override setUp can optionally call this method
   * to log the fact that their setUp process has ended.
   */
  public void postSetUp() {
    log.info("####POSTSETUP " + getName());
  }


  /** Subclasses that override tearDown can optionally call this method
   * to log the fact that the tearDown process has started.  This is necessary
   * since subclasses will want to call super.tearDown() at the *end* of their
   * tearDown method.
   */
  public void preTearDown() {
    log.info("####PRETEARDOWN " + getName());
  }

  /**
   * Shuts down the test harness, and makes the best attempt possible
   * to delete dataDir, unless the system property "solr.test.leavedatadir"
   * is set.
   */
  public static void deleteCore() throws Exception {
    log.info("###deleteCore" );
    if (h != null) { h.close(); }
    if (dataDir != null) {
      String skip = System.getProperty("solr.test.leavedatadir");
      if (null != skip && 0 != skip.trim().length()) {
        System.err.println("NOTE: per solr.test.leavedatadir, dataDir will not be removed: " + dataDir.getAbsolutePath());
      } else {
        if (!recurseDelete(dataDir)) {
          System.err.println("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
        }
      }
    }

    if (factoryProp == null) {
      System.clearProperty("solr.directoryFactory");
    }
    
    dataDir = null;
    solrConfig = null;
    h = null;
    lrf = null;
    configString = schemaString = null;
  }


  /** Validates an update XML String is successful
   */
  public static void assertU(String update) {
    assertU(null, update);
  }

  /** Validates an update XML String is successful
   */
  public static void assertU(String message, String update) {
    checkUpdateU(message, update, true);
  }

  /** Validates an update XML String failed
   */
  public static void assertFailedU(String update) {
    assertFailedU(null, update);
  }

  /** Validates an update XML String failed
   */
  public static void assertFailedU(String message, String update) {
    checkUpdateU(message, update, false);
  }

  /** Checks the success or failure of an update message
   */
  private static void checkUpdateU(String message, String update, boolean shouldSucceed) {
    try {
      String m = (null == message) ? "" : message + " ";
      if (shouldSucceed) {
           String res = h.validateUpdate(update);
         if (res != null) fail(m + "update was not successful: " + res);
      } else {
           String res = h.validateErrorUpdate(update);
         if (res != null) fail(m + "update succeeded, but should have failed: " + res);
      }
    } catch (SAXException e) {
      throw new RuntimeException("Invalid XML", e);
    }
  }

  /** Validates a query matches some XPath test expressions and closes the query */
  public static void assertQ(SolrQueryRequest req, String... tests) {
    assertQ(null, req, tests);
  }

  /** Validates a query matches some XPath test expressions and closes the query */
  public static void assertQ(String message, SolrQueryRequest req, String... tests) {
    try {
      String m = (null == message) ? "" : message + " ";
      String response = h.query(req);

      if (req.getParams().getBool("facet", false)) {
        // add a test to ensure that faceting did not throw an exception
        // internally, where it would be added to facet_counts/exception
        String[] allTests = new String[tests.length+1];
        System.arraycopy(tests,0,allTests,1,tests.length);
        allTests[0] = "*[count(//lst[@name='facet_counts']/*[@name='exception'])=0]";
        tests = allTests;
      }

      String results = h.validateXPath(response, tests);

      if (null != results) {
        String msg = "REQUEST FAILED: xpath=" + results
            + "\n\txml response was: " + response
            + "\n\trequest was:" + req.getParamString();

        log.error(msg);
        throw new RuntimeException(msg);
      }

    } catch (XPathExpressionException e1) {
      throw new RuntimeException("XPath is invalid", e1);
    } catch (Exception e2) {
      SolrException.log(log,"REQUEST FAILED: " + req.getParamString(), e2);
      throw new RuntimeException("Exception during query", e2);
    }
  }

  /** Makes a query request and returns the JSON string response */
  public static String JQ(SolrQueryRequest req) throws Exception {
    SolrParams params = req.getParams();
    if (!"json".equals(params.get("wt","xml")) || params.get("indent")==null) {
      ModifiableSolrParams newParams = new ModifiableSolrParams(params);
      newParams.set("wt","json");
      if (params.get("indent")==null) newParams.set("indent","true");
      req.setParams(newParams);
    }

    String response;
    boolean failed=true;
    try {
      response = h.query(req);
      failed = false;
    } finally {
      if (failed) {
        log.error("REQUEST FAILED: " + req.getParamString());
      }
    }

    return response;
  }

  /**
   * Validates a query matches some JSON test expressions using the default double delta tollerance.
   * @see JSONTestUtil#DEFAULT_DELTA
   * @see #assertJQ(SolrQueryRequest,double,String...)
   */
  public static void assertJQ(SolrQueryRequest req, String... tests) throws Exception {
    assertJQ(req, JSONTestUtil.DEFAULT_DELTA, tests);
  }
  /**
   * Validates a query matches some JSON test expressions and closes the
   * query. The text expression is of the form path:JSON.  To facilitate
   * easy embedding in Java strings, the JSON can have double quotes
   * replaced with single quotes.
   * <p>
   * Please use this with care: this makes it easy to match complete
   * structures, but doing so can result in fragile tests if you are
   * matching more than what you want to test.
   * </p>
   * @param req Solr request to execute
   * @param delta tolerance allowed in comparing float/double values
   * @param tests JSON path expression + '==' + expected value
   */
  public static void assertJQ(SolrQueryRequest req, double delta, String... tests) throws Exception {
    SolrParams params =  null;
    try {
      params = req.getParams();
      if (!"json".equals(params.get("wt","xml")) || params.get("indent")==null) {
        ModifiableSolrParams newParams = new ModifiableSolrParams(params);
        newParams.set("wt","json");
        if (params.get("indent")==null) newParams.set("indent","true");
        req.setParams(newParams);
      }

      String response;
      boolean failed=true;
      try {
        response = h.query(req);
        failed = false;
      } finally {
        if (failed) {
          log.error("REQUEST FAILED: " + req.getParamString());
        }
      }

      for (String test : tests) {
        if (test == null || test.length()==0) continue;
        String testJSON = test.replace('\'', '"');

        try {
          failed = true;
          String err = JSONTestUtil.match(response, testJSON, delta);
          failed = false;
          if (err != null) {
            log.error("query failed JSON validation. error=" + err +
                "\n expected =" + testJSON +
                "\n response = " + response +
                "\n request = " + req.getParamString()
            );
            throw new RuntimeException(err);
          }
        } finally {
          if (failed) {
            log.error("JSON query validation threw an exception." + 
                "\n expected =" + testJSON +
                "\n response = " + response +
                "\n request = " + req.getParamString()
            );
          }
        }
      }
    } finally {
      // restore the params
      if (params != null && params != req.getParams()) req.setParams(params);
    }
  }  


  /** Makes sure a query throws a SolrException with the listed response code */
  public static void assertQEx(String message, SolrQueryRequest req, int code ) {
    try {
      h.query(req);
      fail( message );
    } catch (SolrException sex) {
      assertEquals( code, sex.code() );
    } catch (Exception e2) {
      throw new RuntimeException("Exception during query", e2);
    }
  }

  public static void assertQEx(String message, SolrQueryRequest req, SolrException.ErrorCode code ) {
    try {
      h.query(req);
      fail( message );
    } catch (SolrException e) {
      assertEquals( code.code, e.code() );
    } catch (Exception e2) {
      throw new RuntimeException("Exception during query", e2);
    }
  }


  /**
   * @see TestHarness#optimize
   */
  public static String optimize(String... args) {
    return h.optimize(args);
  }
  /**
   * @see TestHarness#commit
   */
  public static String commit(String... args) {
    return h.commit(args);
  }

  /**
   * Generates a simple &lt;add&gt;&lt;doc&gt;... XML String with no options
   *
   * @param fieldsAndValues 0th and Even numbered args are fields names odds are field values.
   * @see #add
   * @see #doc
   */
  public static String adoc(String... fieldsAndValues) {
    XmlDoc d = doc(fieldsAndValues);
    return add(d);
  }

  /**
   * Generates a simple &lt;add&gt;&lt;doc&gt;... XML String with no options
   */
  public static String adoc(SolrInputDocument sdoc) {
    List<String> fields = new ArrayList<String>();
    for (SolrInputField sf : sdoc) {
      for (Object o : sf.getValues()) {
        fields.add(sf.getName());
        fields.add(o.toString());
      }
    }
    return adoc(fields.toArray(new String[fields.size()]));
  }


  /**
   * Generates an &lt;add&gt;&lt;doc&gt;... XML String with options
   * on the add.
   *
   * @param doc the Document to add
   * @param args 0th and Even numbered args are param names, Odds are param values.
   * @see #add
   * @see #doc
   */
  public static String add(XmlDoc doc, String... args) {
    try {
      StringWriter r = new StringWriter();

      // this is anoying
      if (null == args || 0 == args.length) {
        r.write("<add>");
        r.write(doc.xml);
        r.write("</add>");
      } else {
        XML.writeUnescapedXML(r, "add", doc.xml, (Object[])args);
      }

      return r.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException
        ("this should never happen with a StringWriter", e);
    }
  }

  /**
   * Generates a &lt;delete&gt;... XML string for an ID
   *
   * @see TestHarness#deleteById
   */
  public static String delI(String id) {
    return h.deleteById(id);
  }
  /**
   * Generates a &lt;delete&gt;... XML string for an query
   *
   * @see TestHarness#deleteByQuery
   */
  public static String delQ(String q) {
    return h.deleteByQuery(q);
  }

  /**
   * Generates a simple &lt;doc&gt;... XML String with no options
   *
   * @param fieldsAndValues 0th and Even numbered args are fields names, Odds are field values.
   * @see TestHarness#makeSimpleDoc
   */
  public static XmlDoc doc(String... fieldsAndValues) {
    XmlDoc d = new XmlDoc();
    d.xml = h.makeSimpleDoc(fieldsAndValues).toString();
    return d;
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  /**
   * Generates a SolrQueryRequest using the LocalRequestFactory
   * @see #lrf
   */
  public static SolrQueryRequest req(String... q) {
    return lrf.makeRequest(q);
  }

  /**
   * Generates a SolrQueryRequest using the LocalRequestFactory
   * @see #lrf
   */
  public static SolrQueryRequest req(String[] params, String... moreParams) {
    String[] allParams = moreParams;
    if (params.length!=0) {
      int len = params.length + moreParams.length;
      allParams = new String[len];
      System.arraycopy(params,0,allParams,0,params.length);
      System.arraycopy(moreParams,0,allParams,params.length,moreParams.length);
    }

    return lrf.makeRequest(allParams);
  }

  /**
   * Generates a SolrQueryRequest
   */
  public static SolrQueryRequest req(SolrParams params, String... moreParams) {
    ModifiableSolrParams mp = new ModifiableSolrParams(params);
    for (int i=0; i<moreParams.length; i+=2) {
      mp.add(moreParams[i], moreParams[i+1]);
    }
    return new LocalSolrQueryRequest(h.getCore(), mp);
  }

  /** Neccessary to make method signatures un-ambiguous */
  public static class XmlDoc {
    public String xml;
    @Override
    public String toString() { return xml; }
  }

  public static boolean recurseDelete(File f) {
    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!recurseDelete(sub)) {
          System.err.println("!!!! WARNING: best effort to remove " + sub.getAbsolutePath() + " FAILED !!!!!");
          return false;
        }
      }
    }
    return f.delete();
  }
  
  public void clearIndex() {
    assertU(delQ("*:*"));
  }

  /** Send JSON update commands */
  public static String updateJ(String json, SolrParams args) throws Exception {
    SolrCore core = h.getCore();
    if (args == null) {
      args = params("wt","json","indent","true");
    } else {
      ModifiableSolrParams newArgs = new ModifiableSolrParams(args);
      if (newArgs.get("wt") == null) newArgs.set("wt","json");
      if (newArgs.get("indent") == null) newArgs.set("indent","true");
      args = newArgs;
    }
    DirectSolrConnection connection = new DirectSolrConnection(core);
    SolrRequestHandler handler = core.getRequestHandler("/update/json");
    if (handler == null) {
      handler = new JsonUpdateRequestHandler();
      handler.init(null);
    }
    return connection.request(handler, args, json);
  }

  public static SolrInputDocument sdoc(Object... fieldsAndValues) {
    SolrInputDocument sd = new SolrInputDocument();
    for (int i=0; i<fieldsAndValues.length; i+=2) {
      sd.addField((String)fieldsAndValues[i], fieldsAndValues[i+1]);
    }
    return sd;
  }

  /** Creates JSON from a SolrInputDocument.  Doesn't currently handle boosts. */
  public static String json(SolrInputDocument doc) {
     CharArr out = new CharArr();
    try {
      out.append('{');
      boolean firstField = true;
      for (SolrInputField sfield : doc) {
        if (firstField) firstField=false;
        else out.append(',');
        JSONUtil.writeString(sfield.getName(), 0, sfield.getName().length(), out);
        out.append(':');
        if (sfield.getValueCount() > 1) {
          out.append('[');
        }
        boolean firstVal = true;
        for (Object val : sfield) {
          if (firstVal) firstVal=false;
          else out.append(',');
          out.append(JSONUtil.toJSON(val));
        }
        if (sfield.getValueCount() > 1) {
          out.append(']');
        }
      }
      out.append('}');
    } catch (IOException e) {
      // should never happen
    }
    return out.toString();
  }

  /** Creates a JSON add command from a SolrInputDocument list.  Doesn't currently handle boosts. */
  public static String jsonAdd(SolrInputDocument... docs) {
    CharArr out = new CharArr();
    try {
      out.append('[');
      boolean firstField = true;
      for (SolrInputDocument doc : docs) {
        if (firstField) firstField=false;
        else out.append(',');
        out.append(json(doc));
      }
      out.append(']');
    } catch (IOException e) {
      // should never happen
    }
    return out.toString();
  }

    /** Creates a JSON delete command from an id list */
  public static String jsonDelId(Object... ids) {
    CharArr out = new CharArr();
    try {
      out.append('{');
      boolean first = true;
      for (Object id : ids) {
        if (first) first=false;
        else out.append(',');
        out.append("\"delete\":{\"id\":");
        out.append(JSONUtil.toJSON(id));
        out.append('}');
      }
      out.append('}');
    } catch (IOException e) {
      // should never happen
    }
    return out.toString();
  }


  /** Creates a JSON deleteByQuery command */
  public static String jsonDelQ(String... queries) {
    CharArr out = new CharArr();
    try {
      out.append('{');
      boolean first = true;
      for (Object q : queries) {
        if (first) first=false;
        else out.append(',');
        out.append("\"delete\":{\"query\":");
        out.append(JSONUtil.toJSON(q));
        out.append('}');
      }
      out.append('}');
    } catch (IOException e) {
      // should never happen
    }
    return out.toString();
  }


  public static Long addAndGetVersion(SolrInputDocument sdoc, SolrParams params) throws Exception {
    String response = updateJ(jsonAdd(sdoc), params);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    List lst = (List)rsp.get("adds");
    if (lst == null || lst.size() == 0) return null;
    return (Long) lst.get(1);
  }

  public static Long deleteAndGetVersion(String id, SolrParams params) throws Exception {
    String response = updateJ(jsonDelId(id), params);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    List lst = (List)rsp.get("deletes");
    if (lst == null || lst.size() == 0) return null;
    return (Long) lst.get(1);
  }

  public static Long deleteByQueryAndGetVersion(String q, SolrParams params) throws Exception {
    String response = updateJ(jsonDelQ(q), params);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    List lst = (List)rsp.get("deleteByQuery");
    if (lst == null || lst.size() == 0) return null;
    return (Long) lst.get(1);
  }

  /////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////// random document / index creation ///////////////////////
  /////////////////////////////////////////////////////////////////////////////////////
  
  public abstract static class Vals {
    public abstract Comparable get();
    public String toJSON(Comparable val) {
      return JSONUtil.toJSON(val);
    }

    protected int between(int min, int max) {
      return min != max ? random.nextInt(max-min+1) + min : min;
    }
  }

  public abstract static class IVals extends Vals {
    public abstract int getInt();
  }

  public static class IRange extends IVals {
    final int min;
    final int max;
    public IRange(int min, int max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public int getInt() {
      return between(min,max);
    }

    @Override
    public Comparable get() {
      return getInt();
    }
  }

  public static class FVal extends Vals {
    final float min;
    final float max;
    public FVal(float min, float max) {
      this.min = min;
      this.max = max;
    }

    public float getFloat() {
      if (min >= max) return min;
      return min + random.nextFloat() *  (max - min);
    }

    @Override
    public Comparable get() {
      return getFloat();
    }
  }  

  public static class SVal extends Vals {
    char start;
    char end;
    int minLength;
    int maxLength;

    public SVal() {
      this('a','z',1,10);
    }

    public SVal(char start, char end, int minLength, int maxLength) {
      this.start = start;
      this.end = end;
      this.minLength = minLength;
      this.maxLength = maxLength;
    }

    @Override
    public Comparable get() {
      char[] arr = new char[between(minLength,maxLength)];
      for (int i=0; i<arr.length; i++) {
        arr[i] = (char)between(start, end);
      }
      return new String(arr);
    }
  }

  public static final IRange ZERO_ONE = new IRange(0,1);
  public static final IRange ZERO_TWO = new IRange(0,2);
  public static final IRange ONE_ONE = new IRange(1,1);

  public static class Doc implements Comparable {
    public Comparable id;
    public List<Fld> fields;
    public int order; // the order this document was added to the index


    @Override
    public String toString() {
      return "Doc("+order+"):"+fields.toString();
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Doc)) return false;
      Doc other = (Doc)o;
      return this==other || id != null && id.equals(other.id);
    }

    @Override
    public int compareTo(Object o) {
      if (!(o instanceof Doc)) return this.getClass().hashCode() - o.getClass().hashCode();
      Doc other = (Doc)o;
      return this.id.compareTo(other.id);
    }

    public List<Comparable> getValues(String field) {
      for (Fld fld : fields) {
        if (fld.ftype.fname.equals(field)) return fld.vals;
      }
      return null;
    }

    public Comparable getFirstValue(String field) {
      List<Comparable> vals = getValues(field);
      return vals==null || vals.size()==0 ? null : vals.get(0);
    }

    public Map<String,Object> toObject(IndexSchema schema) {
      Map<String,Object> result = new HashMap<String,Object>();
      for (Fld fld : fields) {
        SchemaField sf = schema.getField(fld.ftype.fname);
        if (!sf.multiValued()) {
          result.put(fld.ftype.fname, fld.vals.get(0));
        } else {
          result.put(fld.ftype.fname, fld.vals);
        }
      }
      return result;
    }

  }

  public static class Fld {
    public FldType ftype;
    public List<Comparable> vals;
    @Override
    public String toString() {
      return ftype.fname + "=" + (vals.size()==1 ? vals.get(0).toString() : vals.toString());
    }
  }

  protected class FldType {
    public String fname;
    public IRange numValues;
    public Vals vals;

    public FldType(String fname, Vals vals) {
      this(fname, ZERO_ONE, vals);
    }

    public FldType(String fname, IRange numValues, Vals vals) {
      this.fname = fname;
      this.numValues = numValues;
      this.vals = vals;      
    }

    public Comparable createValue() {
      return vals.get();
    }

    public List<Comparable> createValues() {
      int nVals = numValues.getInt();
      if (nVals <= 0) return null;
      List<Comparable> vals = new ArrayList<Comparable>(nVals);
      for (int i=0; i<nVals; i++)
        vals.add(createValue());
      return vals;
    }

    public Fld createField() {
      List<Comparable> vals = createValues();
      if (vals == null) return null;

      Fld fld = new Fld();
      fld.ftype = this;
      fld.vals = vals;
      return fld;          
    }

  }


  public Map<Comparable,Doc> indexDocs(List<FldType> descriptor, Map<Comparable,Doc> model, int nDocs) throws Exception {
    if (model == null) {
      model = new LinkedHashMap<Comparable,Doc>();
    }

    // commit an average of 10 times for large sets, or 10% of the time for small sets
    int commitOneOutOf = Math.max(nDocs/10, 10);

    for (int i=0; i<nDocs; i++) {
      Doc doc = createDoc(descriptor);
      // doc.order = order++;
      updateJ(toJSON(doc), null);
      model.put(doc.id, doc);

      // commit 10% of the time
      if (random.nextInt(commitOneOutOf)==0) {
        assertU(commit());
      }

      // duplicate 10% of the docs
      if (random.nextInt(10)==0) {
        updateJ(toJSON(doc), null);
        model.put(doc.id, doc);        
      }
    }

    // optimize 10% of the time
    if (random.nextInt(10)==0) {
      assertU(optimize());
    } else {
      assertU(commit());
    }

    // merging segments no longer selects just adjacent segments hence ids (doc.order) can be shuffled.
    // we need to look at the index to determine the order.
    String responseStr = h.query(req("q","*:*", "fl","id", "sort","_docid_ asc", "rows",Integer.toString(model.size()*2), "wt","json", "indent","true"));
    Object response = ObjectBuilder.fromJSON(responseStr);

    response = ((Map)response).get("response");
    response = ((Map)response).get("docs");
    List<Map> docList = (List<Map>)response;
    int order = 0;
    for (Map doc : docList) {
      Object id = doc.get("id");
      Doc modelDoc = model.get(id);
      if (modelDoc == null) continue;  // may be some docs in the index that aren't modeled
      modelDoc.order = order++;
    }

    // make sure we updated the order of all docs in the model
    assertEquals(order, model.size());

    return model;
  }

  public static Doc createDoc(List<FldType> descriptor) {
    Doc doc = new Doc();
    doc.fields = new ArrayList<Fld>();
    for (FldType ftype : descriptor) {
      Fld fld = ftype.createField();
      if (fld != null) {
        doc.fields.add(fld);
        if ("id".equals(ftype.fname))
          doc.id = fld.vals.get(0);
      }
    }
    return doc;
  }

  public static Comparator<Doc> createSort(IndexSchema schema, List<FldType> fieldTypes, String[] out) {
    StringBuilder sortSpec = new StringBuilder();
    int nSorts = random.nextInt(4);
    List<Comparator<Doc>> comparators = new ArrayList<Comparator<Doc>>();
    for (int i=0; i<nSorts; i++) {
      if (i>0) sortSpec.append(',');

      int which = random.nextInt(fieldTypes.size()+2);
      boolean asc = random.nextBoolean();
      if (which == fieldTypes.size()) {
        // sort by score
        sortSpec.append("score").append(asc ? " asc" : " desc");
        comparators.add(createComparator("score", asc, false, false, false));
      } else if (which == fieldTypes.size() + 1) {
        // sort by docid
        sortSpec.append("_docid_").append(asc ? " asc" : " desc");
        comparators.add(createComparator("_docid_", asc, false, false, false));
      } else {
        String field = fieldTypes.get(which).fname;
        sortSpec.append(field).append(asc ? " asc" : " desc");
        SchemaField sf = schema.getField(field);
        comparators.add(createComparator(field, asc, sf.sortMissingLast(), sf.sortMissingFirst(), !(sf.sortMissingLast()||sf.sortMissingFirst()) ));
      }
    }

    out[0] = sortSpec.length() > 0 ? sortSpec.toString() : null;

    if (comparators.size() == 0) {
      // default sort is by score desc
      comparators.add(createComparator("score", false, false, false, false));      
    }

    return createComparator(comparators);
  }

  public static Comparator<Doc> createComparator(final String field, final boolean asc, final boolean sortMissingLast, final boolean sortMissingFirst, final boolean sortMissingAsZero) {
    final int mul = asc ? 1 : -1;

    if (field.equals("_docid_")) {
     return new Comparator<Doc>() {
      @Override
      public int compare(Doc o1, Doc o2) {
        return (o1.order - o2.order) * mul;
      }
     };
    }

    if (field.equals("score")) {
      return createComparator("score_f", asc, sortMissingLast, sortMissingFirst, sortMissingAsZero);
    }

    return new Comparator<Doc>() {
      private Comparable zeroVal(Comparable template) {
        if (template == null) return null;
        if (template instanceof String) return null;  // fast-path for string
        if (template instanceof Integer) return 0;
        if (template instanceof Long) return (long)0;
        if (template instanceof Float) return (float)0;
        if (template instanceof Double) return (double)0;
        if (template instanceof Short) return (short)0;
        if (template instanceof Byte) return (byte)0;
        if (template instanceof Character) return (char)0;
        return null;
      }

      @Override
      public int compare(Doc o1, Doc o2) {
        Comparable v1 = o1.getFirstValue(field);
        Comparable v2 = o2.getFirstValue(field);

        v1 = v1 == null ? zeroVal(v2) : v1;
        v2 = v2 == null ? zeroVal(v1) : v2;

        int c = 0;
        if (v1 == v2) {
          c = 0;
        } else if (v1 == null) {
          if (sortMissingLast) c = mul;
          else if (sortMissingFirst) c = -mul;
          else c = -1;
        } else if (v2 == null) {
          if (sortMissingLast) c = -mul;
          else if (sortMissingFirst) c = mul;
          else c = 1;
        } else {
          c = v1.compareTo(v2);
        }

        c = c * mul;

        return c;
      }
    };
  }

  public static Comparator<Doc> createComparator(final List<Comparator<Doc>> comparators) {
    return new Comparator<Doc>() {
      @Override
      public int compare(Doc o1, Doc o2) {
        int c = 0;
        for (Comparator<Doc> comparator : comparators) {
          c = comparator.compare(o1, o2);
          if (c!=0) return c;
        }
        return o1.order - o2.order;
      }
    };
  }


  public static String toJSON(Doc doc) {
    CharArr out = new CharArr();
    try {
      out.append("{\"add\":{\"doc\":{");
      boolean firstField = true;
      for (Fld fld : doc.fields) {
        if (firstField) firstField=false;
        else out.append(',');
        JSONUtil.writeString(fld.ftype.fname, 0, fld.ftype.fname.length(), out);
        out.append(':');
        if (fld.vals.size() > 1) {
          out.append('[');
        }
        boolean firstVal = true;
        for (Comparable val : fld.vals) {
          if (firstVal) firstVal=false;
          else out.append(',');
          out.append(JSONUtil.toJSON(val));
        }
        if (fld.vals.size() > 1) {
          out.append(']');
        }
      }
      out.append("}}}");
    } catch (IOException e) {
      // should never happen
    }
    return out.toString();
  }

  /** Return a Map from field value to a list of document ids */
  Map<Comparable, List<Comparable>> invertField(Map<Comparable, Doc> model, String field) {
    Map<Comparable, List<Comparable>> value_to_id = new HashMap<Comparable, List<Comparable>>();

    // invert field
    for (Comparable key : model.keySet()) {
      Doc doc = model.get(key);
      List<Comparable> vals = doc.getValues(field);
      if (vals == null) continue;
      for (Comparable val : vals) {
        List<Comparable> ids = value_to_id.get(val);
        if (ids == null) {
          ids = new ArrayList<Comparable>(2);
          value_to_id.put(val, ids);
        }
        ids.add(key);
      }
    }

    return value_to_id;
  }


  /** Gets a resource from the context classloader as {@link File}. This method should only be used,
   * if a real file is needed. To get a stream, code should prefer
   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
   */
  public static File getFile(String name) {
    try {
      File file = new File(name);
      if (!file.exists()) {
        file = new File(Thread.currentThread().getContextClassLoader().getResource(name).toURI());
      }
      return file;
    } catch (Exception e) {
      /* more friendly than NPE */
      throw new RuntimeException("Cannot find resource: " + name);
    }
  }
  
  public static String TEST_HOME() {
    return getFile("solr/conf").getParent();
  }

  public static Throwable getRootCause(Throwable t) {
    Throwable result = t;
    for (Throwable cause = t; null != cause; cause = cause.getCause()) {
      result = cause;
    }
    return result;
  }
}
