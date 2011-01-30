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


import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * A junit4 Solr test harness that extends LuceneTestCaseJ4.
 * Unlike AbstractSolrTestCase, a new core is not created for each test method.
 *
 */
public abstract class SolrTestCaseJ4 extends LuceneTestCase {

  @BeforeClass
  public static void beforeClassSolrTestCase() throws Exception {
    ignoreException("ignore_exception");
  }

  @AfterClass
  public static void afterClassSolrTestCase() throws Exception {
    deleteCore();
    resetExceptionIgnores();
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
    initCore(config, schema, TEST_HOME);
  }

  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
  public static void initCore(String config, String schema, String solrHome) throws Exception {
    startTrackingSearchers();
    configString = config;
    schemaString = schema;
    if (solrHome != null) {
      System.setProperty("solr.solr.home", solrHome);
    }
    initCore();
  }


  static long numOpens;
  static long numCloses;
  protected static void startTrackingSearchers() {
    numOpens = SolrIndexSearcher.numOpens.get();
    numCloses = SolrIndexSearcher.numCloses.get();
  }

  protected static void endTrackingSearchers() {
     long endNumOpens = SolrIndexSearcher.numOpens.get();
     long endNumCloses = SolrIndexSearcher.numCloses.get();

     if (endNumOpens-numOpens != endNumCloses-numCloses) {
       String msg = "ERROR: SolrIndexSearcher opens=" + (endNumOpens-numOpens) + " closes=" + (endNumCloses-numCloses);
       log.error(msg);
       // fail(msg);
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

    createTempDir();

    // other  methods like starting a jetty instance need these too
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    String configFile = getSolrConfigFile();
    if (configFile != null) {

      solrConfig = h.createConfig(getSolrConfigFile());
      h = new TestHarness( dataDir.getAbsolutePath(),
              solrConfig,
              getSchemaFile());
      lrf = h.getRequestFactory
              ("standard",0,20,"version","2.2");
    }
    log.info("####initCore end");
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

    endTrackingSearchers();
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

  /** Validates a query matches some JSON test expressions and closes the query.
   * The text expression is of the form path:JSON.  To facilitate easy embedding
   * in Java strings, the JSON can have double quotes replaced with single quotes.
   *
   * Please use this with care: this makes it easy to match complete structures, but doing so
   * can result in fragile tests if you are matching more than what you want to test.
   *
   **/
  public static void assertJQ(SolrQueryRequest req, String... tests) throws Exception {
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
        String testJSON = test.replace('\'', '"');

        try {
          failed = true;
          String err = JSONTestUtil.match(response, testJSON);
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
    Doc d = doc(fieldsAndValues);
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
  public static String add(Doc doc, String... args) {
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
  public static Doc doc(String... fieldsAndValues) {
    Doc d = new Doc();
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
  public static class Doc {
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
  
  private static final String SOURCE_HOME = determineSourceHome();
  public static String TEST_HOME = getFile("solr/conf").getParent();
  public static String WEBAPP_HOME = new File(SOURCE_HOME, "src/webapp/web").getAbsolutePath();
  public static String EXAMPLE_HOME = new File(SOURCE_HOME, "example/solr").getAbsolutePath();
  public static String EXAMPLE_MULTICORE_HOME = new File(SOURCE_HOME, "example/multicore").getAbsolutePath();
  public static String EXAMPLE_SCHEMA=EXAMPLE_HOME+"/conf/schema.xml";
  public static String EXAMPLE_CONFIG=EXAMPLE_HOME+"/conf/solrconfig.xml";
  
  static String determineSourceHome() {
    // ugly, ugly hack to determine the example home without depending on the CWD
    // this is needed for example/multicore tests which reside outside the classpath
    File base = getFile("solr/conf/").getAbsoluteFile();
    while (!new File(base, "solr/CHANGES.txt").exists()) {
      base = base.getParentFile();
    }
    return new File(base, "solr/").getAbsolutePath();
  }
}
