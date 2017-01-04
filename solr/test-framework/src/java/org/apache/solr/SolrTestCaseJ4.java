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
package org.apache.solr;

import javax.xml.xpath.XPathExpressionException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.IpTables;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoresLocator;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.DirectSolrConnection;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.RandomizeSSL.SSLRandomizer;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.TestHarness;
import org.apache.solr.util.TestInjection;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.noggit.CharArr;
import org.noggit.JSONUtil;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import static java.util.Objects.requireNonNull;

/**
 * A junit4 Solr test harness that extends LuceneTestCaseJ4. To change which core is used when loading the schema and solrconfig.xml, simply
 * invoke the {@link #initCore(String, String, String, String)} method.
 * 
 * Unlike {@link AbstractSolrTestCase}, a new core is not created for each test method.
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class
})
@SuppressSysoutChecks(bugUrl = "Solr dumps tons of logs to console.")
@SuppressFileSystems("ExtrasFS") // might be ok, the failures with e.g. nightly runs might be "normal"
@RandomizeSSL()
public abstract class SolrTestCaseJ4 extends LuceneTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DEFAULT_TEST_COLLECTION_NAME = "collection1";
  public static final String DEFAULT_TEST_CORENAME = DEFAULT_TEST_COLLECTION_NAME;
  protected static final String CORE_PROPERTIES_FILENAME = "core.properties";

  // keep solr.tests.mergePolicyFactory use i.e. do not remove with SOLR-8668
  public static final String SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY = "solr.tests.mergePolicyFactory";
  @Deprecated // remove solr.tests.mergePolicy use with SOLR-8668
  public static final String SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY = "solr.tests.mergePolicy";

  @Deprecated // remove solr.tests.useMergePolicyFactory with SOLR-8668
  public static final String SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY = "solr.tests.useMergePolicyFactory";
  @Deprecated // remove solr.tests.useMergePolicy use with SOLR-8668
  public static final String SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY = "solr.tests.useMergePolicy";

  private static String coreName = DEFAULT_TEST_CORENAME;

  public static int DEFAULT_CONNECTION_TIMEOUT = 60000;  // default socket connection timeout in ms

  protected void writeCoreProperties(Path coreDirectory, String corename) throws IOException {
    Properties props = new Properties();
    props.setProperty("name", corename);
    props.setProperty("configSet", "collection1");
    props.setProperty("config", "${solrconfig:solrconfig.xml}");
    props.setProperty("schema", "${schema:schema.xml}");

    writeCoreProperties(coreDirectory, props, this.getTestName());
  }

  public static void writeCoreProperties(Path coreDirectory, Properties properties, String testname) throws IOException {
    log.info("Writing core.properties file to {}", coreDirectory);
    Files.createDirectories(coreDirectory);
    try (Writer writer =
             new OutputStreamWriter(Files.newOutputStream(coreDirectory.resolve(CORE_PROPERTIES_FILENAME)), Charset.forName("UTF-8"))) {
      properties.store(writer, testname);
    }
  }

  /**
   * Annotation for test classes that want to disable SSL
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressSSL {
    /** Point to JIRA entry. */
    public String bugUrl() default "None";
  }
  
  /**
   * Annotation for test classes that want to disable ObjectReleaseTracker
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressObjectReleaseTracker {
    /** Point to JIRA entry. */
    public String bugUrl();
  }
  
  // these are meant to be accessed sequentially, but are volatile just to ensure any test
  // thread will read the latest value
  protected static volatile SSLTestConfig sslConfig;

  @ClassRule
  public static TestRule solrClassRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule())
             .around(new RevertDefaultThreadHandlerRule());

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  @BeforeClass
  public static void setupTestCases() {

    initClassLogLevels();

    initCoreDataDir = createTempDir("init-core-data").toFile();
    System.err.println("Creating dataDir: " + initCoreDataDir.getAbsolutePath());

    System.setProperty("zookeeper.forceSync", "no");
    System.setProperty("jetty.testMode", "true");
    System.setProperty("enable.update.log", usually() ? "true" : "false");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    System.setProperty("solr.clustering.enabled", "false");
    System.setProperty("solr.peerSync.useRangeVersions", String.valueOf(random().nextBoolean()));
    startTrackingSearchers();
    ignoreException("ignore_exception");
    newRandomConfig();
    
    sslConfig = buildSSLConfig();
    // based on randomized SSL config, set SchemaRegistryProvider appropriately
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    if(isSSLMode()) {
      // SolrCloud tests should usually clear this
      System.setProperty("urlScheme", "https");
    }
  }

  @AfterClass
  public static void teardownTestCases() throws Exception {
    try {
      deleteCore();
      resetExceptionIgnores();
      
      if (suiteFailureMarker.wasSuccessful()) {
        // if the tests passed, make sure everything was closed / released
        if (!RandomizedContext.current().getTargetClass().isAnnotationPresent(SuppressObjectReleaseTracker.class)) {
          endTrackingSearchers(120, false);
          String orr = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty(30);
          assertNull(orr, orr);
        } else {
          endTrackingSearchers(15, false);
          String orr = ObjectReleaseTracker.checkEmpty();
          if (orr != null) {
            log.warn(
                "Some resources were not closed, shutdown, or released. This has been ignored due to the SuppressObjectReleaseTracker annotation, trying to close them now.");
            ObjectReleaseTracker.tryClose();
          }
        }
      }
      resetFactory();
      coreName = DEFAULT_TEST_CORENAME;
    } finally {
      ObjectReleaseTracker.clear();
      TestInjection.reset();
      initCoreDataDir = null;
      System.clearProperty("zookeeper.forceSync");
      System.clearProperty("jetty.testMode");
      System.clearProperty("tests.shardhandler.randomSeed");
      System.clearProperty("enable.update.log");
      System.clearProperty("useCompoundFile");
      System.clearProperty("urlScheme");
      System.clearProperty("solr.peerSync.useRangeVersions");
      
      HttpClientUtil.resetHttpClientBuilder();

      // clean up static
      sslConfig = null;
      testSolrHome = null;
    }
    
    IpTables.unblockAllPorts();

    LogLevel.Configurer.restoreLogLevels(savedClassLogLevels);
    savedClassLogLevels.clear();
  }

  private static Map<String, String> savedClassLogLevels = new HashMap<>();

  public static void initClassLogLevels() {
    Class currentClass = RandomizedContext.current().getTargetClass();
    LogLevel annotation = (LogLevel) currentClass.getAnnotation(LogLevel.class);
    if (annotation == null) {
      return;
    }
    Map<String, String> previousLevels = LogLevel.Configurer.setLevels(annotation.value());
    savedClassLogLevels.putAll(previousLevels);
  }

  private Map<String, String> savedMethodLogLevels = new HashMap<>();

  @Before
  public void initMethodLogLevels() {
    Method method = RandomizedContext.current().getTargetMethod();
    LogLevel annotation = method.getAnnotation(LogLevel.class);
    if (annotation == null) {
      return;
    }
    Map<String, String> previousLevels = LogLevel.Configurer.setLevels(annotation.value());
    savedMethodLogLevels.putAll(previousLevels);
  }

  @After
  public void restoreMethodLogLevels() {
    LogLevel.Configurer.restoreLogLevels(savedMethodLogLevels);
    savedMethodLogLevels.clear();
  }
  
  protected static boolean isSSLMode() {
    return sslConfig != null && sslConfig.isSSLMode();
  }

  private static boolean changedFactory = false;
  private static String savedFactory;
  /** Use a different directory factory.  Passing "null" sets to an FS-based factory */
  public static void useFactory(String factory) throws Exception {
    // allow calling more than once so a subclass can override a base class
    if (!changedFactory) {
      savedFactory = System.getProperty("solr.DirectoryFactory");
    }

    if (factory == null) {
      factory = random().nextInt(100) < 75 ? "solr.NRTCachingDirectoryFactory" : "solr.StandardDirectoryFactory"; // test the default most of the time
    }
    System.setProperty("solr.directoryFactory", factory);
    changedFactory = true;
  }

  public static void resetFactory() throws Exception {
    if (!changedFactory) return;
    changedFactory = false;
    if (savedFactory != null) {
      System.setProperty("solr.directoryFactory", savedFactory);
    } else {
      System.clearProperty("solr.directoryFactory");
    }
  }

  private static SSLTestConfig buildSSLConfig() {

    SSLRandomizer sslRandomizer =
      SSLRandomizer.getSSLRandomizerForClass(RandomizedContext.current().getTargetClass());
    
    if (Constants.MAC_OS_X) {
      // see SOLR-9039
      // If a solution is found to remove this, please make sure to also update
      // TestMiniSolrCloudClusterSSL.testSslAndClientAuth as well.
      sslRandomizer = new SSLRandomizer(sslRandomizer.ssl, 0.0D, (sslRandomizer.debug + " w/ MAC_OS_X supressed clientAuth"));
    }

    SSLTestConfig result = sslRandomizer.createSSLTestConfig();
    log.info("Randomized ssl ({}) and clientAuth ({}) via: {}",
             result.isSSLMode(), result.isClientAuthMode(), sslRandomizer.debug);
    return result;
  }

  protected static JettyConfig buildJettyConfig(String context) {
    return JettyConfig.builder().setContext(context).withSSLConfig(sslConfig).build();
  }
  
  protected static String buildUrl(final int port, final String context) {
    return (isSSLMode() ? "https" : "http") + "://127.0.0.1:" + port + context;
  }

  protected static MockTokenizer whitespaceMockTokenizer(Reader input) throws IOException {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    mockTokenizer.setReader(input);
    return mockTokenizer;
  }

  protected static MockTokenizer whitespaceMockTokenizer(String input) throws IOException {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    mockTokenizer.setReader(new StringReader(input));
    return mockTokenizer;
  }

  /**
   * Call this from @BeforeClass to set up the test harness and update handler with no cores.
   *
   * @param solrHome The solr home directory.
   * @param xmlStr - the text of an XML file to use. If null, use the what's the absolute minimal file.
   * @throws Exception Lost of file-type things can go wrong.
   */
  public static void setupNoCoreTest(Path solrHome, String xmlStr) throws Exception {

    if (xmlStr == null)
      xmlStr = "<solr></solr>";
    Files.write(solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE), xmlStr.getBytes(StandardCharsets.UTF_8));
    h = new TestHarness(SolrXmlConfig.fromSolrHome(solrHome));
    lrf = h.getRequestFactory("standard", 0, 20, CommonParams.VERSION, "2.2");
  }
  
  /** sets system properties based on 
   * {@link #newIndexWriterConfig(org.apache.lucene.analysis.Analyzer)}
   * 
   * configs can use these system properties to vary the indexwriter settings
   */
  public static void newRandomConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));

    System.setProperty("useCompoundFile", String.valueOf(iwc.getUseCompoundFile()));

    System.setProperty("solr.tests.maxBufferedDocs", String.valueOf(iwc.getMaxBufferedDocs()));
    System.setProperty("solr.tests.ramBufferSizeMB", String.valueOf(iwc.getRAMBufferSizeMB()));

    String mergeSchedulerClass = iwc.getMergeScheduler().getClass().getName();
    if (mergeSchedulerClass.contains("$")) {
      // anonymous subclass - we can't instantiate via the resource loader, so use CMS instead
      mergeSchedulerClass = "org.apache.lucene.index.ConcurrentMergeScheduler";
    }
    System.setProperty("solr.tests.mergeScheduler", mergeSchedulerClass);
  }

  public static Throwable getWrappedException(Throwable e) {
    while (e != null && e.getCause() != e && e.getCause() != null) {
      e = e.getCause();
    }
    return e;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    log.info("###Starting " + getTestName());  // returns <unknown>???
  }

  @Override
  public void tearDown() throws Exception {
    log.info("###Ending " + getTestName());    
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
    assertNotNull(solrHome);
    configString = config;
    schemaString = schema;
    testSolrHome = Paths.get(solrHome);
    System.setProperty("solr.solr.home", solrHome);
    initCore();
  }

  /** Call initCore in @BeforeClass to instantiate a solr core in your test class.
   * deleteCore will be called for you via SolrTestCaseJ4 @AfterClass */
  public static void initCore(String config, String schema, String solrHome, String pCoreName) throws Exception {
    coreName=pCoreName;
    initCore(config,schema,solrHome);
  }
  
  static long numOpens;
  static long numCloses;
  public static void startTrackingSearchers() {
    numOpens = SolrIndexSearcher.numOpens.getAndSet(0);
    numCloses = SolrIndexSearcher.numCloses.getAndSet(0);
    if (numOpens != 0 || numCloses != 0) {
      // NOTE: some other tests don't use this base class and hence won't reset the counts.
      log.warn("startTrackingSearchers: numOpens="+numOpens+" numCloses="+numCloses);
      numOpens = numCloses = 0;
    }
  }

  public static void endTrackingSearchers(int waitSeconds, boolean failTest) {
     long endNumOpens = SolrIndexSearcher.numOpens.get();
     long endNumCloses = SolrIndexSearcher.numCloses.get();

     // wait a bit in case any ending threads have anything to release
     int retries = 0;
     while (endNumOpens - numOpens != endNumCloses - numCloses) {
       if (retries++ > waitSeconds) {
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
       // if it's TestReplicationHandler, ignore it. the test is broken and gets no love
       if ("TestReplicationHandler".equals(RandomizedContext.current().getTargetClass().getSimpleName())) {
         log.warn("TestReplicationHandler wants to fail!: " + msg);
       } else {
         if (failTest) fail(msg);
       }
     }
  }
  
  /** Causes an exception matching the regex pattern to not be logged. */
  public static void ignoreException(String pattern) {
    if (SolrException.ignorePatterns == null)
      SolrException.ignorePatterns = new HashSet<>();
    SolrException.ignorePatterns.add(pattern);
  }

  public static void unIgnoreException(String pattern) {
    if (SolrException.ignorePatterns != null)
      SolrException.ignorePatterns.remove(pattern);
  }

  public static void resetExceptionIgnores() {
    SolrException.ignorePatterns = null;
    ignoreException("ignore_exception");  // always ignore "ignore_exception"    
  }

  protected static String getClassName() {
    return getTestClass().getName();
  }

  protected static String getSimpleClassName() {
    return getTestClass().getSimpleName();
  }

  protected static String configString;
  protected static String schemaString;
  protected static Path testSolrHome;

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
  public static String getSchemaFile() {
    return schemaString;
  }

  /**
   * Subclasses must define this method to return the name of the
   * solrconfig.xml they wish to use.
   */
  public static String getSolrConfigFile() {
    return configString;
  }

  /**
   * The directory used to story the index managed by the TestHarness
   */
  protected static volatile File initCoreDataDir;
  
  // hack due to File dataDir
  protected static String hdfsDataDir;

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

  private static String factoryProp;


  public static void initCore() throws Exception {
    log.info("####initCore");

    ignoreException("ignore_exception");
    factoryProp = System.getProperty("solr.directoryFactory");
    if (factoryProp == null) {
      System.setProperty("solr.directoryFactory","solr.RAMDirectoryFactory");
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

  public static void createCore() {
    assertNotNull(testSolrHome);
    solrConfig = TestHarness.createConfig(testSolrHome, coreName, getSolrConfigFile());
    h = new TestHarness( coreName, hdfsDataDir == null ? initCoreDataDir.getAbsolutePath() : hdfsDataDir,
            solrConfig,
            getSchemaFile());
    lrf = h.getRequestFactory
            ("standard",0,20,CommonParams.VERSION,"2.2");
  }

  public static CoreContainer createCoreContainer(Path solrHome, String solrXML) {
    testSolrHome = requireNonNull(solrHome);
    h = new TestHarness(solrHome, solrXML);
    lrf = h.getRequestFactory("standard", 0, 20, CommonParams.VERSION, "2.2");
    return h.getCoreContainer();
  }

  public static CoreContainer createCoreContainer(NodeConfig config, CoresLocator locator) {
    testSolrHome = config.getSolrResourceLoader().getInstancePath();
    h = new TestHarness(config, locator);
    lrf = h.getRequestFactory("standard", 0, 20, CommonParams.VERSION, "2.2");
    return h.getCoreContainer();
  }

  public static CoreContainer createCoreContainer(String coreName, String dataDir, String solrConfig, String schema) {
    NodeConfig nodeConfig = TestHarness.buildTestNodeConfig(new SolrResourceLoader(TEST_PATH()));
    CoresLocator locator = new TestHarness.TestCoresLocator(coreName, dataDir, solrConfig, schema);
    CoreContainer cc = createCoreContainer(nodeConfig, locator);
    h.coreName = coreName;
    return cc;
  }

  public static CoreContainer createDefaultCoreContainer(Path solrHome) {
    testSolrHome = requireNonNull(solrHome);
    h = new TestHarness("collection1", initCoreDataDir.getAbsolutePath(), "solrconfig.xml", "schema.xml");
    lrf = h.getRequestFactory("standard", 0, 20, CommonParams.VERSION, "2.2");
    return h.getCoreContainer();
  }

  public static boolean hasInitException(String message) {
    for (Map.Entry<String, CoreContainer.CoreLoadFailure> entry : h.getCoreContainer().getCoreInitFailures().entrySet()) {
      if (entry.getValue().exception.getMessage().contains(message))
        return true;
    }
    return false;
  }

  public static boolean hasInitException(Class<? extends Exception> exceptionType) {
    for (Map.Entry<String, CoreContainer.CoreLoadFailure> entry : h.getCoreContainer().getCoreInitFailures().entrySet()) {
      if (exceptionType.isAssignableFrom(entry.getValue().exception.getClass()))
        return true;
    }
    return false;
  }

  /** Subclasses that override setUp can optionally call this method
   * to log the fact that their setUp process has ended.
   */
  public void postSetUp() {
    log.info("####POSTSETUP " + getTestName());
  }


  /** Subclasses that override tearDown can optionally call this method
   * to log the fact that the tearDown process has started.  This is necessary
   * since subclasses will want to call super.tearDown() at the *end* of their
   * tearDown method.
   */
  public void preTearDown() {
    log.info("####PRETEARDOWN " + getTestName());
  }

  /**
   * Shuts down the test harness, and makes the best attempt possible
   * to delete dataDir, unless the system property "solr.test.leavedatadir"
   * is set.
   */
  public static void deleteCore() {
    log.info("###deleteCore" );
    if (h != null) {
      // If the test case set up Zk, it should still have it as available,
      // otherwise the core close will just be unnecessarily delayed.
      CoreContainer cc = h.getCoreContainer();
      if (! cc.getCores().isEmpty() && cc.isZooKeeperAware()) {
        try {
          cc.getZkController().getZkClient().exists("/", false);
        } catch (KeeperException e) {
          log.error("Testing connectivity to ZK by checking for root path failed", e);
          fail("Trying to tear down a ZK aware core container with ZK not reachable");
        } catch (InterruptedException ignored) {}
      }

      h.close();
    }

    if (factoryProp == null) {
      System.clearProperty("solr.directoryFactory");
    }
    
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
   * Validates a query matches some JSON test expressions using the default double delta tolerance.
   * @see JSONTestUtil#DEFAULT_DELTA
   * @see #assertJQ(SolrQueryRequest,double,String...)
   * @return The request response as a JSON String if all test patterns pass
   */
  public static String assertJQ(SolrQueryRequest req, String... tests) throws Exception {
    return assertJQ(req, JSONTestUtil.DEFAULT_DELTA, tests);
  }
  /**
   * Validates a query matches some JSON test expressions and closes the
   * query. The text expression is of the form path:JSON.  The Noggit JSON
   * parser used accepts single quoted strings and bare strings to allow
   * easy embedding in Java Strings.
   * <p>
   * Please use this with care: this makes it easy to match complete
   * structures, but doing so can result in fragile tests if you are
   * matching more than what you want to test.
   * </p>
   * @param req Solr request to execute
   * @param delta tolerance allowed in comparing float/double values
   * @param tests JSON path expression + '==' + expected value
   * @return The request response as a JSON String if all test patterns pass
   */
  public static String assertJQ(SolrQueryRequest req, double delta, String... tests) throws Exception {
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
        String testJSON = json(test);

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
      return response;
    } finally {
      // restore the params
      if (params != null && params != req.getParams()) req.setParams(params);
    }
  }  


  /** Makes sure a query throws a SolrException with the listed response code */
  public static void assertQEx(String message, SolrQueryRequest req, int code ) {
    try {
      ignoreException(".");
      h.query(req);
      fail( message );
    } catch (SolrException sex) {
      assertEquals( code, sex.code() );
    } catch (Exception e2) {
      throw new RuntimeException("Exception during query", e2);
    } finally {
      unIgnoreException(".");
    }
  }

  public static void assertQEx(String message, SolrQueryRequest req, SolrException.ErrorCode code ) {
    try {
      ignoreException(".");
      h.query(req);
      fail( message );
    } catch (SolrException e) {
      assertEquals( code.code, e.code() );
    } catch (Exception e2) {
      throw new RuntimeException("Exception during query", e2);
    } finally {
      unIgnoreException(".");
    }
  }
  /**
   * Makes sure a query throws a SolrException with the listed response code and expected message
   * @param failMessage The assert message to show when the query doesn't throw the expected exception
   * @param exceptionMessage A substring of the message expected in the exception
   * @param req Solr request
   * @param code expected error code for the query
   */
  public static void assertQEx(String failMessage, String exceptionMessage, SolrQueryRequest req, SolrException.ErrorCode code ) {
    try {
      ignoreException(".");
      h.query(req);
      fail( failMessage );
    } catch (SolrException e) {
      assertEquals( code.code, e.code() );
      assertTrue("Unexpected error message. Expecting \"" + exceptionMessage + 
          "\" but got \"" + e.getMessage() + "\"", e.getMessage()!= null && e.getMessage().contains(exceptionMessage));
    } catch (Exception e2) {
      throw new RuntimeException("Exception during query", e2);
    } finally {
      unIgnoreException(".");
    }
  }


  /**
   * @see TestHarness#optimize
   */
  public static String optimize(String... args) {
    return TestHarness.optimize(args);
  }
  /**
   * @see TestHarness#commit
   */
  public static String commit(String... args) {
    return TestHarness.commit(args);
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
    StringWriter out = new StringWriter(512);
    try {
      out.append("<add>");
      ClientUtils.writeXML(sdoc, out);
      out.append("</add>");
    } catch (IOException e) {
      throw new RuntimeException("Inexplicable IO error from StringWriter", e);
    }
    return out.toString();
  }

  public static void addDoc(String doc, String updateRequestProcessorChain) throws Exception {
    Map<String, String[]> params = new HashMap<>();
    MultiMapSolrParams mmparams = new MultiMapSolrParams(params);
    params.put(UpdateParams.UPDATE_CHAIN, new String[]{updateRequestProcessorChain});
    SolrQueryRequestBase req = new SolrQueryRequestBase(h.getCore(),
        (SolrParams) mmparams) {
    };

    UpdateRequestHandler handler = new UpdateRequestHandler();
    handler.init(null);
    ArrayList<ContentStream> streams = new ArrayList<>(2);
    streams.add(new ContentStreamBase.StringStream(doc));
    req.setContentStreams(streams);
    handler.handleRequestBody(req, new SolrQueryResponse());
    req.close();
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

      // this is annoying
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
    return TestHarness.deleteById(id);
  }
  /**
   * Generates a &lt;delete&gt;... XML string for an query
   *
   * @see TestHarness#deleteByQuery
   */
  public static String delQ(String q) {
    return TestHarness.deleteByQuery(q);
  }

  /**
   * Generates a simple &lt;doc&gt;... XML String with no options
   *
   * @param fieldsAndValues 0th and Even numbered args are fields names, Odds are field values.
   * @see TestHarness#makeSimpleDoc
   */
  public static XmlDoc doc(String... fieldsAndValues) {
    XmlDoc d = new XmlDoc();
    d.xml = TestHarness.makeSimpleDoc(fieldsAndValues);
    return d;
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  public static Map map(Object... params) {
    LinkedHashMap ret = new LinkedHashMap();
    for (int i=0; i<params.length; i+=2) {
      Object o = ret.put(params[i], params[i+1]);
      // TODO: handle multi-valued map?
    }
    return ret;
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

  /** Necessary to make method signatures un-ambiguous */
  public static class XmlDoc {
    public String xml;
    @Override
    public String toString() { return xml; }
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
      handler = new UpdateRequestHandler();
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

  public static List<SolrInputDocument> sdocs(SolrInputDocument... docs) {
    return Arrays.asList(docs);
  }

  /** Converts "test JSON" strings into JSON parseable by our JSON parser.
   *  For example, this method changed single quoted strings into double quoted strings before
   *  the parser could natively handle them.
   *
   * This transformation is automatically applied to JSON test srings (like assertJQ).
   */
  public static String json(String testJSON) {
    return testJSON;
  }


  /** Creates JSON from a SolrInputDocument.  Doesn't currently handle boosts.
   *  @see #json(SolrInputDocument,CharArr)
   */
  public static String json(SolrInputDocument doc) {
    CharArr out = new CharArr();
    json(doc, out);
    return out.toString();
  }

  /**
   * Appends to the <code>out</code> array with JSON from the <code>doc</code>.
   * Doesn't currently handle boosts, but does recursively handle child documents
   */
  public static void json(SolrInputDocument doc, CharArr out) {
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
          boolean firstVal = true;
          for (Object val : sfield) {
            if (firstVal) firstVal=false;
            else out.append(',');
            out.append(JSONUtil.toJSON(val));
          }
          out.append(']');
        } else {
          out.append(JSONUtil.toJSON(sfield.getValue()));
        }
      }

      boolean firstChildDoc = true;
      if(doc.hasChildDocuments()) {
        out.append(",\"_childDocuments_\": [");
        List<SolrInputDocument> childDocuments = doc.getChildDocuments();
        for(SolrInputDocument childDocument : childDocuments) {
          if (firstChildDoc) firstChildDoc=false;
          else out.append(',');
          json(childDocument, out);
        }
        out.append(']');
      }
      out.append('}');
    } catch (IOException e) {
      // should never happen
    }
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
    if (params==null || params.get("versions") == null) {
      ModifiableSolrParams mparams = new ModifiableSolrParams(params);
      mparams.set("versions","true");
      params = mparams;
    }
    String response = updateJ(jsonAdd(sdoc), params);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    List lst = (List)rsp.get("adds");
    if (lst == null || lst.size() == 0) return null;
    return (Long) lst.get(1);
  }

  public static Long deleteAndGetVersion(String id, SolrParams params) throws Exception {
    if (params==null || params.get("versions") == null) {
      ModifiableSolrParams mparams = new ModifiableSolrParams(params);
      mparams.set("versions","true");
      params = mparams;
    }
    String response = updateJ(jsonDelId(id), params);
    Map rsp = (Map)ObjectBuilder.fromJSON(response);
    List lst = (List)rsp.get("deletes");
    if (lst == null || lst.size() == 0) return null;
    return (Long) lst.get(1);
  }

  public static Long deleteByQueryAndGetVersion(String q, SolrParams params) throws Exception {
    if (params==null || params.get("versions") == null) {
      ModifiableSolrParams mparams = new ModifiableSolrParams(params);
      mparams.set("versions","true");
      params = mparams;
    }
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
      return min != max ? random().nextInt(max-min+1) + min : min;
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

  public static class IValsPercent extends IVals {
    final int[] percentAndValue;
    public IValsPercent(int... percentAndValue) {
      this.percentAndValue = percentAndValue;
    }

    @Override
    public int getInt() {
      int r = between(0,99);
      int cumulative = 0;
      for (int i=0; i<percentAndValue.length; i+=2) {
        cumulative += percentAndValue[i];
        if (r < cumulative) {
          return percentAndValue[i+1];
        }
      }

      return percentAndValue[percentAndValue.length-1];
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
      return min + random().nextFloat() *  (max - min);
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
      Map<String,Object> result = new HashMap<>();
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
    public IVals numValues;
    public Vals vals;

    public FldType(String fname, Vals vals) {
      this(fname, ZERO_ONE, vals);
    }

    public FldType(String fname, IVals numValues, Vals vals) {
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
      List<Comparable> vals = new ArrayList<>(nVals);
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
      model = new LinkedHashMap<>();
    }

    // commit an average of 10 times for large sets, or 10% of the time for small sets
    int commitOneOutOf = Math.max(nDocs/10, 10);

    for (int i=0; i<nDocs; i++) {
      Doc doc = createDoc(descriptor);
      // doc.order = order++;
      updateJ(toJSON(doc), null);
      model.put(doc.id, doc);

      // commit 10% of the time
      if (random().nextInt(commitOneOutOf)==0) {
        assertU(commit());
      }

      // duplicate 10% of the docs
      if (random().nextInt(10)==0) {
        updateJ(toJSON(doc), null);
        model.put(doc.id, doc);        
      }
    }

    // optimize 10% of the time
    if (random().nextInt(10)==0) {
      assertU(optimize());
    } else {
      if (random().nextInt(10) == 0) {
        assertU(commit());
      } else {
        assertU(commit("softCommit","true"));
      }
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
    doc.fields = new ArrayList<>();
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
    int nSorts = random().nextInt(4);
    List<Comparator<Doc>> comparators = new ArrayList<>();
    for (int i=0; i<nSorts; i++) {
      if (i>0) sortSpec.append(',');

      int which = random().nextInt(fieldTypes.size()+2);
      boolean asc = random().nextBoolean();
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
     return (o1, o2) -> (o1.order - o2.order) * mul;
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
    return (o1, o2) -> {
      int c = 0;
      for (Comparator<Doc> comparator : comparators) {
        c = comparator.compare(o1, o2);
        if (c!=0) return c;
      }
      return o1.order - o2.order;
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
  public Map<Comparable, List<Comparable>> invertField(Map<Comparable, Doc> model, String field) {
    Map<Comparable, List<Comparable>> value_to_id = new HashMap<>();

    // invert field
    for (Comparable key : model.keySet()) {
      Doc doc = model.get(key);
      List<Comparable> vals = doc.getValues(field);
      if (vals == null) continue;
      for (Comparable val : vals) {
        List<Comparable> ids = value_to_id.get(val);
        if (ids == null) {
          ids = new ArrayList<>(2);
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
    final URL url = Thread.currentThread().getContextClassLoader().getResource(name.replace(File.separatorChar, '/'));
    if (url != null) {
      try {
        return new File(url.toURI());
      } catch (Exception e) {
        throw new RuntimeException("Resource was found on classpath, but cannot be resolved to a " + 
            "normal file (maybe it is part of a JAR file): " + name);
      }
    }
    final File file = new File(name);
    if (file.exists()) {
      return file;
    }
    throw new RuntimeException("Cannot find resource in classpath or in file-system (relative to CWD): " + name);
  }
  
  public static String TEST_HOME() {
    return getFile("solr/collection1").getParent();
  }

  public static Path TEST_PATH() { return getFile("solr/collection1").getParentFile().toPath(); }

  public static Path configset(String name) {
    return TEST_PATH().resolve("configsets").resolve(name).resolve("conf");
  }

  public static Throwable getRootCause(Throwable t) {
    Throwable result = t;
    for (Throwable cause = t; null != cause; cause = cause.getCause()) {
      result = cause;
    }
    return result;
  }

  public static void assertXmlFile(final File file, String... xpath)
      throws IOException, SAXException {

    try {
      String xml = FileUtils.readFileToString(file, "UTF-8");
      String results = TestHarness.validateXPath(xml, xpath);
      if (null != results) {
        String msg = "File XPath failure: file=" + file.getPath() + " xpath="
            + results + "\n\nxml was: " + xml;
        fail(msg);
      }
    } catch (XPathExpressionException e2) {
      throw new RuntimeException("XPath is invalid", e2);
    }
  }
                                                         
  /**
   * Fails if the number of documents in the given SolrDocumentList differs
   * from the given number of expected values, or if any of the values in the
   * given field don't match the expected values in the same order.
   */
  public static void assertFieldValues(SolrDocumentList documents, String fieldName, Object... expectedValues) {
    if (documents.size() != expectedValues.length) {
      fail("Number of documents (" + documents.size()
          + ") is different from number of expected values (" + expectedValues.length);
    }
    for (int docNum = 1 ; docNum <= documents.size() ; ++docNum) {
      SolrDocument doc = documents.get(docNum - 1);
      Object expected = expectedValues[docNum - 1];
      Object actual = doc.get(fieldName);
      if ((null == expected && null != actual) ||
          (null != expected && null == actual) ||
          (null != expected && null != actual && !expected.equals(actual))) {
        fail("Unexpected " + fieldName + " field value in document #" + docNum
            + ": expected=[" + expected + "], actual=[" + actual + "]");
      }
    }
  }

  public static void copyMinConf(File dstRoot) throws IOException {
    copyMinConf(dstRoot, null);
  }

  // Creates a minimal conf dir, adding in a core.properties file from the string passed in
  // the string to write to the core.properties file may be null in which case nothing is done with it.
  // propertiesContent may be an empty string, which will actually work.
  public static void copyMinConf(File dstRoot, String propertiesContent) throws IOException {

    File subHome = new File(dstRoot, "conf");
    if (! dstRoot.exists()) {
      assertTrue("Failed to make subdirectory ", dstRoot.mkdirs());
    }
    Files.createFile(dstRoot.toPath().resolve("core.properties"));
    if (propertiesContent != null) {
      FileUtils.writeStringToFile(new File(dstRoot, "core.properties"), propertiesContent, StandardCharsets.UTF_8);
    }
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(subHome, "schema.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(subHome, "solrconfig.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));
  }

  // Creates minimal full setup, including the old solr.xml file that used to be hard coded in ConfigSolrXmlOld
  // TODO: remove for 5.0
  public static void copyMinFullSetup(File dstRoot) throws IOException {
    if (! dstRoot.exists()) {
      assertTrue("Failed to make subdirectory ", dstRoot.mkdirs());
    }
    File xmlF = new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml");
    FileUtils.copyFile(xmlF, new File(dstRoot, "solr.xml"));
    copyMinConf(dstRoot);
  }

  // Creates a consistent configuration, _including_ solr.xml at dstRoot. Creates collection1/conf and copies
  // the stock files in there.

  public static void copySolrHomeToTemp(File dstRoot, String collection) throws IOException {
    if (!dstRoot.exists()) {
      assertTrue("Failed to make subdirectory ", dstRoot.mkdirs());
    }

    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(dstRoot, "solr.xml"));

    File subHome = new File(dstRoot, collection + File.separator + "conf");
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "currency.xml"), new File(subHome, "currency.xml"));
    FileUtils.copyFile(new File(top, "mapping-ISOLatin1Accent.txt"), new File(subHome, "mapping-ISOLatin1Accent.txt"));
    FileUtils.copyFile(new File(top, "old_synonyms.txt"), new File(subHome, "old_synonyms.txt"));
    FileUtils.copyFile(new File(top, "open-exchange-rates.json"), new File(subHome, "open-exchange-rates.json"));
    FileUtils.copyFile(new File(top, "protwords.txt"), new File(subHome, "protwords.txt"));
    FileUtils.copyFile(new File(top, "schema.xml"), new File(subHome, "schema.xml"));
    FileUtils.copyFile(new File(top, "enumsConfig.xml"), new File(subHome, "enumsConfig.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.xml"), new File(subHome, "solrconfig.xml"));
    FileUtils.copyFile(new File(top, "stopwords.txt"), new File(subHome, "stopwords.txt"));
    FileUtils.copyFile(new File(top, "synonyms.txt"), new File(subHome, "synonyms.txt"));
  }

  public boolean compareSolrDocument(Object expected, Object actual) {

    if (!(expected instanceof SolrDocument)  || !(actual instanceof SolrDocument)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrDocument solrDocument1 = (SolrDocument) expected;
    SolrDocument solrDocument2 = (SolrDocument) actual;

    if(solrDocument1.getFieldNames().size() != solrDocument2.getFieldNames().size()) {
      return false;
    }

    Iterator<String> iter1 = solrDocument1.getFieldNames().iterator();
    Iterator<String> iter2 = solrDocument2.getFieldNames().iterator();

    if(iter1.hasNext()) {
      String key1 = iter1.next();
      String key2 = iter2.next();

      Object val1 = solrDocument1.getFieldValues(key1);
      Object val2 = solrDocument2.getFieldValues(key2);

      if(!key1.equals(key2) || !val1.equals(val2)) {
        return false;
      }
    }

    if(solrDocument1.getChildDocuments() == null && solrDocument2.getChildDocuments() == null) {
      return true;
    }
    if(solrDocument1.getChildDocuments() == null || solrDocument2.getChildDocuments() == null) {
      return false;
    } else if(solrDocument1.getChildDocuments().size() != solrDocument2.getChildDocuments().size()) {
      return false;
    } else {
      Iterator<SolrDocument> childDocsIter1 = solrDocument1.getChildDocuments().iterator();
      Iterator<SolrDocument> childDocsIter2 = solrDocument2.getChildDocuments().iterator();
      while(childDocsIter1.hasNext()) {
        if(!compareSolrDocument(childDocsIter1.next(), childDocsIter2.next())) {
          return false;
        }
      }
      return true;
    }
  }

  public boolean compareSolrDocumentList(Object expected, Object actual) {
    if (!(expected instanceof SolrDocumentList)  || !(actual instanceof SolrDocumentList)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrDocumentList list1 = (SolrDocumentList) expected;
    SolrDocumentList list2 = (SolrDocumentList) actual;

    if(Float.compare(list1.getMaxScore(), list2.getMaxScore()) != 0 || list1.getNumFound() != list2.getNumFound() ||
        list1.getStart() != list2.getStart()) {
      return false;
    }
    for(int i=0; i<list1.getNumFound(); i++) {
      if(!compareSolrDocument(list1.get(i), list2.get(i))) {
        return false;
      }
    }
    return true;
  }

  public boolean compareSolrInputDocument(Object expected, Object actual) {

    if (!(expected instanceof SolrInputDocument) || !(actual instanceof SolrInputDocument)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrInputDocument sdoc1 = (SolrInputDocument) expected;
    SolrInputDocument sdoc2 = (SolrInputDocument) actual;
    if (Float.compare(sdoc1.getDocumentBoost(), sdoc2.getDocumentBoost()) != 0) {
      return false;
    }

    if(sdoc1.getFieldNames().size() != sdoc2.getFieldNames().size()) {
      return false;
    }

    Iterator<String> iter1 = sdoc1.getFieldNames().iterator();
    Iterator<String> iter2 = sdoc2.getFieldNames().iterator();

    if(iter1.hasNext()) {
      String key1 = iter1.next();
      String key2 = iter2.next();

      Object val1 = sdoc1.getFieldValues(key1);
      Object val2 = sdoc2.getFieldValues(key2);

      if(!key1.equals(key2) || !val1.equals(val2)) {
        return false;
      }
    }
    if(sdoc1.getChildDocuments() == null && sdoc2.getChildDocuments() == null) {
      return true;
    }
    if(sdoc1.getChildDocuments() == null || sdoc2.getChildDocuments() == null) {
      return false;
    } else if(sdoc1.getChildDocuments().size() != sdoc2.getChildDocuments().size()) {
      return false;
    } else {
      Iterator<SolrInputDocument> childDocsIter1 = sdoc1.getChildDocuments().iterator();
      Iterator<SolrInputDocument> childDocsIter2 = sdoc2.getChildDocuments().iterator();
      while(childDocsIter1.hasNext()) {
        if(!compareSolrInputDocument(childDocsIter1.next(), childDocsIter2.next())) {
          return false;
        }
      }
      return true;
    }
  }

  public boolean assertSolrInputFieldEquals(Object expected, Object actual) {
    if (!(expected instanceof SolrInputField) || !(actual instanceof  SolrInputField)) {
      return false;
    }

    if (expected == actual) {
      return true;
    }

    SolrInputField sif1 = (SolrInputField) expected;
    SolrInputField sif2 = (SolrInputField) actual;

    if (!sif1.getName().equals(sif2.getName())) {
      return false;
    }

    if (!sif1.getValue().equals(sif2.getValue())) {
      return false;
    }

    if (Float.compare(sif1.getBoost(), sif2.getBoost()) != 0) {
      return false;
    }

    return true;
  }

  /** 
   * Returns <code>likely</code> most (1/10) of the time, otherwise <code>unlikely</code>
   */
  public static Object skewed(Object likely, Object unlikely) {
    return (0 == TestUtil.nextInt(random(), 0, 9)) ? unlikely : likely;
  }
  
  public static class CloudSolrClientBuilder extends CloudSolrClient.Builder {

    private boolean configuredDUTflag = false;

    public CloudSolrClientBuilder() {
      super();
    }

    @Override
    public CloudSolrClient.Builder sendDirectUpdatesToShardLeadersOnly() {
      configuredDUTflag = true;
      return super.sendDirectUpdatesToShardLeadersOnly();
    }

    @Override
    public CloudSolrClient.Builder sendDirectUpdatesToAnyShardReplica() {
      configuredDUTflag = true;
      return super.sendDirectUpdatesToAnyShardReplica();
    }

    private void randomlyChooseDirectUpdatesToLeadersOnly() {
      if (random().nextBoolean()) {
        sendDirectUpdatesToShardLeadersOnly();
      } else {
        sendDirectUpdatesToAnyShardReplica();
      }
    }

    @Override
    public CloudSolrClient build() {
      if (configuredDUTflag == false) {
        // flag value not explicity configured
        if (random().nextBoolean()) {
          // so randomly choose a value
          randomlyChooseDirectUpdatesToLeadersOnly();
        } else {
          // or go with whatever the default value is
          configuredDUTflag = true;
        }
      }
      return super.build();
    }
  }

  public static CloudSolrClient getCloudSolrClient(String zkHost) {
    if (random().nextBoolean()) {
      return new CloudSolrClient(zkHost);
    }
    return new CloudSolrClientBuilder()
        .withZkHost(zkHost)
        .build();
  }
  
  public static CloudSolrClient getCloudSolrClient(String zkHost, HttpClient httpClient) {
    if (random().nextBoolean()) {
      return new CloudSolrClient(zkHost, httpClient);
    }
    return new CloudSolrClientBuilder()
        .withZkHost(zkHost)
        .withHttpClient(httpClient)
        .build();
  }
  
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly) {
    if (random().nextBoolean()) {
      return new CloudSolrClient(zkHost, shardLeadersOnly);
    }
    
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder()
          .withZkHost(zkHost)
          .sendUpdatesOnlyToShardLeaders()
          .build();
    }
    return new CloudSolrClientBuilder()
        .withZkHost(zkHost)
        .sendUpdatesToAllReplicasInShard()
        .build();
  }
  
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly, HttpClient httpClient) {
    if (random().nextBoolean()) {
      return new CloudSolrClient(zkHost, shardLeadersOnly, httpClient);
    }
    
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder()
          .withZkHost(zkHost)
          .withHttpClient(httpClient)
          .sendUpdatesOnlyToShardLeaders()
          .build();
    }
    return new CloudSolrClientBuilder()
        .withZkHost(zkHost)
        .withHttpClient(httpClient)
        .sendUpdatesToAllReplicasInShard()
        .build();
  }
  
  public static ConcurrentUpdateSolrClient getConcurrentUpdateSolrClient(String baseSolrUrl, int queueSize, int threadCount) {
    if (random().nextBoolean()) {
      return new ConcurrentUpdateSolrClient(baseSolrUrl, queueSize, threadCount);
    }
    return new ConcurrentUpdateSolrClient.Builder(baseSolrUrl)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .build();
  }
  
  public static ConcurrentUpdateSolrClient getConcurrentUpdateSolrClient(String baseSolrUrl, HttpClient httpClient, int queueSize, int threadCount) {
    if (random().nextBoolean()) {
      return new ConcurrentUpdateSolrClient(baseSolrUrl, httpClient, queueSize, threadCount);
    }
    return new ConcurrentUpdateSolrClient.Builder(baseSolrUrl)
        .withHttpClient(httpClient)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .build();
  }
  
  public static LBHttpSolrClient getLBHttpSolrClient(HttpClient client, String... solrUrls) {
    if (random().nextBoolean()) {
      return new LBHttpSolrClient(client, solrUrls);
    }
    
    return new LBHttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrls(solrUrls)
        .build();
  }
  
  public static LBHttpSolrClient getLBHttpSolrClient(String... solrUrls) throws MalformedURLException {
    if (random().nextBoolean()) {
      return new LBHttpSolrClient(solrUrls);
    }
    return new LBHttpSolrClient.Builder()
        .withBaseSolrUrls(solrUrls)
        .build();
  }
  
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient, ResponseParser responseParser, boolean compression) {
    if(random().nextBoolean()) {
      return new HttpSolrClient(url, httpClient, responseParser, compression);
    }
    return new Builder(url)
        .withHttpClient(httpClient)
        .withResponseParser(responseParser)
        .allowCompression(compression)
        .build();
  }
  
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient, ResponseParser responseParser) {
    if(random().nextBoolean()) {
      return new HttpSolrClient(url, httpClient, responseParser);
    }
    return new Builder(url)
        .withHttpClient(httpClient)
        .withResponseParser(responseParser)
        .build();
  }
  
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient) {
    if(random().nextBoolean()) {
      return new HttpSolrClient(url, httpClient);
    }
    return new Builder(url)
        .withHttpClient(httpClient)
        .build();
  }

  public static HttpSolrClient getHttpSolrClient(String url) {
    if(random().nextBoolean()) {
      return new HttpSolrClient(url);
    }
    return new Builder(url)
        .build();
  }

  /** 
   * Returns a randomly generated Date in the appropriate Solr external (input) format 
   * @see #randomSkewedDate
   */
  public static String randomDate() {
    return Instant.ofEpochMilli(random().nextLong()).toString();
  }

  /** 
   * Returns a Date such that all results from this method always have the same values for 
   * year+month+day+hour+minute but the seconds are randomized.  This can be helpful for 
   * indexing documents with random date values that are biased for a narrow window 
   * (one day) to test collisions/overlaps
   *
   * @see #randomDate
   */
  public static String randomSkewedDate() {
    return String.format(Locale.ROOT, "2010-10-31T10:31:%02d.000Z",
                         TestUtil.nextInt(random(), 0, 59));
  }

  /**
   * We want "realistic" unicode strings beyond simple ascii, but because our
   * updates use XML we need to ensure we don't get "special" code block.
   */
  public static String randomXmlUsableUnicodeString() {
    String result = TestUtil.randomRealisticUnicodeString(random());
    if (result.matches(".*\\p{InSpecials}.*")) {
      result = TestUtil.randomSimpleString(random());
    }
    return result;
  }

  protected void waitForWarming() throws InterruptedException {
    RefCounted<SolrIndexSearcher> registeredSearcher = h.getCore().getRegisteredSearcher();
    RefCounted<SolrIndexSearcher> newestSearcher = h.getCore().getNewestSearcher(false);
    ;
    while (registeredSearcher == null || registeredSearcher.get() != newestSearcher.get()) {
      if (registeredSearcher != null) {
        registeredSearcher.decref();
      }
      newestSearcher.decref();
      Thread.sleep(50);
      registeredSearcher = h.getCore().getRegisteredSearcher();
      newestSearcher = h.getCore().getNewestSearcher(false);
    }
    registeredSearcher.decref();
    newestSearcher.decref();
  }

  @BeforeClass
  public static void chooseMPForMP() throws Exception {
    if (random().nextBoolean()) {
      System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "true");
      System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "false");
    } else {
      System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "false");
      System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "true");
    }
  }

  @AfterClass
  public static void unchooseMPForMP() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY);
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY);
  }

  @Deprecated // remove with SOLR-8668
  protected static void systemSetPropertySolrTestsMergePolicy(String value) {
    System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY, value);
  }

  @Deprecated // remove with SOLR-8668
  protected static void systemClearPropertySolrTestsMergePolicy() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY);
  }

  protected static void systemSetPropertySolrTestsMergePolicyFactory(String value) {
    System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY, value);
  }

  protected static void systemClearPropertySolrTestsMergePolicyFactory() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY);
  }
}
