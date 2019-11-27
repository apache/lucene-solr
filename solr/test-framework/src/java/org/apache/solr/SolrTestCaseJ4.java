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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.TraceFormatting;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.logging.log4j.Level;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.cloud.IpTables;
import org.apache.solr.cloud.MiniSolrCloudCluster;
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
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
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
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributedZkUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.RandomizeSSL.SSLRandomizer;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.TestHarness;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
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
import static org.apache.solr.cloud.SolrZkServer.ZK_WHITELIST_PROPERTY;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * A junit4 Solr test harness that extends SolrTestCase and, by extension, LuceneTestCase.
 * To change which core is used when loading the schema and solrconfig.xml, simply
 * invoke the {@link #initCore(String, String, String, String)} method.
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class
})
@SuppressSysoutChecks(bugUrl = "Solr dumps tons of logs to console.")
@SuppressFileSystems("ExtrasFS") // might be ok, the failures with e.g. nightly runs might be "normal"
@RandomizeSSL()
@ThreadLeakLingering(linger = 10000)
public abstract class SolrTestCaseJ4 extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final List<String> DEFAULT_STACK_FILTERS = Arrays.asList(new String [] {
      "org.junit.",
      "junit.framework.",
      "sun.",
      "java.lang.reflect.",
      "com.carrotsearch.randomizedtesting.",
  });
  
  public static final String DEFAULT_TEST_COLLECTION_NAME = "collection1";
  public static final String DEFAULT_TEST_CORENAME = DEFAULT_TEST_COLLECTION_NAME;
  protected static final String CORE_PROPERTIES_FILENAME = "core.properties";

  public static final String SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY = "solr.tests.mergePolicyFactory";

  @Deprecated // For backwards compatibility only. Please do not use in new tests.
  public static final String SYSTEM_PROPERTY_SOLR_DISABLE_SHARDS_WHITELIST = "solr.disable.shardsWhitelist";

  protected static String coreName = DEFAULT_TEST_CORENAME;

  public static int DEFAULT_CONNECTION_TIMEOUT = 60000;  // default socket connection timeout in ms
  
  private static String initialRootLogLevel;
  
  protected volatile static ExecutorService testExecutor;

  protected void writeCoreProperties(Path coreDirectory, String corename) throws IOException {
    Properties props = new Properties();
    props.setProperty("name", corename);
    props.setProperty("configSet", "collection1");
    props.setProperty("config", "${solrconfig:solrconfig.xml}");
    props.setProperty("schema", "${schema:schema.xml}");

    writeCoreProperties(coreDirectory, props, this.getSaferTestName());
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
   * Annotation for test classes that want to disable PointFields.
   * PointFields will otherwise randomly used by some schemas.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressPointFields {
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
    initialRootLogLevel = StartupLoggingUtils.getLogLevelString();
    initClassLogLevels();
    resetExceptionIgnores();
    
    testExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
        15L, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new SolrjNamedThreadFactory("testExecutor"),
        true);

    // not strictly needed by this class at this point in the control lifecycle, but for
    // backcompat create it now in case any third party tests expect initCoreDataDir to be
    // non-null after calling setupTestCases()
    initAndGetDataDir();

    System.setProperty("solr.zkclienttimeout", "90000"); 
    
    System.setProperty("solr.httpclient.retries", "1");
    System.setProperty("solr.retries.on.forward", "1");
    System.setProperty("solr.retries.to.followers", "1");

    System.setProperty("solr.v2RealPath", "true");
    System.setProperty("zookeeper.forceSync", "no");
    System.setProperty("jetty.testMode", "true");
    System.setProperty("enable.update.log", usually() ? "true" : "false");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    System.setProperty("solr.clustering.enabled", "false");
    System.setProperty("solr.peerSync.useRangeVersions", String.valueOf(random().nextBoolean()));
    System.setProperty("solr.cloud.wait-for-updates-with-stale-state-pause", "500");
    System.setProperty(ZK_WHITELIST_PROPERTY, "*");
    startTrackingSearchers();
    ignoreException("ignore_exception");
    newRandomConfig();

    sslConfig = buildSSLConfig();
    // based on randomized SSL config, set SchemaRegistryProvider appropriately
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    if(isSSLMode()) {
      // SolrCloud tests should usually clear this
      System.setProperty("urlScheme", "https");
    }
  }

  @AfterClass
  public static void teardownTestCases() throws Exception {
    TestInjection.notifyPauseForeverDone();
    try {
      try {
        deleteCore();
      } catch (Exception e) {
        log.error("Error deleting SolrCore.");
      }
      
      if (null != testExecutor) {
        ExecutorUtil.shutdownAndAwaitTermination(testExecutor);
      }

      resetExceptionIgnores();

      if (suiteFailureMarker.wasSuccessful()) {
        // if the tests passed, make sure everything was closed / released
        String orr = clearObjectTrackerAndCheckEmpty(60, false);
        assertNull(orr, orr);
      } else {
        ObjectReleaseTracker.tryClose();
      }
      resetFactory();
      coreName = DEFAULT_TEST_CORENAME;
    } finally {
      ObjectReleaseTracker.clear();
      TestInjection.reset();
      initCoreDataDir = null;
      System.clearProperty("solr.v2RealPath");
      System.clearProperty("zookeeper.forceSync");
      System.clearProperty("jetty.testMode");
      System.clearProperty("tests.shardhandler.randomSeed");
      System.clearProperty("enable.update.log");
      System.clearProperty("useCompoundFile");
      System.clearProperty("urlScheme");
      System.clearProperty("solr.peerSync.useRangeVersions");
      System.clearProperty("solr.cloud.wait-for-updates-with-stale-state-pause");
      System.clearProperty("solr.zkclienttmeout");
      System.clearProperty(ZK_WHITELIST_PROPERTY);
      HttpClientUtil.resetHttpClientBuilder();
      Http2SolrClient.resetSslContextFactory();

      clearNumericTypesProperties();

      // clean up static
      sslConfig = null;
      testSolrHome = null;

      IpTables.unblockAllPorts();

      LogLevel.Configurer.restoreLogLevels(savedClassLogLevels);
      savedClassLogLevels.clear();
      StartupLoggingUtils.changeLogLevel(initialRootLogLevel);
    }
  }
  
  /** Assumes that Mockito/Bytebuddy is available and can be used to mock classes (e.g., fails if Java version is too new). */
  public static void assumeWorkingMockito() {
    // we use reflection here, because we do not have ByteBuddy/Mockito in all modules and the test framework!
    try {
      Class.forName("net.bytebuddy.ClassFileVersion").getMethod("ofThisVm").invoke(null);
    } catch (InvocationTargetException e) {
      RandomizedTest.assumeNoException("SOLR-11606: ByteBuddy used by Mockito is not working with this JVM version.",
          e.getTargetException());
    } catch (ReflectiveOperationException e) {
      fail("ByteBuddy and Mockito are not available on classpath: " + e.toString());
    }
  }
  
  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty(int waitSeconds) {
    return clearObjectTrackerAndCheckEmpty(waitSeconds, false);
  }
  
  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty(int waitSeconds, boolean tryClose) {
    int retries = 0;
    String result;
    do {
      result = ObjectReleaseTracker.checkEmpty();
      if (result == null)
        break;
      try {
        if (retries % 10 == 0) {
          log.info("Waiting for all tracked resources to be released");
          if (retries > 10) {
            TraceFormatting tf = new TraceFormatting(DEFAULT_STACK_FILTERS);
            Map<Thread,StackTraceElement[]> stacksMap = Thread.getAllStackTraces();
            Set<Entry<Thread,StackTraceElement[]>> entries = stacksMap.entrySet();
            for (Entry<Thread,StackTraceElement[]> entry : entries) {
              String stack = tf.formatStackTrace(entry.getValue());
              System.err.println(entry.getKey().getName() + ":\n" + stack);
            }
          }
        }
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) { break; }
    }
    while (retries++ < waitSeconds);
    
    
    log.info("------------------------------------------------------- Done waiting for tracked resources to be released");
    
    ObjectReleaseTracker.clear();
    
    return result;
  }

  @SuppressForbidden(reason = "Using the Level class from log4j2 directly")
  private static Map<String, Level> savedClassLogLevels = new HashMap<>();

  public static void initClassLogLevels() {
    Class currentClass = RandomizedContext.current().getTargetClass();
    LogLevel annotation = (LogLevel) currentClass.getAnnotation(LogLevel.class);
    if (annotation == null) {
      return;
    }
    Map<String, Level> previousLevels = LogLevel.Configurer.setLevels(annotation.value());
    savedClassLogLevels.putAll(previousLevels);
  }

  private Map<String, Level> savedMethodLogLevels = new HashMap<>();

  @Before
  public void initMethodLogLevels() {
    Method method = RandomizedContext.current().getTargetMethod();
    LogLevel annotation = method.getAnnotation(LogLevel.class);
    if (annotation == null) {
      return;
    }
    Map<String, Level> previousLevels = LogLevel.Configurer.setLevels(annotation.value());
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
    return JettyConfig.builder().setContext(context).withSSLConfig(sslConfig.buildServerSSLConfig()).build();
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
    lrf = h.getRequestFactory("/select", 0, 20, CommonParams.VERSION, "2.2");
  }
  
  /** 
   * Sets system properties to allow generation of random configurations of
   * solrconfig.xml and schema.xml. 
   * Sets properties used on  
   * {@link #newIndexWriterConfig(org.apache.lucene.analysis.Analyzer)}
   *  and base schema.xml (Point Fields)
   */
  public static void newRandomConfig() {
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));

    System.setProperty("useCompoundFile", String.valueOf(iwc.getUseCompoundFile()));

    System.setProperty("solr.tests.maxBufferedDocs", String.valueOf(iwc.getMaxBufferedDocs()));
    System.setProperty("solr.tests.ramPerThreadHardLimitMB", String.valueOf(iwc.getRAMPerThreadHardLimitMB()));
    System.setProperty("solr.tests.ramBufferSizeMB", String.valueOf(iwc.getRAMBufferSizeMB()));

    String mergeSchedulerClass = iwc.getMergeScheduler().getClass().getName();
    if (mergeSchedulerClass.contains("$")) {
      // anonymous subclass - we can't instantiate via the resource loader, so use CMS instead
      mergeSchedulerClass = "org.apache.lucene.index.ConcurrentMergeScheduler";
    }
    System.setProperty("solr.tests.mergeScheduler", mergeSchedulerClass);
    randomizeNumericTypesProperties();
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

  /**
   * Subclasses may call this method to access the "dataDir" that will be used by 
   * {@link #initCore} (either prior to or after the core is created).
   * <p>
   * If the dataDir has not yet been initialized when this method is called, this method will do so.
   * Calling {@link #deleteCore} will "reset" the value, such that subsequent calls will 
   * re-initialize a new value.  All directories returned by any calls to this method will 
   * automatically be cleaned up per {@link #createTempDir}
   * </p>
   * <p>
   * NOTE: calling this method is not requried, it will be implicitly called as needed when
   * initializing cores.  Callers that don't care about using {@link #initCore} and just want
   * a temporary directory to put data in sould instead be using {@link #createTempDir} directly.
   * </p>
   *
   * @see #initCoreDataDir
   */
  protected static File initAndGetDataDir() {
    File dataDir = initCoreDataDir;
    if (null == dataDir) {
      final int id = dataDirCount.incrementAndGet();
      dataDir = initCoreDataDir = createTempDir("data-dir-"+ id).toFile();
      assertNotNull(dataDir);
      log.info("Created dataDir: {}", dataDir.getAbsolutePath());
    }
    return dataDir;
  }
  /** 
   * Counter for ensuring we don't ask {@link #createTempDir} to try and 
   * re-create the same dir prefix over and over.
   * <p>
   * (createTempDir has it's own counter for uniqueness, but it tries all numbers in a loop 
   * until it finds one available.  No reason to force that O(N^2) behavior when we know we've 
   * already created N previous directories with the same prefix.)
   * </p>
   */
  private static final AtomicInteger dataDirCount = new AtomicInteger(0);
  
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
  
  /** Causes an exception matching the regex pattern to not be logged. */
  public static void ignoreException(String pattern) {
    if (SolrException.ignorePatterns == null) // usually initialized already but in case not...
      resetExceptionIgnores();
    SolrException.ignorePatterns.add(pattern);
  }

  public static void unIgnoreException(String pattern) {
    if (SolrException.ignorePatterns != null)
      SolrException.ignorePatterns.remove(pattern);
  }

  /**
   * Clears all exception patterns, although keeps {@code "ignore_exception"}.
   * {@link SolrTestCaseJ4} calls this in {@link AfterClass} so usually tests don't need to call this.
   */
  public static void resetExceptionIgnores() {
    // CopyOnWrite for safety; see SOLR-11757
    SolrException.ignorePatterns = new CopyOnWriteArraySet<>(Collections.singleton("ignore_exception"));
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
   * Harness initialized by create[Default]Core[Container].
   *
   * <p>
   * For use in test methods as needed.
   * </p>
   */
  protected static TestHarness h;

  /**
   * LocalRequestFactory initialized by create[Default]Core[Container] using sensible
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
   * The directory used as the <code>dataDir</code> for the TestHarness unless 
   * {@link #hdfsDataDir} is non null.  
   * <p>
   * Will be set to null by {@link #deleteCore} and re-initialized as needed by {@link #createCore}.  
   * In the event of a test failure, the contents will be left on disk.
   * </p>
   * @see #createTempDir(String)
   * @see #initAndGetDataDir()
   * @deprecated use initAndGetDataDir instead of directly accessing this variable
   */
  @Deprecated
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
    h = new TestHarness( coreName, hdfsDataDir == null ? initAndGetDataDir().getAbsolutePath() : hdfsDataDir,
            solrConfig,
            getSchemaFile());
    lrf = h.getRequestFactory
            ("",0,20,CommonParams.VERSION,"2.2");
  }

  public static CoreContainer createCoreContainer(Path solrHome, String solrXML) {
    testSolrHome = requireNonNull(solrHome);
    h = new TestHarness(solrHome, solrXML);
    lrf = h.getRequestFactory("", 0, 20, CommonParams.VERSION, "2.2");
    return h.getCoreContainer();
  }

  public static CoreContainer createCoreContainer(NodeConfig config, CoresLocator locator) {
    testSolrHome = config.getSolrResourceLoader().getInstancePath();
    h = new TestHarness(config, locator);
    lrf = h.getRequestFactory("", 0, 20, CommonParams.VERSION, "2.2");
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
    h = new TestHarness("collection1", initAndGetDataDir().getAbsolutePath(), "solrconfig.xml", "schema.xml");
    lrf = h.getRequestFactory("", 0, 20, CommonParams.VERSION, "2.2");
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
   * Shuts down the test harness and nulls out the values setup by {@link #initCore}
   */
  public static void deleteCore() {
    if (h != null) {
      log.info("###deleteCore" );
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

    if (System.getProperty(UPDATELOG_SYSPROP) != null) {
      // clears the updatelog sysprop at the end of the test run
      System.clearProperty(UPDATELOG_SYSPROP);
    }
    
    solrConfig = null;
    h = null;
    lrf = null;
    configString = schemaString = null;
    initCoreDataDir = null;
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
      String m = (null == message) ? "" : message + " "; // TODO log 'm' !!!
      //since the default (standard) response format is now JSON
      //need to explicitly request XML since this class uses XPath
      ModifiableSolrParams xmlWriterTypeParams = new ModifiableSolrParams(req.getParams());
      xmlWriterTypeParams.set(CommonParams.WT,"xml");
      //for tests, let's turn indention off so we don't have to handle extraneous spaces
      xmlWriterTypeParams.set("indent", xmlWriterTypeParams.get("indent", "off"));
      req.setParams(xmlWriterTypeParams);
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

  /**
   * Generates the correct SolrParams from an even list of strings.
   * A string in an even position will represent the name of a parameter, while the following string
   * at position (i+1) will be the assigned value.
   *
   * @param params an even list of strings
   * @return the ModifiableSolrParams generated from the given list of strings.
   */
  public static ModifiableSolrParams params(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
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

  /**
   * Does a low level delete of all docs in the index. 
   *
   * The behavior of this method is slightly different then doing a normal <code>*:*</code> DBQ because it
   * takes advantage of internal methods to ensure all index data is wiped, regardless of optimistic 
   * concurrency version constraints -- making it suitable for tests that create synthetic versions, 
   * and/or require a completely pristine index w/o any field metdata.
   *
   * @see #deleteByQueryAndGetVersion
   */
  public void clearIndex() {
    if (null == h) {
      // harness not initialized, treat as No-Op so safe to call in cleanup methods
      // even if no tests run
      return;
    }
    try {
      deleteByQueryAndGetVersion("*:*", params("_version_", Long.toString(-Long.MAX_VALUE),
                                               DISTRIB_UPDATE_PARAM,DistribPhase.FROMLEADER.toString()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  public SolrInputDocument sdocWithChildren(String id, String version) {
    return sdocWithChildren(id, version, 2);
  }

  public SolrInputDocument sdocWithChildren(String id, String version, int childCount) {
    SolrInputDocument doc = sdoc("id", id, "_version_", version);
    for (int i = 1; i <= childCount; i++) {
      doc.addChildDocument(sdoc("id", id + "_child" + i));
    }
    return doc;
  }
  public SolrInputDocument sdocWithChildren(Integer id, String version, int childCount) {
    SolrInputDocument doc = sdoc("id", id, "_version_", version);
    for (int i = 1; i <= childCount; i++) {
      doc.addChildDocument(sdoc("id", (1000)*id + i));
    }
    return doc;
  }

  public static List<SolrInputDocument> sdocs(SolrInputDocument... docs) {
    return Arrays.asList(docs);
  }

  /** Converts "test JSON" strings into JSON parseable by our JSON parser.
   *  For example, this method changed single quoted strings into double quoted strings before
   *  the parser could natively handle them.
   *
   * This transformation is automatically applied to JSON test strings (like assertJQ).
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
            if(val instanceof SolrInputDocument) {
              json((SolrInputDocument) val, out);
            }
            out.append(JSONUtil.toJSON(val));
          }
          out.append(']');
        } else if(sfield.getValue() instanceof SolrInputDocument) {
          json((SolrInputDocument) sfield.getValue(), out);
        } else if (sfield.getValue() instanceof Map) {
          Map<String, Object> valMap = (Map<String, Object>) sfield.getValue();
          Set<String> childDocsKeys = valMap.entrySet().stream().filter(record -> isChildDoc(record.getValue()))
              .map(Entry::getKey).collect(Collectors.toSet());
          if(childDocsKeys.size() > 0) {
            Map<String, Object> newMap = new HashMap<>();
            for(Entry<String, Object> entry: valMap.entrySet()) {
              String keyName = entry.getKey();
              Object val = entry.getValue();
              if(childDocsKeys.contains(keyName)) {
                if(val instanceof Collection) {
                  val = ((Collection) val).stream().map(e -> toSolrDoc((SolrInputDocument) e)).collect(Collectors.toList());
                } else {
                  val = toSolrDoc((SolrInputDocument) val);
                }
              }
              newMap.put(keyName, val);
            }
            valMap = newMap;
          }
          out.append(JSONUtil.toJSON(valMap));
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

  public static class BVal extends Vals {

    @Override
    public Comparable get() {
      return random().nextBoolean();
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

  protected static class FldType {
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

  public static void assertResponseValues(SolrResponseBase rsp, Object... assertions) {
    Map<String, Object> values = Utils.makeMap(assertions);
    values.forEach((s, o) -> {
      if (o instanceof String) {
        assertEquals(o, rsp.getResponse()._getStr(s, null));
      } else {
        assertEquals(o, rsp.getResponse()._get(s, null));
      }
    });
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
    final URL url = SolrTestCaseJ4.class.getClassLoader().getResource(name.replace(File.separatorChar, '/'));
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
    copyMinConf(dstRoot, propertiesContent, "solrconfig-minimal.xml");
  }

  public static void copyMinConf(File dstRoot, String propertiesContent, String solrconfigXmlName) throws IOException {

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
    FileUtils.copyFile(new File(top, solrconfigXmlName), new File(subHome, "solrconfig.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(subHome, "solrconfig.snippet.randomindexconfig.xml"));
  }

  // Creates minimal full setup, including solr.xml
  public static void copyMinFullSetup(File dstRoot) throws IOException {
    if (! dstRoot.exists()) {
      assertTrue("Failed to make subdirectory ", dstRoot.mkdirs());
    }
    File xmlF = new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml");
    FileUtils.copyFile(xmlF, new File(dstRoot, "solr.xml"));
    copyMinConf(dstRoot);
  }

  // Just copies the file indicated to the tmp home directory naming it "solr.xml"
  public static void copyXmlToHome(File dstRoot, String fromFile) throws IOException {
    if (! dstRoot.exists()) {
      assertTrue("Failed to make subdirectory ", dstRoot.mkdirs());
    }
    File xmlF = new File(SolrTestCaseJ4.TEST_HOME(), fromFile);
    FileUtils.copyFile(xmlF, new File(dstRoot, "solr.xml"));
    
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

    if (list1.getMaxScore() == null) {
      if (list2.getMaxScore() != null) {
        return false;
      } 
    } else if (list2.getMaxScore() == null) {
      return false;
    } else {
      if (Float.compare(list1.getMaxScore(), list2.getMaxScore()) != 0 || list1.getNumFound() != list2.getNumFound() ||
          list1.getStart() != list2.getStart()) {
        return false;
      }
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

    if(sdoc1.getFieldNames().size() != sdoc2.getFieldNames().size()) {
      return false;
    }

    Iterator<String> iter1 = sdoc1.getFieldNames().iterator();
    Iterator<String> iter2 = sdoc2.getFieldNames().iterator();

    while (iter1.hasNext()) {
      String key1 = iter1.next();
      String key2 = iter2.next();

      Object val1 = sdoc1.getFieldValues(key1);
      Object val2 = sdoc2.getFieldValues(key2);

      if(!key1.equals(key2)) {
        return false;
      }

      if(!(sdoc1.get(key1).getFirstValue() instanceof SolrInputDocument)) {
        if(!val1.equals(val2)) {
          return false;
        }
      } else {
        if (!(sdoc2.get(key2).getFirstValue() instanceof SolrInputDocument)) {
          return false;
        }
        Collection col1 = (Collection) val1;
        Collection col2 = (Collection) val2;
        if (col1.size() != col2.size()) {
          return false;
        }
        Iterator<SolrInputDocument> colIter1 = col1.iterator();
        Iterator<SolrInputDocument> colIter2 = col2.iterator();
        while (colIter1.hasNext()) {
          if (!compareSolrInputDocument(colIter1.next(), colIter2.next())) {
            return false;
          }
        }
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

    return true;
  }

  /** 
   * Returns <code>likely</code> most (1/10) of the time, otherwise <code>unlikely</code>
   */
  public static Object skewed(Object likely, Object unlikely) {
    return (0 == TestUtil.nextInt(random(), 0, 9)) ? unlikely : likely;
  }

  /**
   * A variant of {@link  org.apache.solr.client.solrj.impl.CloudHttp2SolrClient.Builder} that will randomize
   * some internal settings.
   */
  public static class CloudHttp2SolrClientBuilder extends CloudHttp2SolrClient.Builder {

    public CloudHttp2SolrClientBuilder(List<String> zkHosts, Optional<String> zkChroot) {
      super(zkHosts, zkChroot);
      randomizeCloudSolrClient();
    }

    public CloudHttp2SolrClientBuilder(ClusterStateProvider stateProvider) {
      super(new ArrayList<>());
      this.stateProvider = stateProvider;
      randomizeCloudSolrClient();
    }

    public CloudHttp2SolrClientBuilder(MiniSolrCloudCluster cluster) {
      super(new ArrayList<>());
      if (random().nextBoolean()) {
        this.zkHosts.add(cluster.getZkServer().getZkAddress());
      } else {
        populateSolrUrls(cluster);
      }

      randomizeCloudSolrClient();
    }

    private void populateSolrUrls(MiniSolrCloudCluster cluster) {
      if (random().nextBoolean()) {
        final List<JettySolrRunner> solrNodes = cluster.getJettySolrRunners();
        for (JettySolrRunner node : solrNodes) {
          this.solrUrls.add(node.getBaseUrl().toString());
        }
      } else {
        this.solrUrls.add(cluster.getRandomJetty(random()).getBaseUrl().toString());
      }
    }

    private void randomizeCloudSolrClient() {
      this.directUpdatesToLeadersOnly = random().nextBoolean();
      this.shardLeadersOnly = random().nextBoolean();
      this.parallelUpdates = random().nextBoolean();
    }
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the
   * {@link org.apache.solr.client.solrj.impl.CloudHttp2SolrClient.Builder} class directly
   */
  public static CloudHttp2SolrClient getCloudHttp2SolrClient(MiniSolrCloudCluster cluster) {
    return new CloudHttp2SolrClientBuilder(cluster).build();
  }

  /**
   * A variant of {@link  org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} that will randomize
   * some internal settings.
   */
  public static class CloudSolrClientBuilder extends CloudSolrClient.Builder {
    
    public CloudSolrClientBuilder(List<String> zkHosts, Optional<String> zkChroot) {
      super(zkHosts, zkChroot);
      randomizeCloudSolrClient();
    }
    
    public CloudSolrClientBuilder(ClusterStateProvider stateProvider) {
      this.stateProvider = stateProvider;
      randomizeCloudSolrClient();
    }
    
    public CloudSolrClientBuilder(MiniSolrCloudCluster cluster) {
      if (random().nextBoolean()) {
        this.zkHosts.add(cluster.getZkServer().getZkAddress());
      } else {
        populateSolrUrls(cluster);
      }
      
      randomizeCloudSolrClient();
    }
    
    private void populateSolrUrls(MiniSolrCloudCluster cluster) {
      if (random().nextBoolean()) {
        final List<JettySolrRunner> solrNodes = cluster.getJettySolrRunners();
        for (JettySolrRunner node : solrNodes) {
          this.solrUrls.add(node.getBaseUrl().toString());
        }
      } else {
        this.solrUrls.add(cluster.getRandomJetty(random()).getBaseUrl().toString());
      }
    }
    
    private void randomizeCloudSolrClient() {
      this.directUpdatesToLeadersOnly = random().nextBoolean();
      this.shardLeadersOnly = random().nextBoolean();
      this.parallelUpdates = random().nextBoolean();
    }
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost) {
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty()).build();
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */
  public static CloudSolrClient getCloudSolrClient(MiniSolrCloudCluster cluster) {
    return new CloudSolrClientBuilder(cluster).build();
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost, HttpClient httpClient) {
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
        .withHttpClient(httpClient)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly) {
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
          .sendUpdatesOnlyToShardLeaders()
          .build();
    }
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
        .sendUpdatesToAllReplicasInShard()
        .build();
  }

  public static CloudSolrClientBuilder newCloudSolrClient(String zkHost) {
    return (CloudSolrClientBuilder) new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty());
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly, int socketTimeoutMillis) {
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
          .sendUpdatesOnlyToShardLeaders()
          .withSocketTimeout(socketTimeoutMillis)
          .build();
    }
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
        .sendUpdatesToAllReplicasInShard()
        .withSocketTimeout(socketTimeoutMillis)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly, int connectionTimeoutMillis, int socketTimeoutMillis) {
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
          .sendUpdatesOnlyToShardLeaders()
          .withConnectionTimeout(connectionTimeoutMillis)
          .withSocketTimeout(socketTimeoutMillis)
          .build();
    }
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
        .sendUpdatesToAllReplicasInShard()
        .withConnectionTimeout(connectionTimeoutMillis)
        .withSocketTimeout(socketTimeoutMillis)
        .build();
  }
  
  
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly, HttpClient httpClient) {
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
          .withHttpClient(httpClient)
          .sendUpdatesOnlyToShardLeaders()
          .build();
    }
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
        .withHttpClient(httpClient)
        .sendUpdatesToAllReplicasInShard()
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient.Builder} class directly
   */ 
  public static CloudSolrClient getCloudSolrClient(String zkHost, boolean shardLeadersOnly, HttpClient httpClient,
      int connectionTimeoutMillis, int socketTimeoutMillis) {
    if (shardLeadersOnly) {
      return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
          .withHttpClient(httpClient)
          .sendUpdatesOnlyToShardLeaders()
          .withConnectionTimeout(connectionTimeoutMillis)
          .withSocketTimeout(socketTimeoutMillis)
          .build();
    }
    return new CloudSolrClientBuilder(Collections.singletonList(zkHost), Optional.empty())
        .withHttpClient(httpClient)
        .sendUpdatesToAllReplicasInShard()
        .withConnectionTimeout(connectionTimeoutMillis)
        .withSocketTimeout(socketTimeoutMillis)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient.Builder} class directly
   */ 
  public static ConcurrentUpdateSolrClient getConcurrentUpdateSolrClient(String baseSolrUrl, int queueSize, int threadCount) {
    return new ConcurrentUpdateSolrClient.Builder(baseSolrUrl)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient.Builder} class directly
   */ 
  public static ConcurrentUpdateSolrClient getConcurrentUpdateSolrClient(String baseSolrUrl, int queueSize, int threadCount, int connectionTimeoutMillis) {
    return new ConcurrentUpdateSolrClient.Builder(baseSolrUrl)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .withConnectionTimeout(connectionTimeoutMillis)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient.Builder} class directly
   */ 
  public static ConcurrentUpdateSolrClient getConcurrentUpdateSolrClient(String baseSolrUrl, HttpClient httpClient, int queueSize, int threadCount) {
    return new ConcurrentUpdateSolrClient.Builder(baseSolrUrl)
        .withHttpClient(httpClient)
        .withQueueSize(queueSize)
        .withThreadCount(threadCount)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.LBHttpSolrClient.Builder} class directly
   */ 
  public static LBHttpSolrClient getLBHttpSolrClient(HttpClient client, String... solrUrls) {
    return new LBHttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrls(solrUrls)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.LBHttpSolrClient.Builder} class directly
   */ 
  public static LBHttpSolrClient getLBHttpSolrClient(HttpClient client, int connectionTimeoutMillis,
      int socketTimeoutMillis, String... solrUrls) {
    return new LBHttpSolrClient.Builder()
        .withHttpClient(client)
        .withBaseSolrUrls(solrUrls)
        .withConnectionTimeout(connectionTimeoutMillis)
        .withSocketTimeout(socketTimeoutMillis)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.LBHttpSolrClient.Builder} class directly
   */ 
  public static LBHttpSolrClient getLBHttpSolrClient(String... solrUrls) throws MalformedURLException {
    return new LBHttpSolrClient.Builder()
        .withBaseSolrUrls(solrUrls)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient, ResponseParser responseParser, boolean compression) {
    return new Builder(url)
        .withHttpClient(httpClient)
        .withResponseParser(responseParser)
        .allowCompression(compression)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient, ResponseParser responseParser) {
    return new Builder(url)
        .withHttpClient(httpClient)
        .withResponseParser(responseParser)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient) {
    return new Builder(url)
        .withHttpClient(httpClient)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url, HttpClient httpClient, int connectionTimeoutMillis) {
    return new Builder(url)
        .withHttpClient(httpClient)
        .withConnectionTimeout(connectionTimeoutMillis)
        .build();
  }

  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url) {
    return new Builder(url)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url, int connectionTimeoutMillis) {
    return new Builder(url)
        .withConnectionTimeout(connectionTimeoutMillis)
        .build();
  }
  
  /**
   * This method <i>may</i> randomize unspecified aspects of the resulting SolrClient.
   * Tests that do not wish to have any randomized behavior should use the 
   * {@link org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} class directly
   */ 
  public static HttpSolrClient getHttpSolrClient(String url, int connectionTimeoutMillis, int socketTimeoutMillis) {
    return new Builder(url)
        .withConnectionTimeout(connectionTimeoutMillis)
        .withSocketTimeout(socketTimeoutMillis)
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
  
  protected static void waitForWarming(SolrCore core) throws InterruptedException {
    RefCounted<SolrIndexSearcher> registeredSearcher = core.getRegisteredSearcher();
    RefCounted<SolrIndexSearcher> newestSearcher = core.getNewestSearcher(false);
    while (registeredSearcher == null || registeredSearcher.get() != newestSearcher.get()) {
      if (registeredSearcher != null) {
        registeredSearcher.decref();
      }
      newestSearcher.decref();
      Thread.sleep(50);
      registeredSearcher = core.getRegisteredSearcher();
      newestSearcher = core.getNewestSearcher(false);
    }
    registeredSearcher.decref();
    newestSearcher.decref();
  }

  protected void waitForWarming() throws InterruptedException {
    waitForWarming(h.getCore());
  }

  protected String getSaferTestName() {
    // test names can hold additional info, like the test seed
    // only take to first space
    String testName = getTestName();
    int index = testName.indexOf(' ');
    if (index > 0) {
      testName = testName.substring(0, index);
    }
    return testName;
  }
  
  @BeforeClass
  public static void assertNonBlockingRandomGeneratorAvailable() throws InterruptedException {
    final String EGD = "java.security.egd";
    final String URANDOM = "file:/dev/./urandom";
    final String ALLOWED = "test.solr.allowed.securerandom";
    final String allowedAlg = System.getProperty(ALLOWED);
    final String actualEGD = System.getProperty(EGD);
    
    log.info("SecureRandom sanity checks: {}={} & {}={}", ALLOWED, allowedAlg, EGD, actualEGD);

    if (null != allowedAlg) {
      // the user has explicitly requested to bypass our assertions and allow a particular alg
      // the only thing we should do is assert that the algorithm they have whitelisted is actaully used
      
      
      final String actualAlg = (new SecureRandom()).getAlgorithm();
      assertEquals("Algorithm specified using "+ALLOWED+" system property " +
                   "does not match actual algorithm", allowedAlg, actualAlg);
      return;
    }
    // else: no user override, do the checks we want including 
    
    if (null == actualEGD) {
      System.setProperty(EGD, URANDOM);
      log.warn("System property {} was not set by test runner, forcibly set to expected: {}", EGD, URANDOM);
    } else if (! URANDOM.equals(actualEGD) ) {
      log.warn("System property {}={} .. test runner should use expected: {}", EGD, actualEGD, URANDOM);
    }
    
    final String algorithm = (new SecureRandom()).getAlgorithm();
    
    assertFalse("SecureRandom algorithm '" + algorithm + "' is in use by your JVM, " +
                "which is a potentially blocking algorithm on some environments. " +
                "Please report the details of this failure (and your JVM vendor/version) to solr-user@lucene.apache.org. " +
                "You can try to run your tests with -D"+EGD+"="+URANDOM+" or bypass this check using " +
                "-Dtest.solr.allowed.securerandom="+ algorithm +" as a JVM option when running tests.",
                // be permissive in our checks and blacklist only algorithms 
                // that are known to be blocking under some circumstances
                algorithm.equals("NativePRNG") || algorithm.equals("NativePRNGBlocking"));
  }

  protected static void systemSetPropertySolrTestsMergePolicyFactory(String value) {
    System.setProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY, value);
  }

  protected static void systemClearPropertySolrTestsMergePolicyFactory() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY);
  }
  
  @Deprecated // For backwards compatibility only. Please do not use in new tests.
  protected static void systemSetPropertySolrDisableShardsWhitelist(String value) {
    System.setProperty(SYSTEM_PROPERTY_SOLR_DISABLE_SHARDS_WHITELIST, value);
  }

  @Deprecated // For backwards compatibility only. Please do not use in new tests.
  protected static void systemClearPropertySolrDisableShardsWhitelist() {
    System.clearProperty(SYSTEM_PROPERTY_SOLR_DISABLE_SHARDS_WHITELIST);
  }

  protected <T> T pickRandom(T... options) {
    return options[random().nextInt(options.length)];
  }
  
  /**
   * The name of a sysprop that can be set by users when running tests to force the types of numerics 
   * used for test classes that do not have the {@link SuppressPointFields} annotation:
   * <ul>
   *  <li>If unset, then a random variable will be used to decide the type of numerics.</li>
   *  <li>If set to <code>true</code> then Points based numerics will be used.</li>
   *  <li>If set to <code>false</code> (or any other string) then Trie based numerics will be used.</li>
   * </ul>
   * @see #NUMERIC_POINTS_SYSPROP
   */
  public static final String USE_NUMERIC_POINTS_SYSPROP = "solr.tests.use.numeric.points";
  
  /**
   * The name of a sysprop that will either <code>true</code> or <code>false</code> indicating if 
   * numeric points fields are currently in use, depending on the user specified value of 
   * {@link #USE_NUMERIC_POINTS_SYSPROP} and/or the {@link SuppressPointFields} annotation and/or 
   * randomization. Tests can use <code>Boolean.getBoolean(NUMERIC_POINTS_SYSPROP)</code>.
   *
   * @see #randomizeNumericTypesProperties
   */
  public static final String NUMERIC_POINTS_SYSPROP = "solr.tests.numeric.points";
  
  /**
   * The name of a sysprop that will be either <code>true</code> or <code>false</code> indicating if 
   * docValues should be used on a numeric field.  This property string should be used in the 
   * <code>docValues</code> attribute of (most) numeric fieldTypes in schemas, and by default will be 
   * randomized by this class in a <code>@BeforeClass</code>.  Subclasses that need to force specific 
   * behavior can use <code>System.setProperty(NUMERIC_DOCVALUES_SYSPROP,"true")</code> 
   * to override the default randomization.
   *
   * @see #randomizeNumericTypesProperties
   */
  public static final String NUMERIC_DOCVALUES_SYSPROP = "solr.tests.numeric.dv";

  public static final String UPDATELOG_SYSPROP = "solr.tests.ulog";

  /**
   * randomizes the updateLog between different update log implementations for better test coverage
   */
  public static void randomizeUpdateLogImpl() {
    if (random().nextBoolean()) {
      System.setProperty(UPDATELOG_SYSPROP, "solr.CdcrUpdateLog");
    } else {
      System.setProperty(UPDATELOG_SYSPROP,"solr.UpdateLog");
    }
    log.info("updateLog impl={}", System.getProperty(UPDATELOG_SYSPROP));
  }

  /**
   * Sets various sys props related to user specified or randomized choices regarding the types 
   * of numerics that should be used in tests.
   *
   * @see #NUMERIC_DOCVALUES_SYSPROP
   * @see #NUMERIC_POINTS_SYSPROP
   * @see #clearNumericTypesProperties
   * @lucene.experimental
   * @lucene.internal
   */
  private static void randomizeNumericTypesProperties() {

    final boolean useDV = random().nextBoolean();
    System.setProperty(NUMERIC_DOCVALUES_SYSPROP, ""+useDV);
    
    // consume a consistent amount of random data even if sysprop/annotation is set
    final boolean randUsePoints = 0 != random().nextInt(5);  // 80% likelihood

    final String usePointsStr = System.getProperty(USE_NUMERIC_POINTS_SYSPROP);
    final boolean usePoints = (null == usePointsStr) ? randUsePoints : Boolean.parseBoolean(usePointsStr);
    
    if (RandomizedContext.current().getTargetClass().isAnnotationPresent(SolrTestCaseJ4.SuppressPointFields.class)
        || (! usePoints)) {
      log.info("Using TrieFields (NUMERIC_POINTS_SYSPROP=false) w/NUMERIC_DOCVALUES_SYSPROP="+useDV);
      
      org.apache.solr.schema.PointField.TEST_HACK_IGNORE_USELESS_TRIEFIELD_ARGS = false;
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Integer.class, "solr.TrieIntField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Float.class, "solr.TrieFloatField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Long.class, "solr.TrieLongField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Double.class, "solr.TrieDoubleField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Date.class, "solr.TrieDateField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Enum.class, "solr.EnumField");
      
      System.setProperty(NUMERIC_POINTS_SYSPROP, "false");
    } else {
      log.info("Using PointFields (NUMERIC_POINTS_SYSPROP=true) w/NUMERIC_DOCVALUES_SYSPROP="+useDV);

      org.apache.solr.schema.PointField.TEST_HACK_IGNORE_USELESS_TRIEFIELD_ARGS = true;
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Integer.class, "solr.IntPointField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Float.class, "solr.FloatPointField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Long.class, "solr.LongPointField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Double.class, "solr.DoublePointField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Date.class, "solr.DatePointField");
      private_RANDOMIZED_NUMERIC_FIELDTYPES.put(Enum.class, "solr.EnumFieldType");
      
      System.setProperty(NUMERIC_POINTS_SYSPROP, "true");
    }
    for (Map.Entry<Class,String> entry : RANDOMIZED_NUMERIC_FIELDTYPES.entrySet()) {
      System.setProperty("solr.tests." + entry.getKey().getSimpleName() + "FieldType",
                         entry.getValue());

    }
  }

  public static DistributedUpdateProcessor createDistributedUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp,
                                                                            UpdateRequestProcessor next) {
    if(h.getCoreContainer().isZooKeeperAware()) {
      return new DistributedZkUpdateProcessor(req, rsp, next);
    }
    return new DistributedUpdateProcessor(req, rsp, next);
  }
  
  /**
   * Cleans up the randomized sysproperties and variables set by {@link #randomizeNumericTypesProperties}
   *
   * @see #randomizeNumericTypesProperties
   * @lucene.experimental
   * @lucene.internal
   */
  private static void clearNumericTypesProperties() {
    org.apache.solr.schema.PointField.TEST_HACK_IGNORE_USELESS_TRIEFIELD_ARGS = false;
    System.clearProperty("solr.tests.numeric.points");
    System.clearProperty("solr.tests.numeric.points.dv");
    for (Class c : RANDOMIZED_NUMERIC_FIELDTYPES.keySet()) {
      System.clearProperty("solr.tests." + c.getSimpleName() + "FieldType");
    }
    private_RANDOMIZED_NUMERIC_FIELDTYPES.clear();
  }

  private static SolrDocument toSolrDoc(SolrInputDocument sid) {
    SolrDocument doc = new SolrDocument();
    for(SolrInputField field: sid) {
      doc.setField(field.getName(), field.getValue());
    }
    return doc;
  }

  private static boolean isChildDoc(Object o) {
    if(o instanceof Collection) {
      Collection col = (Collection) o;
      if(col.size() == 0) {
        return false;
      }
      return col.iterator().next() instanceof SolrInputDocument;
    }
    return o instanceof SolrInputDocument;
  }

  private static final Map<Class,String> private_RANDOMIZED_NUMERIC_FIELDTYPES = new HashMap<>();
  
  /**
   * A Map of "primitive" java "numeric" types and the string name of the <code>class</code> used in the
   * corresponding schema fieldType declaration.
   * <p>
   * Example: <code>java.util.Date =&gt; "solr.DatePointField"</code>
   * </p>
   *
   * @see #randomizeNumericTypesProperties
   */
  protected static final Map<Class,String> RANDOMIZED_NUMERIC_FIELDTYPES
    = Collections.unmodifiableMap(private_RANDOMIZED_NUMERIC_FIELDTYPES);


  /**
   * See SOLR-11035. There are various "impossible" failures, I can update some documents successfully then not find
   * them.
   * <p>
   * At least one I've seen (Erick Erickson) cannot be cured by waiting on the client side.
   * <p>
   * This is a horrible hack, but until we fix the underlying cause using it will reduce the noise from tests. Once the
   * root cause of SOLR-11035 is found, this should be removed.
   * <p>
   * I don't mind the extra commits (why do two?) as this should be pretty rare.
   *
   * This test fails 10% - 15% of the time without using this method, especially if you @Ignore all the other
   * tests in that suite.
   *
   * ant test  -Dtestcase=DocValuesNotIndexedTest -Dtests.method=testGroupingDVOnly
   * -Dtests.seed=54688F608E614440 -Dtests.slow=true -Dtests.locale=nl-BE
   * -Dtests.timezone=America/North_Dakota/Beulah -Dtests.asserts=true -Dtests.file.encoding=ISO-8859-1
   *
   * This only really works for adding documents. The test at the top of the method will succeed for an update of
   * an existing doc and nothing will be done. If that becomes necessary we should probably create a new method
   * that takes a docID, field and value.
   *
   * @param client - the client that we'll use to send the request
   * @param collection - the target collection we'll add and remove the doc from
   * @param idField - the uniqueKey for this collection. This MUST be a string
   * @param expectedDocCount - numFound for the query
   * @param query - The Solr query to check for expectedDocCount.
   * @param tag - additional information to display on a failure. Often class.method is useful.
   */

  public static void Solr11035BandAid(SolrClient client, String collection, String idField,
                                      long expectedDocCount, String query,
                                      String tag) throws IOException, SolrServerException {

    Solr11035BandAid(client, collection, idField, expectedDocCount, query, tag, false);
  }

  // Pass true for failAnyway to have this bandaid fail if
  // 1> it had to attempt the repair
  // 2> it would have successfully repaired it
  //
  // This is useful for verifying that SOLR-11035.
  //
  public static void Solr11035BandAid(SolrClient client, String collection, String idField,
                                      long expectedDocCount, String query, String tag,
                                      boolean failAnyway) throws IOException, SolrServerException {

    final SolrQuery solrQuery = new SolrQuery(query);
    QueryResponse rsp = client.query(collection, solrQuery);
    long found = rsp.getResults().getNumFound();

    if (found == expectedDocCount) {
      return;
    }

    // OK, our counts differ. Insert a document _guaranteed_ to be unique, then delete it so whatever is counting
    // anything has the correct counts.
    log.warn("Solr11035BandAid attempting repair, found is {}, expected is {}", found, expectedDocCount);

    String bogusID = java.util.UUID.randomUUID().toString();
    SolrInputDocument bogus = new SolrInputDocument();
    bogus.addField(idField, bogusID);

    // Add the bogus doc
    new UpdateRequest().add(bogus).commit(client, collection);

    // Let's spin until we find the doc.
    checkUniqueDoc(client, collection, idField, bogusID, true);

    // Then remove it, we should be OK now since new searchers have been opened.
    new UpdateRequest().deleteById(bogusID).commit(client, collection);

    // Now spin until the doc is gone.
    checkUniqueDoc(client, collection, idField, bogusID, false);

    // At this point we're absolutely, totally, positive that a new searcher has been opened, so go ahead and check
    // the actual condition.
    rsp = client.query(collection, solrQuery);
    found = rsp.getResults().getNumFound();

    if (found != expectedDocCount) {
      // It's good to see the total response. NOTE: some larger responses are over 10240,
      // so change the pattern in log4j2.xml if you need to see the whole thing.
      log.error("Dumping response" + rsp.toString());
      assertEquals("Solr11035BandAid failed, counts differ after updates:", found, expectedDocCount);
    } else if (failAnyway) {
      fail("Solr11035BandAid failAnyway == true, would have successfully repaired the collection: '" + collection
          + "' extra info: '" + tag + "'");
    } else {
      log.warn("Solr11035BandAid, repair successful");
    }
  }
  // Helper for bandaid
  private static void checkUniqueDoc(SolrClient client, String collection, String idField, String id, boolean shouldBeThere) throws IOException, SolrServerException {
    TimeOut timeOut = new TimeOut(100, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    final SolrQuery solrQuery = new SolrQuery(idField + ":" + id);

    while (!timeOut.hasTimedOut()) {
      QueryResponse rsp = client.query(collection, solrQuery);
      long found = rsp.getResults().getNumFound();
      if (shouldBeThere && found == 1) {
        return;
      }
      if (shouldBeThere == false && found == 0) {
        return;
      }
      log.warn("Solr11035BandAid should have succeeded in checkUniqueDoc, shouldBeThere == {}, numFound = {}. Will try again after 250 ms sleep", shouldBeThere, found);
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        return; // just bail
      }
    }

  }
}
