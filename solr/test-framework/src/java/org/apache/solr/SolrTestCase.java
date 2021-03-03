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

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.rules.StaticFieldsInvariantRule;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.FailureMarker;
import org.apache.lucene.util.LuceneJUnit3MethodProvider;
import org.apache.lucene.util.LuceneTestCase;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene86.Lucene86Codec;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.RunListenerPrintReproduceInfo;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.ParWorkExecutor;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.TimeTracker;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.CryptoKeys;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.TestInjection;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * All Solr test cases should derive from this class eventually. This is originally a result of async logging, see:
 * SOLR-12055 and associated. To enable async logging, we must gracefully shut down logging. Many Solr tests subclass
 * LuceneTestCase.
 *
 * Rather than add the cruft from SolrTestCaseJ4 to all the Solr tests that currently subclass LuceneTestCase,
 * we'll add the shutdown to this class and subclass it.
 *
 * Other changes that should affect every Solr test case may go here if they don't require the added capabilities in
 * SolrTestCaseJ4.
 */
//0p-@TimeoutSuite(millis = 130 * TimeUnits.SECOND)

@RunWith(RandomizedRunner.class)
@TestMethodProviders({
    LuceneJUnit3MethodProvider.class,
    JUnit4MethodProvider.class
})
@Listeners({
    RunListenerPrintReproduceInfo.class,
    FailureMarker.class
})
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakGroup(ThreadLeakGroup.Group.MAIN)
@ThreadLeakAction({ThreadLeakAction.Action.WARN, ThreadLeakAction.Action.INTERRUPT})
@SeedDecorators({MixWithSuiteName.class}) // See LUCENE-3995 for rationale.

@ThreadLeakFilters(defaultFilters = true, filters = {
        SolrIgnoredThreadsFilter.class,
        QuickPatchThreadsFilter.class
})
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Solr dumps tons of logs to console.")
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // might be ok, the failures with e.g. nightly runs might be "normal"
@RandomizeSSL()
@ThreadLeakLingering(linger = 0)
public class SolrTestCase extends Assert {

  protected static final boolean VERBOSE = false;

  /**
   * <b>DO NOT REMOVE THIS LOGGER</b>
   * <p>
   * For reasons that aren't 100% obvious, the existence of this logger is neccessary to ensure
   * that the logging framework is properly initialized (even if concrete subclasses do not 
   * themselves initialize any loggers) so that the async logger threads can be properly shutdown
   * on completion of the test suite
   * </p>
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-14247">SOLR-14247</a>
   * @see #afterSolrTestCase()
   */
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String[] EMPTY_STRING_ARRAY = new String[0];

  /**
   * Max 10mb of static data stored in a test suite class after the suite is complete.
   * Prevents static data structures leaking and causing OOMs in subsequent tests.
   */
  private final static long STATIC_LEAK_THRESHOLD = 600; // MRM TODO: I dropped this down hard and enabled it again

  public static final boolean TEST_NIGHTLY = LuceneTestCase.TEST_NIGHTLY;

  public static TestRuleThreadAndTestName threadAndTestNameRule = new TestRuleThreadAndTestName();

  private static TestRuleSetupTeardownChained parentChainCallRule = new TestRuleSetupTeardownChained();

  protected static TestRuleMarkFailure suiteFailureMarker;

  public static TestRuleTemporaryFilesCleanup tempFilesCleanupRule;

  private static TestRuleSetupAndRestoreClassEnv classEnvRule;
  @ClassRule
  public static TestRule solrClassRules =
      RuleChain.outerRule(new SystemPropertiesRestoreRule())
          .around(suiteFailureMarker = new TestRuleMarkFailure())
          .around(tempFilesCleanupRule = new TestRuleTemporaryFilesCleanup(suiteFailureMarker)).around(new StaticFieldsInvariantRule(STATIC_LEAK_THRESHOLD, true) {
    @Override
    protected boolean accept(java.lang.reflect.Field field) {
      // Don't count known classes that consume memory once.
      if (LuceneTestCase.STATIC_LEAK_IGNORED_TYPES.contains(field.getType().getName())) {
        return false;
      }
      // Don't count references from ourselves, we're top-level.
      if (field.getDeclaringClass() == SolrTestCase.class) {
        return false;
      }
      return super.accept(field);
    }
  })
          .around(classEnvRule = new TestRuleSetupAndRestoreClassEnv()).around(new RevertDefaultThreadHandlerRule());
  private final SolrTestUtil solrTestUtil = new SolrTestUtil();

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule())
      .around(new RevertDefaultThreadHandlerRule()).around(threadAndTestNameRule).around(parentChainCallRule);

  private static volatile Random random;

  private volatile static ParWorkExecutor testExecutor;

  private static volatile CryptoKeys.RSAKeyPair reusedKeys;

  private static CryptoKeys.RSAKeyPair getRsaKeyPair() {
    String publicKey = System.getProperty("pkiHandlerPublicKeyPath");
    String privateKey = System.getProperty("pkiHandlerPrivateKeyPath");
    // If both properties unset, then we fall back to generating a new key pair
    if (StringUtils.isEmpty(publicKey) && StringUtils.isEmpty(privateKey)) {
      return new CryptoKeys.RSAKeyPair();
    }

    try {
      return new CryptoKeys.RSAKeyPair(new URL(privateKey), new URL(publicKey));
    } catch (Exception e) {
      log.error("Error in pblic key/private key URLs", e);
    }
    return new CryptoKeys.RSAKeyPair();
  }

  public static synchronized void enableReuseOfCryptoKeys() {
    if (reusedKeys == null) {
      reusedKeys = getRsaKeyPair();
    }
    PublicKeyHandler.REUSABLE_KEYPAIR = reusedKeys;
  }

  public static void disableReuseOfCryptoKeys() {
    PublicKeyHandler.REUSABLE_KEYPAIR = null;
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
   * Annotation for test classes that always need SSL
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface AlwaysUseSSL {

  }

  /**
   * Annotation for test classes that want to disable SSL
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressObjectReleaseTracker {
    public String object();
  }


  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 30000;  // default socket connection timeout in ms
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;  // default socket connection timeout in ms
  public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 30000;

  private static final int SOLR_TEST_TIMEOUT = Integer.getInteger("solr.test.timeout", 25);

  private static long testStartTime;

  // these are meant to be accessed sequentially, but are volatile just to ensure any test
  // thread will read the latest value
  protected static volatile SSLTestConfig sslConfig;

  private final static Set<String> interuptThreadWithNameContains = ConcurrentHashMap.newKeySet();

  public static Random random() {
    return random;
  }

  /**
   * Sets the <code>solr.default.confdir</code> system property to the value of 
   * {@link ExternalPaths#DEFAULT_CONFIGSET} if and only if the system property is not already set, 
   * and the <code>DEFAULT_CONFIGSET</code> exists and is a readable directory.
   * <p>
   * Logs INFO/WARNing messages as appropriate based on these 2 conditions.
   * </p>
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  @BeforeClass
  public static void beforeSolrTestCase() throws Exception {
    log.info("*******************************************************************");
    log.info("@BeforeClass ------------------------------------------------------");

    System.setProperty("org.eclipse.jetty.util.log.class", "org.apache.logging.log4j.appserver.jetty.Log4j2Logger");

    interruptThreadsOnTearDown(false, "SessionTracker");

    if (!SysStats.getSysStats().isAlive()) {
      SysStats.reStartSysStats();
    }

    // random is expensive, you are supposed to cache it
    random = LuceneTestCase.random();

    testStartTime = System.nanoTime();

    sslConfig = SolrTestUtil.buildSSLConfig();
    if (sslConfig != null && sslConfig.isSSLMode()) {
      HttpClientUtil.setSocketFactoryRegistryProvider(sslConfig.buildClientSocketFactoryRegistryProvider());
      Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    }
    // based on randomized SSL config, set SocketFactoryRegistryProvider appropriately
    if (isSSLMode()) {
      // SolrCloud tests should usually clear this
      System.setProperty("urlScheme", "https");
    } else {
      System.setProperty("urlScheme", "http");
    }

    System.setProperty("useCompoundFile", "true");
    System.setProperty("solr.tests.maxBufferedDocs", "1000");

    System.setProperty("pkiHandlerPrivateKeyPath", SolrTestCaseJ4.class.getClassLoader().getResource("cryptokeys/priv_key512_pkcs8.pem").toExternalForm());
    System.setProperty("pkiHandlerPublicKeyPath", SolrTestCaseJ4.class.getClassLoader().getResource("cryptokeys/pub_key512.der").toExternalForm());

    System.setProperty("solr.createCollectionTimeout", "10000");
    System.setProperty("solr.enablePublicKeyHandler", "true");
    System.setProperty("solr.zkclienttimeout", "30000");
    System.setProperty("solr.v2RealPath", "true");
    System.setProperty("zookeeper.forceSync", "no");
    System.setProperty("jetty.testMode", "true");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    System.setProperty("solr.clustering.enabled", "false");
    System.setProperty("solr.peerSync.useRangeVersions", String.valueOf(random().nextBoolean()));
    System.setProperty("zookeeper.nio.directBufferBytes", Integer.toString(32 * 1024 * 2));

    // we need something as a default, at least these are fast
    System.setProperty(SolrTestCaseJ4.USE_NUMERIC_POINTS_SYSPROP, "false");
    System.setProperty("solr.tests.IntegerFieldType", "org.apache.solr.schema.TrieIntField");
    System.setProperty("solr.tests.FloatFieldType", "org.apache.solr.schema.TrieFloatField");
    System.setProperty("solr.tests.LongFieldType", "org.apache.solr.schema.TrieLongField");
    System.setProperty("solr.tests.DoubleFieldType", "org.apache.solr.schema.TrieDoubleField");
    System.setProperty("solr.tests.DateFieldType", "org.apache.solr.schema.TrieDateField");
    System.setProperty("solr.tests.EnumFieldType", "org.apache.solr.schema.EnumFieldType");
    System.setProperty("solr.tests.numeric.dv", "true");

    System.setProperty("solr.tests.ramBufferSizeMB", "100");
    System.setProperty("solr.tests.ramPerThreadHardLimitMB", "100");

    System.setProperty("solr.tests.mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
    System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    System.setProperty("solr.mscheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    //enableReuseOfCryptoKeys();


    // default field types
    System.setProperty(SolrTestCaseJ4.USE_NUMERIC_POINTS_SYSPROP, "false");
    System.setProperty("solr.tests.IntegerFieldType", "org.apache.solr.schema.TrieIntField");
    System.setProperty("solr.tests.FloatFieldType", "org.apache.solr.schema.TrieFloatField");
    System.setProperty("solr.tests.LongFieldType", "org.apache.solr.schema.TrieLongField");
    System.setProperty("solr.tests.DoubleFieldType", "org.apache.solr.schema.TrieDoubleField");
    System.setProperty("solr.tests.DateFieldType", "org.apache.solr.schema.TrieDateField");
    System.setProperty("solr.tests.EnumFieldType", "org.apache.solr.schema.EnumFieldType");
    System.setProperty("solr.tests.numeric.dv", "true");

    System.setProperty("managed.schema.mutable", "false");

    if (!LuceneTestCase.TEST_NIGHTLY) {
      //TestInjection.randomDelayMaxInCoreCreationInSec = 2;
      Lucene86Codec codec = new Lucene86Codec(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
      //Codec.setDefault(codec);
      disableReuseOfCryptoKeys();
      System.setProperty("solr.zkstatewriter.throttle", "0");
      System.setProperty("solr.stateworkqueue.throttle", "0");

      System.setProperty("zkReaderGetLeaderRetryTimeoutMs", "800");

      System.setProperty("solr.enablePublicKeyHandler", "false");
      System.setProperty("solr.zkregister.leaderwait", "3000");
      System.setProperty("solr.lbclient.live_check_interval", "3000");
      System.setProperty("solr.httpShardHandler.completionTimeout", "10000");
      System.setProperty("zookeeper.request.timeout", "15000");


      System.setProperty("solr.concurrentRequests.max", "15");
      System.setProperty("solr.tests.infostream", "false");
      System.setProperty("numVersionBuckets", "16384"); // TODO: wrong sys prop, also not usually setup in conf to work

    //  System.setProperty("solr.per_thread_exec.max_threads", "2");
   //   System.setProperty("solr.per_thread_exec.min_threads", "1");

      System.setProperty("zookeeper.nio.numSelectorThreads", "2");
      System.setProperty("zookeeper.nio.numWorkerThreads", "3");
      System.setProperty("zookeeper.commitProcessor.numWorkerThreads", "2");
      System.setProperty("zookeeper.skipACL", "true");
      System.setProperty("zookeeper.nio.shutdownTimeout", "10");

      // can make things quite slow
      System.setProperty("solr.disableDefaultJmxReporter", "true");

      System.setProperty("solr.skipCommitOnClose", "false");

      // can generate tons of URL garbage and can happen too often, defaults to false now anyway
      System.setProperty("solr.reloadSPI", "false");

      // MRM TODO: - not used again yet
      // System.setProperty("solr.OverseerStateUpdateDelay", "0");

      System.setProperty("solr.enableMetrics", "false");

//      System.setProperty("solr.leaderThrottle", "1000");
//      System.setProperty("solr.recoveryThrottle", "1000");

      System.setProperty("solr.suppressDefaultConfigBootstrap", "true");

      System.setProperty("solr.defaultCollectionActiveWait", "10");

      System.setProperty("solr.http2solrclient.maxpool.size", "16");
      System.setProperty("solr.http2solrclient.pool.keepalive", "1500");

      System.setProperty("solr.dependentupdate.timeout", "1500");

     // System.setProperty("lucene.cms.override_core_count", "3");
     // System.setProperty("lucene.cms.override_spins", "false");

      // unlimited - System.setProperty("solr.maxContainerThreads", "300");
      System.setProperty("solr.lowContainerThreadsThreshold", "-1");
      System.setProperty("solr.minContainerThreads", "8");
      System.setProperty("solr.rootSharedThreadPoolCoreSize", "12");
      System.setProperty("solr.minHttp2ClientThreads", "6");

      System.setProperty("solr.containerThreadsIdleTimeout", "1000");
      System.setProperty("solr.containerThreadsIdle", "1000");


      System.setProperty("solr.tests.maxBufferedDocs", "1000000");
      System.setProperty("solr.tests.ramPerThreadHardLimitMB", "90");

      System.setProperty("solr.tests.ramBufferSizeMB", "100");

      System.setProperty("solr.http2solrclient.default.idletimeout", "15000");
      System.setProperty("distribUpdateSoTimeout", "15000");
      System.setProperty("socketTimeout", "30000");
      System.setProperty("connTimeout", "30000");
      System.setProperty("solr.test.socketTimeout.default", "30000");
      System.setProperty("solr.connect_timeout.default", "10000");
      System.setProperty("solr.so_commit_timeout.default", "15000");
      System.setProperty("solr.httpclient.defaultConnectTimeout", "10000");
      System.setProperty("solr.httpclient.defaultSoTimeout", "30000");


      System.setProperty("solr.indexfetcher.sotimeout", "30000");
      System.setProperty("solr.indexfetch.so_timeout.default", "30000");

      System.setProperty("prepRecoveryReadTimeoutExtraWait", "100");
      System.setProperty("validateAfterInactivity", "-1");
      System.setProperty("leaderVoteWait", "2500"); // this is also apparently controlling how long we wait for a leader on register MRM TODO:
      System.setProperty("leaderConflictResolveWait", "10000");

      System.setProperty("solr.recovery.recoveryThrottle", "0");
      System.setProperty("solr.recovery.leaderThrottle", "0");

      System.setProperty("bucketVersionLockTimeoutMs", "8000");
      System.setProperty("socketTimeout", "15000");
      System.setProperty("connTimeout", "10000");
      System.setProperty("solr.cloud.wait-for-updates-with-stale-state-pause", "0");
      System.setProperty("solr.cloud.starting-recovery-delay-milli-seconds", "0");

      System.setProperty("solr.waitForState", "15"); // secs

      System.setProperty("solr.default.collection_op_timeout", "15000");

      System.setProperty("useCompoundFile", "false");

      System.setProperty("solr.httpclient.retries", "1");
      System.setProperty("solr.retries.on.forward", "1");
      System.setProperty("solr.retries.to.followers", "1");

      SolrTestCaseJ4.useFactory("org.apache.solr.core.RAMDirectoryFactory");
      System.setProperty("solr.lock.type", "single");
      System.setProperty("solr.tests.lockType", "single");

      System.setProperty("solr.codec", "solr.SchemaCodecFactory");
      System.setProperty("tests.COMPRESSION_MODE", "BEST_COMPRESSION");
    }


    final String existingValue = System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    if (null != existingValue) {
      log.info("Test env includes configset dir system property '{}'='{}'", SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, existingValue);
      return;
    }
    final File extPath = new File(ExternalPaths.DEFAULT_CONFIGSET);
    if (extPath.canRead(/* implies exists() */) && extPath.isDirectory()) {
      log.info("Setting '{}' system property to test-framework derived value of '{}'",
               SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
      assert null == existingValue;
      System.setProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    } else {
      log.warn("System property '{}' is not already set, but test-framework derived value ('{}') either " +
               "does not exist or is not a readable directory, you may need to set the property yourself " +
               "for tests to run properly",
               SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, ExternalPaths.DEFAULT_CONFIGSET);
    }

    log.info("@BeforeClass end ------------------------------------------------------");
    log.info("*******************************************************************");
  }

  protected static boolean isSSLMode() {
    return sslConfig != null && sslConfig.isSSLMode();
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link BeforeClass} method
   * @lucene.internal
   */
//  @BeforeClass
//  public static void checkSyspropForceBeforeClassAssumptionFailure() {
//    // ant test -Dargs="-Dtests.force.assumption.failure.beforeclass=true"
//    final String PROP = "tests.force.assumption.failure.beforeclass";
//    assumeFalse(PROP + " == true",
//                systemPropertyAsBoolean(PROP, false));
//  }

  @After
  public void afterSolrTestCase() throws Exception {

  }

  @AfterClass
  public static void afterSolrTestCaseClass() throws Exception {
    log.info("*******************************************************************");
    log.info("@After Class ------------------------------------------------------");
    try {

      SysStats.getSysStats().stopMonitor();

      if (testExecutor != null) {
        testExecutor.disableCloseLock();
        testExecutor.shutdown();
      }


      AlreadyClosedException lastAlreadyClosedExp = CloseTracker.lastAlreadyClosedEx;
      if (lastAlreadyClosedExp != null) {
        CloseTracker.lastAlreadyClosedEx = null;
        throw lastAlreadyClosedExp;
      }


      IllegalCallerException lastIllegalCallerEx = CloseTracker.lastIllegalCallerEx;
      if (lastIllegalCallerEx != null) {
        CloseTracker.lastIllegalCallerEx = null;
        throw lastIllegalCallerEx;
      }

      String object = null;
      // if the tests passed, make sure everything was closed / released
      if (RandomizedTest.getContext().getTargetClass().isAnnotationPresent(SuppressObjectReleaseTracker.class)) {
        SuppressObjectReleaseTracker sor = RandomizedTest.getContext().getTargetClass().getAnnotation(SuppressObjectReleaseTracker.class);
        object = sor.object();
      }

      String orr = ObjectReleaseTracker.checkEmpty(object);
      ObjectReleaseTracker.clear();
      assertNull(orr, orr);

      if (testExecutor != null) {
        boolean success = testExecutor.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(success);
        testExecutor = null;
      }
      ParWork.shutdownParWorkExecutor();

    } finally {
      ObjectReleaseTracker.clear();
      TestInjection.reset();
    }
    try {
      HttpClientUtil.resetHttpClientBuilder();
      Http2SolrClient.resetSslContextFactory();
      TestInjection.reset();
      JSONTestUtil.failRepeatedKeys = false;
      random = null;
      reusedKeys = null;
      sslConfig = null;

      long testTime = TimeUnit.SECONDS.convert(System.nanoTime() - testStartTime, TimeUnit.NANOSECONDS);
      if (!LuceneTestCase.TEST_NIGHTLY && testTime > SOLR_TEST_TIMEOUT) {
        log.error("This test suite is too long for non @Nightly runs! Please improve it's performance, break it up, make parts of it @Nightly or make the whole suite @Nightly: {}"
               , testTime);
//          fail(
//              "This test suite is too long for non @Nightly runs! Please improve it's performance, break it up, make parts of it @Nightly or make the whole suite @Nightly: "
//                  + testTime);
      }
    } finally {

      Class<? extends Object> clazz = null;
      Long tooLongTime = 0L;
      String times = null;
//      try {
//        synchronized (TimeTracker.CLOSE_TIMES) {
//          Map<String, TimeTracker> closeTimes = TimeTracker.CLOSE_TIMES;
//          for (TimeTracker closeTime : closeTimes.values()) {
//            int closeTimeout = Integer.getInteger("solr.parWorkTestTimeout", 10000);
//            if (closeTime.getElapsedMS() > closeTimeout) {
//              tooLongTime = closeTime.getElapsedMS();
//              clazz = closeTime.getClazz();
//              times = closeTime.getCloseTimes();
//            }
//            // turn off until layout is fixed again
//            // closeTime.printCloseTimes();
//          }
//        }
//
//      } finally {
        TimeTracker.CLOSE_TIMES.clear();
//      }

      if (clazz != null) {
        // MRM TODO: - leave this on
        if (!LuceneTestCase.TEST_NIGHTLY) fail("A " + clazz.getName() + " took too long to close: " + tooLongTime + "\n" + times);
      }
    }
    if (log.isInfoEnabled()) {
      log.info("@AfterClass end ------------------------------------------------------");
    }
    if (log.isInfoEnabled()) {
      log.info("*******************************************************************");
    }

    checkForInterruptRequest();
    interuptThreadWithNameContains.clear();
    StartupLoggingUtils.shutdown();
  }

  private static void checkForInterruptRequest() {
    try {
      interruptThreadsOnTearDown(true, interuptThreadWithNameContains.toArray(EMPTY_STRING_ARRAY));
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("", e);
    }
  }

  /**
   * For subclasses to override. Overrides must call {@code super.setUp()}.
   */
  @Before
  public void setUp() throws Exception {
    parentChainCallRule.setupCalled = true;
  }

  /**
   * For subclasses to override. Overrides must call {@code super.tearDown()}.
   */
  @After
  public void tearDown() throws Exception {
    parentChainCallRule.teardownCalled = true;
  }

  // expert - for special cases
  public static void interruptThreadsOnTearDown(boolean now, String... nameContains) {
    if (!now) {
      interuptThreadWithNameContains.addAll(Arrays.asList(nameContains));
      return;
    }

    log.info("Checking leaked threads after test");

   // System.out.println("DO FORCED INTTERUPTS");
    //  we need to filter and only do this for known threads? dont want users to count on this behavior unless necessary
    String testThread = Thread.currentThread().getName();
   // System.out.println("test thread:" + testThread);
    ThreadGroup tg = Thread.currentThread().getThreadGroup();
  //  System.out.println("test group:" + tg.getName());
    Set<Map.Entry<Thread,StackTraceElement[]>> threadSet = Thread.getAllStackTraces().entrySet();
    if (log.isInfoEnabled()) {
      log.info("thread count={}", threadSet.size());
    }
    List<Thread> waitThreads = new ArrayList<>();
    for (Map.Entry<Thread,StackTraceElement[]> threadEntry : threadSet) {
      Thread thread = threadEntry.getKey();
      ThreadGroup threadGroup = thread.getThreadGroup();
      if (threadGroup != null) {
        log.warn("thread is {}", thread.getName());
        if (threadGroup.getName().equals(tg.getName()) && !(thread.getName().startsWith("SUITE") && thread.getName().endsWith("]"))) {
          if (interrupt(thread, nameContains)) {
            waitThreads.add(thread);
          }
        }
      }

      while (threadGroup != null && threadGroup.getParent() != null && !(thread.getName().startsWith("SUITE") && thread.getName().endsWith("]"))) {
        threadGroup = threadGroup.getParent();
        //if (thread.getState().equals(Thread.State.TERMINATED) || nameContains != null && threadGroup.getName().equals(tg.getName())) {
        if (threadGroup.getName().equals(tg.getName())) {
          log.warn("thread is {}", thread.getName());
          if (interrupt(thread, nameContains)) {
            waitThreads.add(thread);
          }
        }
      }
    }
    for (Thread thread : waitThreads) {
      SolrTestUtil.wait(thread);
    }

    waitThreads.clear();
  }

  private static boolean interrupt(Thread thread, String... nameContains) {
    if (thread.getName().contains("ForkJoinPool.") || thread.getName().contains("Log4j2-")) {
      return false;
    }

    if (thread.getName().contains("-SendThread")) {
      log.warn("interrupt on {}",  thread.getName());
      thread.interrupt();
      return true;
    }
    if ((thread.getName().contains(ParWork.ROOT_EXEC_NAME + "-") || thread.getName().contains("ParWork-") || thread.getName().contains("Core-")
        || thread.getName().contains("ProcessThread(") && thread.getState() != Thread.State.TERMINATED)) {
      log.warn("interrupt on {}", thread.getName());
      thread.interrupt();
      return true;
    }
    if (interruptThreadListContains(nameContains, thread.getName()) && thread.getState() != Thread.State.TERMINATED) {
      log.warn("interrupt on {}", thread.getName());
      thread.interrupt();
      return true;
    }
    return false;
  }

  private static boolean interruptThreadListContains(String[] nameContains, String name) {
    for (String interruptThread : nameContains) {
      if (name.contains(interruptThread)) {
        return true;
      }
    }
    return false;
  }

  public static SolrQueuedThreadPool getQtp() throws Exception {
    return new SolrQueuedThreadPool("solr-test-qtp");
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
      savedFactory = null;
    } else {
      System.clearProperty("solr.directoryFactory");
    }
  }


  public String getSaferTestName() {
    // test names can hold additional info, like the test seed
    // only take to first space
    return solrTestUtil.getSaferTestName();
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

  public boolean compareSolrDocumentList(Object expected, Object actual) {

    return solrTestUtil.compareSolrDocumentList(expected, actual);
  }

  public boolean compareSolrDocument(Object expected, Object actual) {

    return solrTestUtil.compareSolrDocument(expected, actual);
  }

  public static ExecutorService getTestExecutor() {
    synchronized (SolrTestCase.class) {
      if (testExecutor != null) {
        return testExecutor;
      }
      testExecutor = (ParWorkExecutor) ParWork.getParExecutorService(
          "testExecutor", 5, 30, 500, new BlockingArrayQueue(12, 16));
      testExecutor.prestartAllCoreThreads();
      ((ParWorkExecutor) testExecutor).enableCloseLock();
      return testExecutor;
    }
  }

  private static class SolrTestWatcher extends TestWatcher {
    @Override
    protected void failed(Throwable e, Description description) {

    }

    @Override
    protected void succeeded(Description description) {
    }
  }

  /**
   * Saves the executing thread and method name of the test case.
   */
  final static class TestRuleThreadAndTestName implements TestRule {

    public volatile Thread testCaseThread;

    /**
     * Test method name.
     */
    public volatile String testMethodName = "<unknown>";

    @Override
    public Statement apply(final Statement base, final Description description) {
      return new TestStatement(base, description);
    }

    private class TestStatement extends Statement {
      private final Statement base;
      private final Description description;

      public TestStatement(Statement base, Description description) {
        this.base = base;
        this.description = description;
      }

      @Override
      public void evaluate() throws Throwable {
        try {
          Thread current = Thread.currentThread();
          testCaseThread = current;
          testMethodName = description.getMethodName();

          base.evaluate();
        } finally {
          testCaseThread = null;
          testMethodName = null;
        }
      }
    }
  }

  private IndexableField newTextField(String value, String foo_bar_bar_bar_bar, Field.Store no) {
    return solrTestUtil.newTextField(value, foo_bar_bar_bar_bar, no);
  }

  protected IndexableField newStringField(String value, String bar, Field.Store yes) {
    return solrTestUtil.newStringField(value, bar, yes);
  }

  /**
   * Make sure {@link LuceneTestCase#setUp()} and {@link LuceneTestCase#tearDown()} were invoked even if they
   * have been overriden. We assume nobody will call these out of non-overriden
   * methods (they have to be public by contract, unfortunately). The top-level
   * methods just set a flag that is checked upon successful execution of each test
   * case.
   */
  static class TestRuleSetupTeardownChained implements TestRule {
    /**
     * see org.apache.lucene.util.TestRuleSetupTeardownChained
     */
    public volatile boolean setupCalled;

    /**
     * see org.apache.lucene.util.TestRuleSetupTeardownChained
     */
    public volatile boolean teardownCalled;

    @Override
    public Statement apply(final Statement base, Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          setupCalled = false;
          teardownCalled = false;
          base.evaluate();

          // I assume we don't want to check teardown chaining if something happens in the
          // test because this would obscure the original exception?
          if (!setupCalled) {
            Assert.fail("One of the overrides of setUp does not propagate the call.");
          }
          if (!teardownCalled) {
            Assert.fail("One of the overrides of tearDown does not propagate the call.");
          }
        }
      };
    }
  }
}
