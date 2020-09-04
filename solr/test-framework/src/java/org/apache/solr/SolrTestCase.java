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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.codecs.lucene86.Lucene86Codec;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.autoscaling.ScheduledTriggers;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.PerThreadExecService;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.TimeTracker;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.apache.solr.common.util.SysStats;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

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
@ThreadLeakFilters(defaultFilters = true, filters = {
        SolrIgnoredThreadsFilter.class,
        QuickPatchThreadsFilter.class
})
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "Solr dumps tons of logs to console.")
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // might be ok, the failures with e.g. nightly runs might be "normal"
@RandomizeSSL()
@ThreadLeakLingering(linger = 0)
public class SolrTestCase extends LuceneTestCase {

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

  @ClassRule
  public static TestRule solrClassRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule())
             .around(new RevertDefaultThreadHandlerRule());

  private static volatile Random random;

  private static volatile boolean failed = false;

  protected volatile static ExecutorService testExecutor;

  protected static volatile SolrQueuedThreadPool qtp;

  @Rule
  public TestRule solrTestRules =
          RuleChain.outerRule(new SystemPropertiesRestoreRule()).around(new SolrTestWatcher());

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
   * Annotation for test classes that want to disable SSL
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressObjectReleaseTracker {
    public String object();
  }


  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 20000;  // default socket connection timeout in ms
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;  // default socket connection timeout in ms
  public static final int DEFAULT_SOCKET_TIMEOUT_MILLIS = 15000;

  private static final int SOLR_TEST_TIMEOUT = Integer.getInteger("solr.test.timeout", 25);

  private static long testStartTime;

  // these are meant to be accessed sequentially, but are volatile just to ensure any test
  // thread will read the latest value
  protected static volatile SSLTestConfig sslConfig;

  private volatile static String interuptThreadWithNameContains;

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
  public static void setDefaultConfigDirSysPropIfNotSet() throws Exception {
    log.info("*******************************************************************");
    log.info("@BeforeClass ------------------------------------------------------");

    System.setProperty("org.eclipse.jetty.util.log.class", "org.apache.logging.log4j.appserver.jetty.Log4j2Logger");

    // we do this because threads can be finished, but waiting
    // for an idle timeout or in a TERMINATED state, and we don't
    // want to wait for them - in prod these threads are daemon
    interruptThreadsOnTearDown("ParWork", false);

    if (!SysStats.getSysStats().isAlive()) {
      SysStats.reStartSysStats();
    }

    // random is expensive, you are supposed to cache it
    random = LuceneTestCase.random();

    testStartTime = System.nanoTime();


    testExecutor = ParWork.getMyPerThreadExecutor();
    ((PerThreadExecService) testExecutor).closeLock(true);
    // stop zkserver threads that can linger
    //interruptThreadsOnTearDown("nioEventLoopGroup", false);

    sslConfig = buildSSLConfig();

    HttpClientUtil.setSocketFactoryRegistryProvider(sslConfig.buildClientSocketFactoryRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    // based on randomized SSL config, set SocketFactoryRegistryProvider appropriately
    if(isSSLMode()) {
      // SolrCloud tests should usually clear this
      System.setProperty("urlScheme", "https");
    } else {
      System.setProperty("urlScheme", "http");
    }


    System.setProperty("solr.zkclienttimeout", "30000");
    System.setProperty("solr.v2RealPath", "true");
    System.setProperty("zookeeper.forceSync", "no");
    System.setProperty("jetty.testMode", "true");
    System.setProperty("enable.update.log", usually() ? "true" : "false");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    System.setProperty("solr.clustering.enabled", "false");
    System.setProperty("solr.peerSync.useRangeVersions", String.valueOf(random().nextBoolean()));
    System.setProperty("zookeeper.nio.directBufferBytes", Integer.toString(32 * 1024 * 2));
    System.setProperty("solr.disablePublicKeyHandler", "true");

    if (!TEST_NIGHTLY) {
      //TestInjection.randomDelayMaxInCoreCreationInSec = 2;
      Lucene86Codec codec = new Lucene86Codec(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
      //Codec.setDefault(codec);
      System.setProperty("solr.lbclient.live_check_interval", "3000");
      System.setProperty("solr.httpShardHandler.completionTimeout", "10000");
      System.setProperty("zookeeper.request.timeout", "15000");
      System.setProperty(SolrTestCaseJ4.USE_NUMERIC_POINTS_SYSPROP, "false");
      System.setProperty("solr.tests.IntegerFieldType", "org.apache.solr.schema.TrieIntField");
      System.setProperty("solr.tests.FloatFieldType", "org.apache.solr.schema.TrieFloatField");
      System.setProperty("solr.tests.LongFieldType", "org.apache.solr.schema.TrieLongField");
      System.setProperty("solr.tests.DoubleFieldType", "org.apache.solr.schema.TrieDoubleField");
      System.setProperty("solr.tests.DateFieldType", "org.apache.solr.schema.TrieDateField");
      System.setProperty("solr.tests.EnumFieldType", "org.apache.solr.schema.EnumFieldType");
      System.setProperty("solr.tests.numeric.dv", "true");


      System.setProperty("solr.concurrentRequests.max", "15");
      System.setProperty("solr.tests.infostream", "false");
      System.setProperty("numVersionBuckets", "16384");

    //  System.setProperty("solr.per_thread_exec.max_threads", "2");
   //   System.setProperty("solr.per_thread_exec.min_threads", "1");

      System.setProperty("zookeeper.nio.numSelectorThreads", "2");
      System.setProperty("zookeeper.nio.numWorkerThreads", "3");
      System.setProperty("zookeeper.commitProcessor.numWorkerThreads", "2");
      System.setProperty("zookeeper.skipACL", "true");
      System.setProperty("zookeeper.nio.shutdownTimeout", "10");

      // can make things quite slow
      System.setProperty("solr.disableJmxReporter", "true");
      System.setProperty("solr.skipCommitOnClose", "true");

      // can generate tons of URL garbage and can happen too often, defaults to false now anyway
      System.setProperty("solr.reloadSPI", "false");

      // nocommit - not used again yet
      // System.setProperty("solr.OverseerStateUpdateDelay", "0");

      System.setProperty("solr.disableMetricsHistoryHandler", "true");

      System.setProperty("solr.leaderThrottle", "1000");
      System.setProperty("solr.recoveryThrottle", "1000");

      System.setProperty("solr.suppressDefaultConfigBootstrap", "true");

      System.setProperty("solr.defaultCollectionActiveWait", "10");

      System.setProperty("solr.http2solrclient.maxpool.size", "6");
      System.setProperty("solr.http2solrclient.pool.keepalive", "1500");

      System.setProperty("solr.disablePublicKeyHandler", "false");
      System.setProperty("solr.dependentupdate.timeout", "1"); // seconds

     // System.setProperty("lucene.cms.override_core_count", "3");
     // System.setProperty("lucene.cms.override_spins", "false");

      // unlimited - System.setProperty("solr.maxContainerThreads", "300");
      System.setProperty("solr.lowContainerThreadsThreshold", "-1");
      System.setProperty("solr.minContainerThreads", "8");

      ScheduledTriggers.DEFAULT_COOLDOWN_PERIOD_SECONDS = 1;
      ScheduledTriggers.DEFAULT_ACTION_THROTTLE_PERIOD_SECONDS =1;
      ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE = 2;

      System.setProperty("solr.tests.maxBufferedDocs", "1000000");
      System.setProperty("solr.tests.ramPerThreadHardLimitMB", "30");

      System.setProperty("solr.tests.ramBufferSizeMB", "100");

      System.setProperty("solr.http2solrclient.default.idletimeout", "15000");
      System.setProperty("distribUpdateSoTimeout", "10000");
      System.setProperty("socketTimeout", "15000");
      System.setProperty("connTimeout", "10000");
      System.setProperty("solr.test.socketTimeout.default", "15000");
      System.setProperty("solr.connect_timeout.default", "10000");
      System.setProperty("solr.so_commit_timeout.default", "15000");
      System.setProperty("solr.httpclient.defaultConnectTimeout", "10000");
      System.setProperty("solr.httpclient.defaultSoTimeout", "15000");
      // System.setProperty("solr.containerThreadsIdle", "30000"); no need to set

      System.setProperty("solr.indexfetcher.sotimeout", "15000");
      System.setProperty("solr.indexfetch.so_timeout.default", "15000");

      System.setProperty("prepRecoveryReadTimeoutExtraWait", "100");
      System.setProperty("validateAfterInactivity", "-1");
      System.setProperty("leaderVoteWait", "2500"); // this is also apparently controlling how long we wait for a leader on register nocommit
      System.setProperty("leaderConflictResolveWait", "10000");

      System.setProperty("solr.recovery.recoveryThrottle", "500");
      System.setProperty("solr.recovery.leaderThrottle", "100");

      System.setProperty("bucketVersionLockTimeoutMs", "8000");
      System.setProperty("socketTimeout", "30000");
      System.setProperty("connTimeout", "10000");
      System.setProperty("solr.cloud.wait-for-updates-with-stale-state-pause", "0");
      System.setProperty("solr.cloud.starting-recovery-delay-milli-seconds", "500");

      System.setProperty("solr.waitForState", "10"); // secs

      System.setProperty("solr.default.collection_op_timeout", "10000");


      System.setProperty("solr.httpclient.retries", "1");
      System.setProperty("solr.retries.on.forward", "1");
      System.setProperty("solr.retries.to.followers", "1");

      SolrTestCaseJ4.useFactory("org.apache.solr.core.RAMDirectoryFactory");
      System.setProperty("solr.lock.type", "single");
      System.setProperty("solr.tests.lockType", "single");

      System.setProperty("solr.tests.mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
      System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
      System.setProperty("solr.mscheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");

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
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link Before} method
   * @lucene.internal
   */
//  @Before
//  public void checkSyspropForceBeforeAssumptionFailure() {
//    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
//    final String PROP = "tests.force.assumption.failure.before";
//    assumeFalse(PROP + " == true",
//                systemPropertyAsBoolean(PROP, false));
//  }

  public static String TEST_HOME() {
    return getFile("solr/collection1").getParent();
  }

  public static Path TEST_PATH() { return getFile("solr/collection1").getParentFile().toPath(); }

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
        ParWork.propegateInterrupt(e);
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

  @AfterClass
  public static void afterSolrTestCase() throws Exception {
    log.info("*******************************************************************");
    log.info("@After Class ------------------------------------------------------");
    try {

      SolrQueuedThreadPool fqtp = qtp;
      if (fqtp != null) {
        fqtp.close();
        qtp = null;
      }

      SysStats.getSysStats().stopMonitor();

      ParWork.closeMyPerThreadExecutor(true);
      ParWork.shutdownRootSharedExec();

      if (!failed && suiteFailureMarker.wasSuccessful() ) {
        String object = null;
        // if the tests passed, make sure everything was closed / released
        if (RandomizedTest.getContext().getTargetClass().isAnnotationPresent(SuppressObjectReleaseTracker.class)) {
          SuppressObjectReleaseTracker sor = RandomizedTest.getContext().getTargetClass()
              .getAnnotation(SuppressObjectReleaseTracker.class);
           object = sor.object();
        }

        String orr = ObjectReleaseTracker.checkEmpty(object);
        ObjectReleaseTracker.clear();
        assertNull(orr, orr);
      }
    } finally {
      ObjectReleaseTracker.clear();
      TestInjection.reset();
    }
    try {
      HttpClientUtil.resetHttpClientBuilder();
      Http2SolrClient.resetSslContextFactory();
      TestInjection.reset();

      long testTime = TimeUnit.SECONDS.convert(System.nanoTime() - testStartTime, TimeUnit.NANOSECONDS);
      if (!failed && !TEST_NIGHTLY && testTime > SOLR_TEST_TIMEOUT) {
        log.error("This test suite is too long for non @Nightly runs! Please improve it's performance, break it up, make parts of it @Nightly or make the whole suite @Nightly: "
                + testTime);
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
        // nocommit - leave this on
        if (!TEST_NIGHTLY) fail("A " + clazz.getName() + " took too long to close: " + tooLongTime + "\n" + times);
      }
    }
    log.info("@AfterClass end ------------------------------------------------------");
    log.info("*******************************************************************");

    StartupLoggingUtils.shutdown();

    checkForInterruptRequest();
  }

  private static SSLTestConfig buildSSLConfig() {

    if (!TEST_NIGHTLY) {
      return new SSLTestConfig();
    }

    RandomizeSSL.SSLRandomizer sslRandomizer =
            RandomizeSSL.SSLRandomizer.getSSLRandomizerForClass(RandomizedContext.current().getTargetClass());

    if (Constants.MAC_OS_X) {
      // see SOLR-9039
      // If a solution is found to remove this, please make sure to also update
      // TestMiniSolrCloudClusterSSL.testSslAndClientAuth as well.
      sslRandomizer = new RandomizeSSL.SSLRandomizer(sslRandomizer.ssl, 0.0D, (sslRandomizer.debug + " w/ MAC_OS_X supressed clientAuth"));
    }

    SSLTestConfig result = sslRandomizer.createSSLTestConfig();
    if (log.isInfoEnabled()) {
      log.info("Randomized ssl ({}) and clientAuth ({}) via: {}",
              result.isSSLMode(), result.isClientAuthMode(), sslRandomizer.debug);
    }
    return result;
  }

  private static void checkForInterruptRequest() {
    try {
      String interruptThread = interuptThreadWithNameContains;

        interruptThreadsOnTearDown(interruptThread, true);
        interuptThreadWithNameContains = null;

    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      log.error("", e);
    }
  }


  // expert - for special cases
  public static void interruptThreadsOnTearDown(String nameContains, boolean now) {
    if (!now) {
      interuptThreadWithNameContains = nameContains;
      return;
    }

   // System.out.println("DO FORCED INTTERUPTS");
    //  we need to filter and only do this for known threads? dont want users to count on this behavior unless necessary
    String testThread = Thread.currentThread().getName();
   // System.out.println("test thread:" + testThread);
    ThreadGroup tg = Thread.currentThread().getThreadGroup();
  //  System.out.println("test group:" + tg.getName());
    Set<Map.Entry<Thread,StackTraceElement[]>> threadSet = Thread.getAllStackTraces().entrySet();
  //  System.out.println("thread count: " + threadSet.size());
    for (Map.Entry<Thread,StackTraceElement[]> threadEntry : threadSet) {
      Thread thread = threadEntry.getKey();
      ThreadGroup threadGroup = thread.getThreadGroup();
      if (threadGroup != null) {
    //    System.out.println("thread is " + thread.getName());
        if (threadGroup.getName().equals(tg.getName()) && !thread.getName().startsWith("SUITE")) {
          interrupt(thread, nameContains);
          continue;
        }
      }

      while (threadGroup != null && threadGroup.getParent() != null && !thread.getName().startsWith("SUITE")) {
        threadGroup = threadGroup.getParent();
        if (thread.getState().equals(Thread.State.TERMINATED) || nameContains != null && threadGroup.getName().equals(tg.getName())) {
        //  System.out.println("thread is " + thread.getName());
          interrupt(thread, nameContains);
          continue;
        }
      }
    }
  }


  public static Path configset(String name) {
    return TEST_PATH().resolve("configsets").resolve(name).resolve("conf");
  }

  private static void interrupt(Thread thread, String nameContains) {
    if ((nameContains != null && thread.getName().contains(nameContains)) || (interuptThreadWithNameContains != null && thread.getName().contains(interuptThreadWithNameContains)) ) {
      if (thread.getState() == Thread.State.TERMINATED || thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.BLOCKED || thread.getState() == Thread.State.RUNNABLE) { // adding RUNNABLE, BLOCKED, WAITING is not ideal, but we can be in
        // processWorkerExit in this state - ideally we would check also we are in an exit or terminate method if !TERMINATED
        log.warn("interrupt on " + thread.getName());
        thread.interrupt();
        try {
          thread.join(250);
        } catch (InterruptedException e) {
          ParWork.propegateInterrupt(e);
        }
      } else {
        log.info("skipping interrupt due to state:" + thread.getState());
      }
    }
  }

  public static SolrQueuedThreadPool getQtp() {

    SolrQueuedThreadPool qtp = new SolrQueuedThreadPool("solr-test-qtp");;
    return qtp;
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
    String testName = getTestName();
    int index = testName.indexOf(' ');
    if (index > 0) {
      testName = testName.substring(0, index);
    }
    return testName;
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

  protected static <T> T pickRandom(T... options) {
    return options[random().nextInt(options.length)];
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

  private static class SolrTestWatcher extends TestWatcher {
    @Override
    protected void failed(Throwable e, Description description) {
      failed = true;
    }

    @Override
    protected void succeeded(Description description) {
    }
  }
}
