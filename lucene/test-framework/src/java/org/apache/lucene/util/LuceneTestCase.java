package org.apache.lucene.util;

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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AlcoholicMergePolicy;
import org.apache.lucene.index.AssertingAtomicReader;
import org.apache.lucene.index.AssertingDirectoryReader;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldFilterAtomicReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MockRandomMergePolicy;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ParallelAtomicReader;
import org.apache.lucene.index.ParallelCompositeReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils.FCInvisibleMultiReader;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.store.RateLimitedDirectoryWrapper;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup.Group;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.rules.NoClassHooksShadowingRule;
import com.carrotsearch.randomizedtesting.rules.NoInstanceHooksOverridesRule;
import com.carrotsearch.randomizedtesting.rules.StaticFieldsInvariantRule;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesInvariantRule;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsInt;

/**
 * Base class for all Lucene unit tests, Junit3 or Junit4 variant.
 * 
 * <h3>Class and instance setup.</h3>
 * 
 * <p>
 * The preferred way to specify class (suite-level) setup/cleanup is to use
 * static methods annotated with {@link BeforeClass} and {@link AfterClass}. Any
 * code in these methods is executed within the test framework's control and
 * ensure proper setup has been made. <b>Try not to use static initializers
 * (including complex final field initializers).</b> Static initializers are
 * executed before any setup rules are fired and may cause you (or somebody 
 * else) headaches.
 * 
 * <p>
 * For instance-level setup, use {@link Before} and {@link After} annotated
 * methods. If you override either {@link #setUp()} or {@link #tearDown()} in
 * your subclass, make sure you call <code>super.setUp()</code> and
 * <code>super.tearDown()</code>. This is detected and enforced.
 * 
 * <h3>Specifying test cases</h3>
 * 
 * <p>
 * Any test method with a <code>testXXX</code> prefix is considered a test case.
 * Any test method annotated with {@link Test} is considered a test case.
 * 
 * <h3>Randomized execution and test facilities</h3>
 * 
 * <p>
 * {@link LuceneTestCase} uses {@link RandomizedRunner} to execute test cases.
 * {@link RandomizedRunner} has built-in support for tests randomization
 * including access to a repeatable {@link Random} instance. See
 * {@link #random()} method. Any test using {@link Random} acquired from
 * {@link #random()} should be fully reproducible (assuming no race conditions
 * between threads etc.). The initial seed for a test case is reported in many
 * ways:
 * <ul>
 *   <li>as part of any exception thrown from its body (inserted as a dummy stack
 *   trace entry),</li>
 *   <li>as part of the main thread executing the test case (if your test hangs,
 *   just dump the stack trace of all threads and you'll see the seed),</li>
 *   <li>the master seed can also be accessed manually by getting the current
 *   context ({@link RandomizedContext#current()}) and then calling
 *   {@link RandomizedContext#getRunnerSeedAsString()}.</li>
 * </ul>
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({
  LuceneJUnit3MethodProvider.class,
  JUnit4MethodProvider.class
})
@Listeners({
  RunListenerPrintReproduceInfo.class,
  FailureMarker.class
})
@SeedDecorators({MixWithSuiteName.class}) // See LUCENE-3995 for rationale.
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakGroup(Group.MAIN)
@ThreadLeakAction({Action.WARN, Action.INTERRUPT})
@ThreadLeakLingering(linger = 20000) // Wait long for leaked threads to complete before failure. zk needs this.
@ThreadLeakZombies(Consequence.IGNORE_REMAINING_TESTS)
@TimeoutSuite(millis = 2 * TimeUnits.HOUR)
@ThreadLeakFilters(defaultFilters = true, filters = {
    QuickPatchThreadsFilter.class
})
@TestRuleLimitSysouts.Limit(bytes = TestRuleLimitSysouts.DEFAULT_SYSOUT_BYTES_THRESHOLD)
public abstract class LuceneTestCase extends Assert {

  // --------------------------------------------------------------------
  // Test groups, system properties and other annotations modifying tests
  // --------------------------------------------------------------------

  public static final String SYSPROP_NIGHTLY = "tests.nightly";
  public static final String SYSPROP_WEEKLY = "tests.weekly";
  public static final String SYSPROP_MONSTER = "tests.monster";
  public static final String SYSPROP_AWAITSFIX = "tests.awaitsfix";
  public static final String SYSPROP_SLOW = "tests.slow";
  public static final String SYSPROP_BADAPPLES = "tests.badapples";

  /** @see #ignoreAfterMaxFailures*/
  public static final String SYSPROP_MAXFAILURES = "tests.maxfailures";

  /** @see #ignoreAfterMaxFailures*/
  public static final String SYSPROP_FAILFAST = "tests.failfast";

  /**
   * Annotation for tests that should only be run during nightly builds.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_NIGHTLY)
  public @interface Nightly {}

  /**
   * Annotation for tests that should only be run during weekly builds
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_WEEKLY)
  public @interface Weekly {}
  
  /**
   * Annotation for monster tests that require special setup (e.g. use tons of disk and RAM)
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_MONSTER)
  public @interface Monster {
    String value();
  }

  /**
   * Annotation for tests which exhibit a known issue and are temporarily disabled.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_AWAITSFIX)
  public @interface AwaitsFix {
    /** Point to JIRA entry. */
    public String bugUrl();
  }

  /**
   * Annotation for tests that are slow. Slow tests do run by default but can be
   * disabled if a quick run is needed.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = true, sysProperty = SYSPROP_SLOW)
  public @interface Slow {}

  /**
   * Annotation for tests that fail frequently and should
   * be moved to a <a href="https://builds.apache.org/job/Lucene-BadApples-trunk-java7/">"vault" plan in Jenkins</a>.
   *
   * Tests annotated with this will be turned off by default. If you want to enable
   * them, set:
   * <pre>
   * -Dtests.badapples=true
   * </pre>
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = SYSPROP_BADAPPLES)
  public @interface BadApple {
    /** Point to JIRA entry. */
    public String bugUrl();
  }

  /**
   * Annotation for test classes that should avoid certain codec types
   * (because they are expensive, for example).
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressCodecs {
    String[] value();
  }
  
  /**
   * Marks any suites which are known not to close all the temporary
   * files. This may prevent temp. files and folders from being cleaned
   * up after the suite is completed.
   * 
   * @see LuceneTestCase#createTempDir()
   * @see LuceneTestCase#createTempFile(String, String)
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressTempFileChecks {
    /** Point to JIRA entry. */
    public String bugUrl() default "None";
  }

  /**
   * Ignore {@link TestRuleLimitSysouts} for any suite which is known to print 
   * over the default limit of bytes to {@link System#out} or {@link System#err}.
   * 
   * @see TestRuleLimitSysouts
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressSysoutChecks {
    /** Point to JIRA entry. */
    public String bugUrl();
  }

  // -----------------------------------------------------------------
  // Truly immutable fields and constants, initialized once and valid 
  // for all suites ever since.
  // -----------------------------------------------------------------

  // :Post-Release-Update-Version.LUCENE_XY:
  /** 
   * Use this constant when creating Analyzers and any other version-dependent stuff.
   * <p><b>NOTE:</b> Change this when development starts for new Lucene version:
   */
  public static final Version TEST_VERSION_CURRENT = Version.LUCENE_4_10_0;

  /**
   * True if and only if tests are run in verbose mode. If this flag is false
   * tests are not expected to print any messages. Enforced with {@link TestRuleLimitSysouts}.
   */
  public static final boolean VERBOSE = systemPropertyAsBoolean("tests.verbose", false);

  /**
   * Enables or disables dumping of {@link InfoStream} messages. 
   */
  public static final boolean INFOSTREAM = systemPropertyAsBoolean("tests.infostream", VERBOSE);

  /**
   * A random multiplier which you should use when writing random tests:
   * multiply it by the number of iterations to scale your tests (for nightly builds).
   */
  public static final int RANDOM_MULTIPLIER = systemPropertyAsInt("tests.multiplier", 1);

  /** TODO: javadoc? */
  public static final String DEFAULT_LINE_DOCS_FILE = "europarl.lines.txt.gz";

  /** TODO: javadoc? */
  public static final String JENKINS_LARGE_LINE_DOCS_FILE = "enwiki.random.lines.txt";

  /** Gets the codec to run tests with. */
  public static final String TEST_CODEC = System.getProperty("tests.codec", "random");

  /** Gets the postingsFormat to run tests with. */
  public static final String TEST_POSTINGSFORMAT = System.getProperty("tests.postingsformat", "random");
  
  /** Gets the docValuesFormat to run tests with */
  public static final String TEST_DOCVALUESFORMAT = System.getProperty("tests.docvaluesformat", "random");

  /** Gets the directory to run tests with */
  public static final String TEST_DIRECTORY = System.getProperty("tests.directory", "random");

  /** the line file used by LineFileDocs */
  public static final String TEST_LINE_DOCS_FILE = System.getProperty("tests.linedocsfile", DEFAULT_LINE_DOCS_FILE);

  /** Whether or not {@link Nightly} tests should run. */
  public static final boolean TEST_NIGHTLY = systemPropertyAsBoolean(SYSPROP_NIGHTLY, false);

  /** Whether or not {@link Weekly} tests should run. */
  public static final boolean TEST_WEEKLY = systemPropertyAsBoolean(SYSPROP_WEEKLY, false);
  
  /** Whether or not {@link AwaitsFix} tests should run. */
  public static final boolean TEST_AWAITSFIX = systemPropertyAsBoolean(SYSPROP_AWAITSFIX, false);

  /** Whether or not {@link Slow} tests should run. */
  public static final boolean TEST_SLOW = systemPropertyAsBoolean(SYSPROP_SLOW, false);

  /** Throttling, see {@link MockDirectoryWrapper#setThrottling(Throttling)}. */
  public static final Throttling TEST_THROTTLING = TEST_NIGHTLY ? Throttling.SOMETIMES : Throttling.NEVER;

  /** Leave temporary files on disk, even on successful runs. */
  public static final boolean LEAVE_TEMPORARY;
  static {
    boolean defaultValue = false;
    for (String property : Arrays.asList(
        "tests.leaveTemporary" /* ANT tasks's (junit4) flag. */,
        "tests.leavetemporary" /* lowercase */,
        "tests.leavetmpdir" /* default */,
        "solr.test.leavetmpdir" /* Solr's legacy */)) {
      defaultValue |= systemPropertyAsBoolean(property, false);
    }
    LEAVE_TEMPORARY = defaultValue;
  }

  /**
   * These property keys will be ignored in verification of altered properties.
   * @see SystemPropertiesInvariantRule
   * @see #ruleChain
   * @see #classRules
   */
  private static final String [] IGNORED_INVARIANT_PROPERTIES = {
    "user.timezone", "java.rmi.server.randomIDs"
  };

  /** Filesystem-based {@link Directory} implementations. */
  private static final List<String> FS_DIRECTORIES = Arrays.asList(
    "SimpleFSDirectory",
    "NIOFSDirectory",
    "MMapDirectory"
  );

  /** All {@link Directory} implementations. */
  private static final List<String> CORE_DIRECTORIES;
  static {
    CORE_DIRECTORIES = new ArrayList<>(FS_DIRECTORIES);
    CORE_DIRECTORIES.add("RAMDirectory");
  };
  
  protected static final Set<String> doesntSupportOffsets = new HashSet<>(Arrays.asList(
    "Lucene3x",
    "MockFixedIntBlock",
    "MockVariableIntBlock",
    "MockSep",
    "MockRandom"
  ));
  
  // -----------------------------------------------------------------
  // Fields initialized in class or instance rules.
  // -----------------------------------------------------------------

  /**
   * When {@code true}, Codecs for old Lucene version will support writing
   * indexes in that format. Defaults to {@code false}, can be disabled by
   * specific tests on demand.
   * 
   * @lucene.internal
   */
  public static boolean OLD_FORMAT_IMPERSONATION_IS_ACTIVE = false;

  // -----------------------------------------------------------------
  // Class level (suite) rules.
  // -----------------------------------------------------------------
  
  /**
   * Stores the currently class under test.
   */
  private static final TestRuleStoreClassName classNameRule; 

  /**
   * Class environment setup rule.
   */
  static final TestRuleSetupAndRestoreClassEnv classEnvRule;

  /**
   * Suite failure marker (any error in the test or suite scope).
   */
  private static TestRuleMarkFailure suiteFailureMarker;

  /**
   * Ignore tests after hitting a designated number of initial failures. This
   * is truly a "static" global singleton since it needs to span the lifetime of all
   * test classes running inside this JVM (it cannot be part of a class rule).
   * 
   * <p>This poses some problems for the test framework's tests because these sometimes
   * trigger intentional failures which add up to the global count. This field contains
   * a (possibly) changing reference to {@link TestRuleIgnoreAfterMaxFailures} and we
   * dispatch to its current value from the {@link #classRules} chain using {@link TestRuleDelegate}.  
   */
  private static final AtomicReference<TestRuleIgnoreAfterMaxFailures> ignoreAfterMaxFailuresDelegate;
  private static final TestRule ignoreAfterMaxFailures;
  static {
    int maxFailures = systemPropertyAsInt(SYSPROP_MAXFAILURES, Integer.MAX_VALUE);
    boolean failFast = systemPropertyAsBoolean(SYSPROP_FAILFAST, false);

    if (failFast) {
      if (maxFailures == Integer.MAX_VALUE) {
        maxFailures = 1;
      } else {
        Logger.getLogger(LuceneTestCase.class.getSimpleName()).warning(
            "Property '" + SYSPROP_MAXFAILURES + "'=" + maxFailures + ", 'failfast' is" +
            " ignored.");
      }
    }

    ignoreAfterMaxFailuresDelegate = 
        new AtomicReference<>(
            new TestRuleIgnoreAfterMaxFailures(maxFailures));
    ignoreAfterMaxFailures = TestRuleDelegate.of(ignoreAfterMaxFailuresDelegate);
  }
  
  /**
   * Try to capture streams early so that other classes don't have a chance to steal references
   * to them (as is the case with ju.logging handlers).
   */
  static {
    TestRuleLimitSysouts.checkCaptureStreams();
    Logger.getGlobal().getHandlers();
  }

  /**
   * Temporarily substitute the global {@link TestRuleIgnoreAfterMaxFailures}. See
   * {@link #ignoreAfterMaxFailuresDelegate} for some explanation why this method 
   * is needed.
   */
  public static TestRuleIgnoreAfterMaxFailures replaceMaxFailureRule(TestRuleIgnoreAfterMaxFailures newValue) {
    return ignoreAfterMaxFailuresDelegate.getAndSet(newValue);
  }

  /**
   * Max 10mb of static data stored in a test suite class after the suite is complete.
   * Prevents static data structures leaking and causing OOMs in subsequent tests.
   */
  private final static long STATIC_LEAK_THRESHOLD = 10 * 1024 * 1024;

  /** By-name list of ignored types like loggers etc. */
  private final static Set<String> STATIC_LEAK_IGNORED_TYPES = 
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "org.slf4j.Logger",
      "org.apache.solr.SolrLogFormatter",
      EnumSet.class.getName())));

  /**
   * This controls how suite-level rules are nested. It is important that _all_ rules declared
   * in {@link LuceneTestCase} are executed in proper order if they depend on each 
   * other.
   */
  @ClassRule
  public static TestRule classRules = RuleChain
    .outerRule(new TestRuleIgnoreTestSuites())
    .around(ignoreAfterMaxFailures)
    .around(suiteFailureMarker = new TestRuleMarkFailure())
    .around(new TestRuleAssertionsRequired())
    .around(new TestRuleLimitSysouts(suiteFailureMarker))
    .around(new TemporaryFilesCleanupRule())
    .around(new StaticFieldsInvariantRule(STATIC_LEAK_THRESHOLD, true) {
      @Override
      protected boolean accept(java.lang.reflect.Field field) {
        // Don't count known classes that consume memory once.
        if (STATIC_LEAK_IGNORED_TYPES.contains(field.getType().getName())) {
          return false;
        }
        // Don't count references from ourselves, we're top-level.
        if (field.getDeclaringClass() == LuceneTestCase.class) {
          return false;
        }
        return super.accept(field);
      }
    })
    .around(new NoClassHooksShadowingRule())
    .around(new NoInstanceHooksOverridesRule() {
      @Override
      protected boolean verify(Method key) {
        String name = key.getName();
        return !(name.equals("setUp") || name.equals("tearDown"));
      }
    })
    .around(new SystemPropertiesInvariantRule(IGNORED_INVARIANT_PROPERTIES))
    .around(classNameRule = new TestRuleStoreClassName())
    .around(classEnvRule = new TestRuleSetupAndRestoreClassEnv());


  // -----------------------------------------------------------------
  // Test level rules.
  // -----------------------------------------------------------------

  /** Enforces {@link #setUp()} and {@link #tearDown()} calls are chained. */
  private TestRuleSetupTeardownChained parentChainCallRule = new TestRuleSetupTeardownChained();

  /** Save test thread and name. */
  private TestRuleThreadAndTestName threadAndTestNameRule = new TestRuleThreadAndTestName();

  /** Taint suite result with individual test failures. */
  private TestRuleMarkFailure testFailureMarker = new TestRuleMarkFailure(suiteFailureMarker); 
  
  /**
   * This controls how individual test rules are nested. It is important that
   * _all_ rules declared in {@link LuceneTestCase} are executed in proper order
   * if they depend on each other.
   */
  @Rule
  public final TestRule ruleChain = RuleChain
    .outerRule(testFailureMarker)
    .around(ignoreAfterMaxFailures)
    .around(threadAndTestNameRule)
    .around(new SystemPropertiesInvariantRule(IGNORED_INVARIANT_PROPERTIES))
    .around(new TestRuleSetupAndRestoreInstanceEnv())
    .around(new TestRuleFieldCacheSanity())
    .around(parentChainCallRule);

  private static final Map<String,FieldType> fieldToType = new HashMap<String,FieldType>();

  enum LiveIWCFlushMode {BY_RAM, BY_DOCS, EITHER};

  /** Set by TestRuleSetupAndRestoreClassEnv */
  static LiveIWCFlushMode liveIWCFlushMode;

  static void setLiveIWCFlushMode(LiveIWCFlushMode flushMode) {
    liveIWCFlushMode = flushMode;
  }

  // -----------------------------------------------------------------
  // Suite and test case setup/ cleanup.
  // -----------------------------------------------------------------

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
    fieldToType.clear();

    // Test is supposed to call this itself, but we do this defensively in case it forgot:
    restoreIndexWriterMaxDocs();
  }

  /** Tells {@link IndexWriter} to enforce the specified limit as the maximum number of documents in one index; call
   *  {@link #restoreIndexWriterMaxDocs} once your test is done. */
  public void setIndexWriterMaxDocs(int limit) {
    Method m;
    try {
      m = IndexWriter.class.getDeclaredMethod("setMaxDocs", int.class);
    } catch (NoSuchMethodException nsme) {
      throw new RuntimeException(nsme);
    }
    m.setAccessible(true);
    try {
      m.invoke(IndexWriter.class, limit);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    } catch (InvocationTargetException ite) {
      throw new RuntimeException(ite);
    }
  }

  /** Returns the default {@link IndexWriter#MAX_DOCS} limit. */
  public void restoreIndexWriterMaxDocs() {
    setIndexWriterMaxDocs(IndexWriter.MAX_DOCS);
  }

  // -----------------------------------------------------------------
  // Test facilities and facades for subclasses. 
  // -----------------------------------------------------------------

  /**
   * Access to the current {@link RandomizedContext}'s Random instance. It is safe to use
   * this method from multiple threads, etc., but it should be called while within a runner's
   * scope (so no static initializers). The returned {@link Random} instance will be 
   * <b>different</b> when this method is called inside a {@link BeforeClass} hook (static 
   * suite scope) and within {@link Before}/ {@link After} hooks or test methods. 
   * 
   * <p>The returned instance must not be shared with other threads or cross a single scope's 
   * boundary. For example, a {@link Random} acquired within a test method shouldn't be reused
   * for another test case.
   * 
   * <p>There is an overhead connected with getting the {@link Random} for a particular context
   * and thread. It is better to cache the {@link Random} locally if tight loops with multiple
   * invocations are present or create a derivative local {@link Random} for millions of calls 
   * like this:
   * <pre>
   * Random random = new Random(random().nextLong());
   * // tight loop with many invocations. 
   * </pre>
   */
  public static Random random() {
    return RandomizedContext.current().getRandom();
  }

  /**
   * Registers a {@link Closeable} resource that should be closed after the test
   * completes.
   * 
   * @return <code>resource</code> (for call chaining).
   */
  public <T extends Closeable> T closeAfterTest(T resource) {
    return RandomizedContext.current().closeAtEnd(resource, LifecycleScope.TEST);
  }

  /**
   * Registers a {@link Closeable} resource that should be closed after the suite
   * completes.
   * 
   * @return <code>resource</code> (for call chaining).
   */
  public static <T extends Closeable> T closeAfterSuite(T resource) {
    return RandomizedContext.current().closeAtEnd(resource, LifecycleScope.SUITE);
  }

  /**
   * Return the current class being tested.
   */
  public static Class<?> getTestClass() {
    return classNameRule.getTestClass();
  }

  /**
   * Return the name of the currently executing test case.
   */
  public String getTestName() {
    return threadAndTestNameRule.testMethodName;
  }

  /**
   * Some tests expect the directory to contain a single segment, and want to 
   * do tests on that segment's reader. This is an utility method to help them.
   */
  public static SegmentReader getOnlySegmentReader(DirectoryReader reader) {
    List<AtomicReaderContext> subReaders = reader.leaves();
    if (subReaders.size() != 1)
      throw new IllegalArgumentException(reader + " has " + subReaders.size() + " segments instead of exactly one");
    final AtomicReader r = subReaders.get(0).reader();
    assertTrue(r instanceof SegmentReader);
    return (SegmentReader) r;
  }

  /**
   * Returns true if and only if the calling thread is the primary thread 
   * executing the test case. 
   */
  protected boolean isTestThread() {
    assertNotNull("Test case thread not set?", threadAndTestNameRule.testCaseThread);
    return Thread.currentThread() == threadAndTestNameRule.testCaseThread;
  }

  /**
   * Asserts that FieldCacheSanityChecker does not detect any
   * problems with FieldCache.DEFAULT.
   * <p>
   * If any problems are found, they are logged to System.err
   * (allong with the msg) when the Assertion is thrown.
   * </p>
   * <p>
   * This method is called by tearDown after every test method,
   * however IndexReaders scoped inside test methods may be garbage
   * collected prior to this method being called, causing errors to
   * be overlooked. Tests are encouraged to keep their IndexReaders
   * scoped at the class level, or to explicitly call this method
   * directly in the same scope as the IndexReader.
   * </p>
   *
   * @see org.apache.lucene.util.FieldCacheSanityChecker
   */
  protected static void assertSaneFieldCaches(final String msg) {
    final CacheEntry[] entries = FieldCache.DEFAULT.getCacheEntries();
    Insanity[] insanity = null;
    try {
      try {
        insanity = FieldCacheSanityChecker.checkSanity(entries);
      } catch (RuntimeException e) {
        dumpArray(msg + ": FieldCache", entries, System.err);
        throw e;
      }

      assertEquals(msg + ": Insane FieldCache usage(s) found",
                   0, insanity.length);
      insanity = null;
    } finally {

      // report this in the event of any exception/failure
      // if no failure, then insanity will be null anyway
      if (null != insanity) {
        dumpArray(msg + ": Insane FieldCache usage(s)", insanity, System.err);
      }
    }
  }

  /**
   * Returns a number of at least <code>i</code>
   * <p>
   * The actual number returned will be influenced by whether {@link #TEST_NIGHTLY}
   * is active and {@link #RANDOM_MULTIPLIER}, but also with some random fudge.
   */
  public static int atLeast(Random random, int i) {
    int min = (TEST_NIGHTLY ? 2*i : i) * RANDOM_MULTIPLIER;
    int max = min+(min/2);
    return TestUtil.nextInt(random, min, max);
  }
  
  public static int atLeast(int i) {
    return atLeast(random(), i);
  }
  
  /**
   * Returns true if something should happen rarely,
   * <p>
   * The actual number returned will be influenced by whether {@link #TEST_NIGHTLY}
   * is active and {@link #RANDOM_MULTIPLIER}.
   */
  public static boolean rarely(Random random) {
    int p = TEST_NIGHTLY ? 10 : 1;
    p += (p * Math.log(RANDOM_MULTIPLIER));
    int min = 100 - Math.min(p, 50); // never more than 50
    return random.nextInt(100) >= min;
  }
  
  public static boolean rarely() {
    return rarely(random());
  }
  
  public static boolean usually(Random random) {
    return !rarely(random);
  }
  
  public static boolean usually() {
    return usually(random());
  }

  public static void assumeTrue(String msg, boolean condition) {
    RandomizedTest.assumeTrue(msg, condition);
  }

  public static void assumeFalse(String msg, boolean condition) {
    RandomizedTest.assumeFalse(msg, condition);
  }

  public static void assumeNoException(String msg, Exception e) {
    RandomizedTest.assumeNoException(msg, e);
  }

  /**
   * Return <code>args</code> as a {@link Set} instance. The order of elements is not
   * preserved in iterators.
   */
  @SafeVarargs @SuppressWarnings("varargs")
  public static <T> Set<T> asSet(T... args) {
    return new HashSet<>(Arrays.asList(args));
  }

  /**
   * Convenience method for logging an iterator.
   *
   * @param label  String logged before/after the items in the iterator
   * @param iter   Each next() is toString()ed and logged on it's own line. If iter is null this is logged differnetly then an empty iterator.
   * @param stream Stream to log messages to.
   */
  public static void dumpIterator(String label, Iterator<?> iter,
                                  PrintStream stream) {
    stream.println("*** BEGIN " + label + " ***");
    if (null == iter) {
      stream.println(" ... NULL ...");
    } else {
      while (iter.hasNext()) {
        stream.println(iter.next().toString());
      }
    }
    stream.println("*** END " + label + " ***");
  }

  /**
   * Convenience method for logging an array.  Wraps the array in an iterator and delegates
   *
   * @see #dumpIterator(String,Iterator,PrintStream)
   */
  public static void dumpArray(String label, Object[] objs,
                               PrintStream stream) {
    Iterator<?> iter = (null == objs) ? null : Arrays.asList(objs).iterator();
    dumpIterator(label, iter, stream);
  }

  /** create a new index writer config with random defaults */
  public static IndexWriterConfig newIndexWriterConfig(Analyzer a) {
    return newIndexWriterConfig(random(), TEST_VERSION_CURRENT, a);
  }
  
  /** create a new index writer config with random defaults using the specified random */
  public static IndexWriterConfig newIndexWriterConfig(Random r, Version v, Analyzer a) {
    IndexWriterConfig c = new IndexWriterConfig(v, a);
    c.setSimilarity(classEnvRule.similarity);
    if (VERBOSE) {
      // Even though TestRuleSetupAndRestoreClassEnv calls
      // InfoStream.setDefault, we do it again here so that
      // the PrintStreamInfoStream.messageID increments so
      // that when there are separate instances of
      // IndexWriter created we see "IW 0", "IW 1", "IW 2",
      // ... instead of just always "IW 0":
      c.setInfoStream(new TestRuleSetupAndRestoreClassEnv.ThreadNameFixingPrintStreamInfoStream(System.out));
    }

    if (r.nextBoolean()) {
      c.setMergeScheduler(new SerialMergeScheduler());
    } else if (rarely(r)) {
      int maxThreadCount = TestUtil.nextInt(r, 1, 4);
      int maxMergeCount = TestUtil.nextInt(r, maxThreadCount, maxThreadCount+4);
      ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
      cms.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
      c.setMergeScheduler(cms);
    }
    if (r.nextBoolean()) {
      if (rarely(r)) {
        // crazy value
        c.setMaxBufferedDocs(TestUtil.nextInt(r, 2, 15));
      } else {
        // reasonable value
        c.setMaxBufferedDocs(TestUtil.nextInt(r, 16, 1000));
      }
    }
    if (r.nextBoolean()) {
      if (rarely(r)) {
        // crazy value
        c.setTermIndexInterval(r.nextBoolean() ? TestUtil.nextInt(r, 1, 31) : TestUtil.nextInt(r, 129, 1000));
      } else {
        // reasonable value
        c.setTermIndexInterval(TestUtil.nextInt(r, 32, 128));
      }
    }
    if (r.nextBoolean()) {
      int maxNumThreadStates = rarely(r) ? TestUtil.nextInt(r, 5, 20) // crazy value
          : TestUtil.nextInt(r, 1, 4); // reasonable value

      c.setMaxThreadStates(maxNumThreadStates);
    }

    c.setMergePolicy(newMergePolicy(r));

    if (rarely(r)) {
      c.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(c.getInfoStream()));
    }
    c.setUseCompoundFile(r.nextBoolean());
    c.setReaderPooling(r.nextBoolean());
    c.setReaderTermsIndexDivisor(TestUtil.nextInt(r, 1, 4));
    c.setCheckIntegrityAtMerge(r.nextBoolean());
    return c;
  }

  public static MergePolicy newMergePolicy(Random r) {
    if (rarely(r)) {
      return new MockRandomMergePolicy(r);
    } else if (r.nextBoolean()) {
      return newTieredMergePolicy(r);
    } else if (r.nextInt(5) == 0) { 
      return newAlcoholicMergePolicy(r, classEnvRule.timeZone);
    }
    return newLogMergePolicy(r);
  }

  public static MergePolicy newMergePolicy() {
    return newMergePolicy(random());
  }

  public static LogMergePolicy newLogMergePolicy() {
    return newLogMergePolicy(random());
  }

  public static TieredMergePolicy newTieredMergePolicy() {
    return newTieredMergePolicy(random());
  }

  public static AlcoholicMergePolicy newAlcoholicMergePolicy() {
    return newAlcoholicMergePolicy(random(), classEnvRule.timeZone);
  }
  
  public static AlcoholicMergePolicy newAlcoholicMergePolicy(Random r, TimeZone tz) {
    return new AlcoholicMergePolicy(tz, new Random(r.nextLong()));
  }

  public static LogMergePolicy newLogMergePolicy(Random r) {
    LogMergePolicy logmp = r.nextBoolean() ? new LogDocMergePolicy() : new LogByteSizeMergePolicy();
    logmp.setCalibrateSizeByDeletes(r.nextBoolean());
    if (rarely(r)) {
      logmp.setMergeFactor(TestUtil.nextInt(r, 2, 9));
    } else {
      logmp.setMergeFactor(TestUtil.nextInt(r, 10, 50));
    }
    configureRandom(r, logmp);
    return logmp;
  }
  
  private static void configureRandom(Random r, MergePolicy mergePolicy) {
    if (r.nextBoolean()) {
      mergePolicy.setNoCFSRatio(0.1 + r.nextDouble()*0.8);
    } else {
      mergePolicy.setNoCFSRatio(r.nextBoolean() ? 1.0 : 0.0);
    }
    
    if (rarely(r)) {
      mergePolicy.setMaxCFSSegmentSizeMB(0.2 + r.nextDouble() * 2.0);
    } else {
      mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    }
  }

  public static TieredMergePolicy newTieredMergePolicy(Random r) {
    TieredMergePolicy tmp = new TieredMergePolicy();
    if (rarely(r)) {
      tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 2, 9));
      tmp.setMaxMergeAtOnceExplicit(TestUtil.nextInt(r, 2, 9));
    } else {
      tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 10, 50));
      tmp.setMaxMergeAtOnceExplicit(TestUtil.nextInt(r, 10, 50));
    }
    if (rarely(r)) {
      tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
    } else {
      tmp.setMaxMergedSegmentMB(r.nextDouble() * 100);
    }
    tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
    tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
    if (rarely(r)) {
      tmp.setSegmentsPerTier(TestUtil.nextInt(r, 2, 20));
    } else {
      tmp.setSegmentsPerTier(TestUtil.nextInt(r, 10, 50));
    }
    configureRandom(r, tmp);
    tmp.setReclaimDeletesWeight(r.nextDouble()*4);
    return tmp;
  }

  public static MergePolicy newLogMergePolicy(boolean useCFS) {
    MergePolicy logmp = newLogMergePolicy();
    logmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    return logmp;
  }

  public static MergePolicy newLogMergePolicy(boolean useCFS, int mergeFactor) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
    logmp.setMergeFactor(mergeFactor);
    return logmp;
  }

  public static MergePolicy newLogMergePolicy(int mergeFactor) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setMergeFactor(mergeFactor);
    return logmp;
  }
  
  // if you want it in LiveIndexWriterConfig: it must and will be tested here.
  public static void maybeChangeLiveIndexWriterConfig(Random r, LiveIndexWriterConfig c) {
    boolean didChange = false;

    if (rarely(r)) {
      // change flush parameters:
      // this is complicated because the api requires you "invoke setters in a magical order!"
      // LUCENE-5661: workaround for race conditions in the API
      synchronized (c) {
        boolean flushByRAM;
        switch (liveIWCFlushMode) {
        case BY_RAM:
          flushByRAM = true;
          break;
        case BY_DOCS:
          flushByRAM = false;
          break;
        case EITHER:
          flushByRAM = r.nextBoolean();
          break;
        default:
          throw new AssertionError();
        }
        if (flushByRAM) { 
          c.setRAMBufferSizeMB(TestUtil.nextInt(r, 1, 10));
          c.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        } else {
          if (rarely(r)) {
            // crazy value
            c.setMaxBufferedDocs(TestUtil.nextInt(r, 2, 15));
          } else {
            // reasonable value
            c.setMaxBufferedDocs(TestUtil.nextInt(r, 16, 1000));
          }
          c.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        }
      }
      didChange = true;
    }
    
    if (rarely(r)) {
      // change buffered deletes parameters
      boolean limitBufferedDeletes = r.nextBoolean();
      if (limitBufferedDeletes) {
        c.setMaxBufferedDeleteTerms(TestUtil.nextInt(r, 1, 1000));
      } else {
        c.setMaxBufferedDeleteTerms(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      }
      didChange = true;
    }
    
    if (rarely(r)) {
      // change warmer parameters
      if (r.nextBoolean()) {
        c.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(c.getInfoStream()));
      } else {
        c.setMergedSegmentWarmer(null);
      }
      didChange = true;
    }
    
    if (rarely(r)) {
      // change CFS flush parameters
      c.setUseCompoundFile(r.nextBoolean());
      didChange = true;
    }
    
    if (rarely(r)) {
      // change merge integrity check parameters
      c.setCheckIntegrityAtMerge(r.nextBoolean());
      didChange = true;
    }
    
    if (rarely(r)) {
      // change CMS merge parameters
      MergeScheduler ms = c.getMergeScheduler();
      if (ms instanceof ConcurrentMergeScheduler) {
        int maxThreadCount = TestUtil.nextInt(r, 1, 4);
        int maxMergeCount = TestUtil.nextInt(r, maxThreadCount, maxThreadCount + 4);
        ((ConcurrentMergeScheduler)ms).setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
      }
      didChange = true;
    }
    
    if (rarely(r)) {
      MergePolicy mp = c.getMergePolicy();
      configureRandom(r, mp);
      if (mp instanceof LogMergePolicy) {
        LogMergePolicy logmp = (LogMergePolicy) mp;
        logmp.setCalibrateSizeByDeletes(r.nextBoolean());
        if (rarely(r)) {
          logmp.setMergeFactor(TestUtil.nextInt(r, 2, 9));
        } else {
          logmp.setMergeFactor(TestUtil.nextInt(r, 10, 50));
        }
      } else if (mp instanceof TieredMergePolicy) {
        TieredMergePolicy tmp = (TieredMergePolicy) mp;
        if (rarely(r)) {
          tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 2, 9));
          tmp.setMaxMergeAtOnceExplicit(TestUtil.nextInt(r, 2, 9));
        } else {
          tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 10, 50));
          tmp.setMaxMergeAtOnceExplicit(TestUtil.nextInt(r, 10, 50));
        }
        if (rarely(r)) {
          tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
        } else {
          tmp.setMaxMergedSegmentMB(r.nextDouble() * 100);
        }
        tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
        tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
        if (rarely(r)) {
          tmp.setSegmentsPerTier(TestUtil.nextInt(r, 2, 20));
        } else {
          tmp.setSegmentsPerTier(TestUtil.nextInt(r, 10, 50));
        }
        configureRandom(r, tmp);
        tmp.setReclaimDeletesWeight(r.nextDouble()*4);
      }
      didChange = true;
    }
    if (VERBOSE && didChange) {
      System.out.println("NOTE: LuceneTestCase: randomly changed IWC's live settings to:\n" + c);
    }
  }

  /**
   * Returns a new Directory instance. Use this when the test does not
   * care about the specific Directory implementation (most tests).
   * <p>
   * The Directory is wrapped with {@link BaseDirectoryWrapper}.
   * this means usually it will be picky, such as ensuring that you
   * properly close it and all open files in your test. It will emulate
   * some features of Windows, such as not allowing open files to be
   * overwritten.
   */
  public static BaseDirectoryWrapper newDirectory() {
    return newDirectory(random());
  }
  
  /**
   * Returns a new Directory instance, using the specified random.
   * See {@link #newDirectory()} for more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r) {
    return wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), rarely(r));
  }

  public static MockDirectoryWrapper newMockDirectory() {
    return newMockDirectory(random());
  }

  public static MockDirectoryWrapper newMockDirectory(Random r) {
    return (MockDirectoryWrapper) wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), false);
  }

  public static MockDirectoryWrapper newMockFSDirectory(File f) {
    return (MockDirectoryWrapper) newFSDirectory(f, null, false);
  }

  /**
   * Returns a new Directory instance, with contents copied from the
   * provided directory. See {@link #newDirectory()} for more
   * information.
   */
  public static BaseDirectoryWrapper newDirectory(Directory d) throws IOException {
    return newDirectory(random(), d);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static BaseDirectoryWrapper newFSDirectory(File f) {
    return newFSDirectory(f, null);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static BaseDirectoryWrapper newFSDirectory(File f, LockFactory lf) {
    return newFSDirectory(f, lf, rarely());
  }

  private static BaseDirectoryWrapper newFSDirectory(File f, LockFactory lf, boolean bare) {
    String fsdirClass = TEST_DIRECTORY;
    if (fsdirClass.equals("random")) {
      fsdirClass = RandomPicks.randomFrom(random(), FS_DIRECTORIES); 
    }

    Class<? extends FSDirectory> clazz;
    try {
      try {
        clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
      } catch (ClassCastException e) {
        // TEST_DIRECTORY is not a sub-class of FSDirectory, so draw one at random
        fsdirClass = RandomPicks.randomFrom(random(), FS_DIRECTORIES);
        clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
      }

      Directory fsdir = newFSDirectoryImpl(clazz, f);
      BaseDirectoryWrapper wrapped = wrapDirectory(random(), fsdir, bare);
      if (lf != null) {
        wrapped.setLockFactory(lf);
      }
      return wrapped;
    } catch (Exception e) {
      Rethrow.rethrow(e);
      throw null; // dummy to prevent compiler failure
    }
  }

  /**
   * Returns a new Directory instance, using the specified random
   * with contents copied from the provided directory. See 
   * {@link #newDirectory()} for more information.
   */
  public static BaseDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    for (String file : d.listAll()) {
     d.copy(impl, file, file, newIOContext(r));
    }
    return wrapDirectory(r, impl, rarely(r));
  }
  
  private static BaseDirectoryWrapper wrapDirectory(Random random, Directory directory, boolean bare) {
    if (rarely(random) && !bare) {
      directory = new NRTCachingDirectory(directory, random.nextDouble(), random.nextDouble());
    }
    
    if (rarely(random) && !bare) { 
      final double maxMBPerSec = 10 + 5*(random.nextDouble()-0.5);
      if (LuceneTestCase.VERBOSE) {
        System.out.println("LuceneTestCase: will rate limit output IndexOutput to " + maxMBPerSec + " MB/sec");
      }
      final RateLimitedDirectoryWrapper rateLimitedDirectoryWrapper = new RateLimitedDirectoryWrapper(directory);
      switch (random.nextInt(10)) {
        case 3: // sometimes rate limit on flush
          rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.FLUSH);
          break;
        case 2: // sometimes rate limit flush & merge
          rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.FLUSH);
          rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.MERGE);
          break;
        default:
          rateLimitedDirectoryWrapper.setMaxWriteMBPerSec(maxMBPerSec, Context.MERGE);
      }
      directory =  rateLimitedDirectoryWrapper;
      
    }

    if (bare) {
      BaseDirectoryWrapper base = new BaseDirectoryWrapper(directory);
      closeAfterSuite(new CloseableDirectory(base, suiteFailureMarker));
      return base;
    } else {
      MockDirectoryWrapper mock = new MockDirectoryWrapper(random, directory);
      
      mock.setThrottling(TEST_THROTTLING);
      closeAfterSuite(new CloseableDirectory(mock, suiteFailureMarker));
      return mock;
    }
  }
  
  public static Field newStringField(String name, String value, Store stored) {
    return newField(random(), name, value, stored == Store.YES ? StringField.TYPE_STORED : StringField.TYPE_NOT_STORED);
  }

  public static Field newTextField(String name, String value, Store stored) {
    return newField(random(), name, value, stored == Store.YES ? TextField.TYPE_STORED : TextField.TYPE_NOT_STORED);
  }
  
  public static Field newStringField(Random random, String name, String value, Store stored) {
    return newField(random, name, value, stored == Store.YES ? StringField.TYPE_STORED : StringField.TYPE_NOT_STORED);
  }
  
  public static Field newTextField(Random random, String name, String value, Store stored) {
    return newField(random, name, value, stored == Store.YES ? TextField.TYPE_STORED : TextField.TYPE_NOT_STORED);
  }
  
  public static Field newField(String name, String value, FieldType type) {
    return newField(random(), name, value, type);
  }

  /** Returns a FieldType derived from newType but whose
   *  term vector options match the old type */
  private static FieldType mergeTermVectorOptions(FieldType newType, FieldType oldType) {
    if (newType.indexed() && oldType.storeTermVectors() == true && newType.storeTermVectors() == false) {
      newType = new FieldType(newType);
      newType.setStoreTermVectors(oldType.storeTermVectors());
      newType.setStoreTermVectorPositions(oldType.storeTermVectorPositions());
      newType.setStoreTermVectorOffsets(oldType.storeTermVectorOffsets());
      newType.setStoreTermVectorPayloads(oldType.storeTermVectorPayloads());
      newType.freeze();
    }

    return newType;
  }

  // TODO: if we can pull out the "make term vector options
  // consistent across all instances of the same field name"
  // write-once schema sort of helper class then we can
  // remove the sync here.  We can also fold the random
  // "enable norms" (now commented out, below) into that:
  public synchronized static Field newField(Random random, String name, String value, FieldType type) {

    // Defeat any consumers that illegally rely on intern'd
    // strings (we removed this from Lucene a while back):
    name = new String(name);

    FieldType prevType = fieldToType.get(name);

    if (usually(random) || !type.indexed() || prevType != null) {
      // most of the time, don't modify the params
      if (prevType == null) {
        fieldToType.put(name, new FieldType(type));
      } else {
        type = mergeTermVectorOptions(type, prevType);
      }

      return new Field(name, value, type);
    }

    // TODO: once all core & test codecs can index
    // offsets, sometimes randomly turn on offsets if we are
    // already indexing positions...

    FieldType newType = new FieldType(type);
    if (!newType.stored() && random.nextBoolean()) {
      newType.setStored(true); // randomly store it
    }

    // Randomly turn on term vector options, but always do
    // so consistently for the same field name:
    if (!newType.storeTermVectors() && random.nextBoolean()) {
      newType.setStoreTermVectors(true);
      if (!newType.storeTermVectorPositions()) {
        newType.setStoreTermVectorPositions(random.nextBoolean());
        
        if (newType.storeTermVectorPositions()) {
          if (!newType.storeTermVectorPayloads() && !OLD_FORMAT_IMPERSONATION_IS_ACTIVE) {
            newType.setStoreTermVectorPayloads(random.nextBoolean());
          }
        }
      }
      
      if (!newType.storeTermVectorOffsets()) {
        newType.setStoreTermVectorOffsets(random.nextBoolean());
      }

      if (VERBOSE) {
        System.out.println("NOTE: LuceneTestCase: upgrade name=" + name + " type=" + newType);
      }
    }
    newType.freeze();
    fieldToType.put(name, newType);

    // TODO: we need to do this, but smarter, ie, most of
    // the time we set the same value for a given field but
    // sometimes (rarely) we change it up:
    /*
    if (newType.omitNorms()) {
      newType.setOmitNorms(random.nextBoolean());
    }
    */
    
    return new Field(name, value, newType);
  }

  /** 
   * Return a random Locale from the available locales on the system.
   * @see "https://issues.apache.org/jira/browse/LUCENE-4020"
   */
  public static Locale randomLocale(Random random) {
    Locale locales[] = Locale.getAvailableLocales();
    return locales[random.nextInt(locales.length)];
  }

  /** 
   * Return a random TimeZone from the available timezones on the system
   * @see "https://issues.apache.org/jira/browse/LUCENE-4020" 
   */
  public static TimeZone randomTimeZone(Random random) {
    String tzIds[] = TimeZone.getAvailableIDs();
    return TimeZone.getTimeZone(tzIds[random.nextInt(tzIds.length)]);
  }

  /** return a Locale object equivalent to its programmatic name */
  public static Locale localeForName(String localeName) {
    String elements[] = localeName.split("\\_");
    switch(elements.length) {
      case 4: /* fallthrough for special cases */
      case 3: return new Locale(elements[0], elements[1], elements[2]);
      case 2: return new Locale(elements[0], elements[1]);
      case 1: return new Locale(elements[0]);
      default: throw new IllegalArgumentException("Invalid Locale: " + localeName);
    }
  }

  public static boolean defaultCodecSupportsDocValues() {
    return !Codec.getDefault().getName().equals("Lucene3x");
  }

  private static Directory newFSDirectoryImpl(
      Class<? extends FSDirectory> clazz, File file)
      throws IOException {
    FSDirectory d = null;
    try {
      d = CommandLineUtil.newFSDirectory(clazz, file);
    } catch (Exception e) {
      Rethrow.rethrow(e);
    }
    return d;
  }

  static Directory newDirectoryImpl(Random random, String clazzName) {
    if (clazzName.equals("random")) {
      if (rarely(random)) {
        clazzName = RandomPicks.randomFrom(random, CORE_DIRECTORIES);
      } else {
        clazzName = "RAMDirectory";
      }
    }

    try {
      final Class<? extends Directory> clazz = CommandLineUtil.loadDirectoryClass(clazzName);
      // If it is a FSDirectory type, try its ctor(File)
      if (FSDirectory.class.isAssignableFrom(clazz)) {
        final File dir = createTempDir("index-" + clazzName);
        return newFSDirectoryImpl(clazz.asSubclass(FSDirectory.class), dir);
      }

      // See if it has a File ctor even though it's not an
      // FSDir subclass:
      Constructor<? extends Directory> fileCtor = null;
      try {
        fileCtor = clazz.getConstructor(File.class);
      } catch (NoSuchMethodException nsme) {
        // Ignore
      }

      if (fileCtor != null) {
        final File dir = createTempDir("index");
        return fileCtor.newInstance(dir);
      }

      // try empty ctor
      return clazz.newInstance();
    } catch (Exception e) {
      Rethrow.rethrow(e);
      throw null; // dummy to prevent compiler failure
    }
  }
  
  /**
   * Sometimes wrap the IndexReader as slow, parallel or filter reader (or
   * combinations of that)
   */
  public static IndexReader maybeWrapReader(IndexReader r) throws IOException {
    Random random = random();
    if (rarely()) {
      // TODO: remove this, and fix those tests to wrap before putting slow around:
      final boolean wasOriginallyAtomic = r instanceof AtomicReader;
      for (int i = 0, c = random.nextInt(6)+1; i < c; i++) {
        switch(random.nextInt(5)) {
          case 0:
            r = SlowCompositeReaderWrapper.wrap(r);
            break;
          case 1:
            // will create no FC insanity in atomic case, as ParallelAtomicReader has own cache key:
            r = (r instanceof AtomicReader) ?
              new ParallelAtomicReader((AtomicReader) r) :
              new ParallelCompositeReader((CompositeReader) r);
            break;
          case 2:
            // Häckidy-Hick-Hack: a standard MultiReader will cause FC insanity, so we use
            // QueryUtils' reader with a fake cache key, so insanity checker cannot walk
            // along our reader:
            r = new FCInvisibleMultiReader(r);
            break;
          case 3:
            final AtomicReader ar = SlowCompositeReaderWrapper.wrap(r);
            final List<String> allFields = new ArrayList<>();
            for (FieldInfo fi : ar.getFieldInfos()) {
              allFields.add(fi.name);
            }
            Collections.shuffle(allFields, random);
            final int end = allFields.isEmpty() ? 0 : random.nextInt(allFields.size());
            final Set<String> fields = new HashSet<>(allFields.subList(0, end));
            // will create no FC insanity as ParallelAtomicReader has own cache key:
            r = new ParallelAtomicReader(
              new FieldFilterAtomicReader(ar, fields, false),
              new FieldFilterAtomicReader(ar, fields, true)
            );
            break;
          case 4:
            // Häckidy-Hick-Hack: a standard Reader will cause FC insanity, so we use
            // QueryUtils' reader with a fake cache key, so insanity checker cannot walk
            // along our reader:
            if (r instanceof AtomicReader) {
              r = new AssertingAtomicReader((AtomicReader)r);
            } else if (r instanceof DirectoryReader) {
              r = new AssertingDirectoryReader((DirectoryReader)r);
            }
            break;
          default:
            fail("should not get here");
        }
      }
      if (wasOriginallyAtomic) {
        r = SlowCompositeReaderWrapper.wrap(r);
      } else if ((r instanceof CompositeReader) && !(r instanceof FCInvisibleMultiReader)) {
        // prevent cache insanity caused by e.g. ParallelCompositeReader, to fix we wrap one more time:
        r = new FCInvisibleMultiReader(r);
      }
      if (VERBOSE) {
        System.out.println("maybeWrapReader wrapped: " +r);
      }
    }
    return r;
  }

  /** TODO: javadoc */
  public static IOContext newIOContext(Random random) {
    return newIOContext(random, IOContext.DEFAULT);
  }

  /** TODO: javadoc */
  public static IOContext newIOContext(Random random, IOContext oldContext) {
    final int randomNumDocs = random.nextInt(4192);
    final int size = random.nextInt(512) * randomNumDocs;
    if (oldContext.flushInfo != null) {
      // Always return at least the estimatedSegmentSize of
      // the incoming IOContext:
      return new IOContext(new FlushInfo(randomNumDocs, Math.max(oldContext.flushInfo.estimatedSegmentSize, size)));
    } else if (oldContext.mergeInfo != null) {
      // Always return at least the estimatedMergeBytes of
      // the incoming IOContext:
      return new IOContext(new MergeInfo(randomNumDocs, Math.max(oldContext.mergeInfo.estimatedMergeBytes, size), random.nextBoolean(), TestUtil.nextInt(random, 1, 100)));
    } else {
      // Make a totally random IOContext:
      final IOContext context;
      switch (random.nextInt(5)) {
      case 0:
        context = IOContext.DEFAULT;
        break;
      case 1:
        context = IOContext.READ;
        break;
      case 2:
        context = IOContext.READONCE;
        break;
      case 3:
        context = new IOContext(new MergeInfo(randomNumDocs, size, true, -1));
        break;
      case 4:
        context = new IOContext(new FlushInfo(randomNumDocs, size));
        break;
      default:
        context = IOContext.DEFAULT;
      }
      return context;
    }
  }

  /**
   * Create a new searcher over the reader. This searcher might randomly use
   * threads.
   */
  public static IndexSearcher newSearcher(IndexReader r) {
    return newSearcher(r, true);
  }

  /**
   * Create a new searcher over the reader. This searcher might randomly use
   * threads.
   */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) {
    return newSearcher(r, maybeWrap, true);
  }

  /**
   * Create a new searcher over the reader. This searcher might randomly use
   * threads. if <code>maybeWrap</code> is true, this searcher might wrap the
   * reader with one that returns null for getSequentialSubReaders. If
   * <code>wrapWithAssertions</code> is true, this searcher might be an
   * {@link AssertingIndexSearcher} instance.
   */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap, boolean wrapWithAssertions) {
    Random random = random();
    if (usually()) {
      if (maybeWrap) {
        try {
          r = maybeWrapReader(r);
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
      }
      // TODO: this whole check is a coverage hack, we should move it to tests for various filterreaders.
      // ultimately whatever you do will be checkIndex'd at the end anyway. 
      if (random.nextInt(500) == 0 && r instanceof AtomicReader) {
        // TODO: not useful to check DirectoryReader (redundant with checkindex)
        // but maybe sometimes run this on the other crazy readers maybeWrapReader creates?
        try {
          TestUtil.checkReader(r);
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
      }
      final IndexSearcher ret;
      if (wrapWithAssertions) {
        ret = random.nextBoolean() ? new AssertingIndexSearcher(random, r) : new AssertingIndexSearcher(random, r.getContext());
      } else {
        ret = random.nextBoolean() ? new IndexSearcher(r) : new IndexSearcher(r.getContext());
      }
      ret.setSimilarity(classEnvRule.similarity);
      return ret;
    } else {
      int threads = 0;
      final ThreadPoolExecutor ex;
      if (random.nextBoolean()) {
        ex = null;
      } else {
        threads = TestUtil.nextInt(random, 1, 8);
        ex = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("LuceneTestCase"));
        // uncomment to intensify LUCENE-3840
        // ex.prestartAllCoreThreads();
      }
      if (ex != null) {
       if (VERBOSE) {
         System.out.println("NOTE: newSearcher using ExecutorService with " + threads + " threads");
       }
       r.addReaderClosedListener(new ReaderClosedListener() {
         @Override
         public void onClose(IndexReader reader) {
           TestUtil.shutdownExecutorService(ex);
         }
       });
      }
      IndexSearcher ret;
      if (wrapWithAssertions) {
        ret = random.nextBoolean()
            ? new AssertingIndexSearcher(random, r, ex)
            : new AssertingIndexSearcher(random, r.getContext(), ex);
      } else {
        ret = random.nextBoolean()
            ? new IndexSearcher(r, ex)
            : new IndexSearcher(r.getContext(), ex);
      }
      ret.setSimilarity(classEnvRule.similarity);
      return ret;
    }
  }

  /**
   * Gets a resource from the classpath as {@link File}. This method should only
   * be used, if a real file is needed. To get a stream, code should prefer
   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
   */
  protected File getDataFile(String name) throws IOException {
    try {
      return new File(this.getClass().getResource(name).toURI());
    } catch (Exception e) {
      throw new IOException("Cannot find resource: " + name);
    }
  }
  
  /** Returns true if the default codec supports single valued docvalues with missing values */ 
  public static boolean defaultCodecSupportsMissingDocValues() {
    String name = Codec.getDefault().getName();
    if (name.equals("Lucene3x") ||
        name.equals("Lucene40") || name.equals("Appending") ||
        name.equals("Lucene41") || 
        name.equals("Lucene42")) {
      return false;
    }
    return true;
  }
  
  /** Returns true if the default codec supports SORTED_SET docvalues */ 
  public static boolean defaultCodecSupportsSortedSet() {
    if (!defaultCodecSupportsDocValues()) {
      return false;
    }
    String name = Codec.getDefault().getName();
    if (name.equals("Lucene40") || name.equals("Lucene41") || name.equals("Appending")) {
      return false;
    }
    return true;
  }
  
  /** Returns true if the default codec supports SORTED_NUMERIC docvalues */ 
  public static boolean defaultCodecSupportsSortedNumeric() {
    if (!defaultCodecSupportsDocValues()) {
      return false;
    }
    String name = Codec.getDefault().getName();
    if (name.equals("Lucene40") || name.equals("Lucene41") || name.equals("Lucene42") || name.equals("Lucene45") || name.equals("Lucene46")) {
      return false;
    }
    return true;
  }
  
  /** Returns true if the codec "supports" docsWithField 
   * (other codecs return MatchAllBits, because you couldnt write missing values before) */
  public static boolean defaultCodecSupportsDocsWithField() {
    if (!defaultCodecSupportsDocValues()) {
      return false;
    }
    String name = Codec.getDefault().getName();
    if (name.equals("Appending") || name.equals("Lucene40") || name.equals("Lucene41") || name.equals("Lucene42")) {
      return false;
    }
    return true;
  }
  
  /** Returns true if the codec "supports" field updates. */
  public static boolean defaultCodecSupportsFieldUpdates() {
    String name = Codec.getDefault().getName();
    if (name.equals("Lucene3x") || name.equals("Appending")
        || name.equals("Lucene40") || name.equals("Lucene41")
        || name.equals("Lucene42") || name.equals("Lucene45")) {
      return false;
    }
    return true;
  }

  public void assertReaderEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    assertReaderStatisticsEquals(info, leftReader, rightReader);
    assertFieldsEquals(info, leftReader, MultiFields.getFields(leftReader), MultiFields.getFields(rightReader), true);
    assertNormsEquals(info, leftReader, rightReader);
    assertStoredFieldsEquals(info, leftReader, rightReader);
    assertTermVectorsEquals(info, leftReader, rightReader);
    assertDocValuesEquals(info, leftReader, rightReader);
    assertDeletedDocsEquals(info, leftReader, rightReader);
    assertFieldInfosEquals(info, leftReader, rightReader);
  }

  /** 
   * checks that reader-level statistics are the same 
   */
  public void assertReaderStatisticsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    // Somewhat redundant: we never delete docs
    assertEquals(info, leftReader.maxDoc(), rightReader.maxDoc());
    assertEquals(info, leftReader.numDocs(), rightReader.numDocs());
    assertEquals(info, leftReader.numDeletedDocs(), rightReader.numDeletedDocs());
    assertEquals(info, leftReader.hasDeletions(), rightReader.hasDeletions());
  }

  /** 
   * Fields api equivalency 
   */
  public void assertFieldsEquals(String info, IndexReader leftReader, Fields leftFields, Fields rightFields, boolean deep) throws IOException {
    // Fields could be null if there are no postings,
    // but then it must be null for both
    if (leftFields == null || rightFields == null) {
      assertNull(info, leftFields);
      assertNull(info, rightFields);
      return;
    }
    assertFieldStatisticsEquals(info, leftFields, rightFields);
    
    Iterator<String> leftEnum = leftFields.iterator();
    Iterator<String> rightEnum = rightFields.iterator();
    
    while (leftEnum.hasNext()) {
      String field = leftEnum.next();
      assertEquals(info, field, rightEnum.next());
      assertTermsEquals(info, leftReader, leftFields.terms(field), rightFields.terms(field), deep);
    }
    assertFalse(rightEnum.hasNext());
  }

  /** 
   * checks that top-level statistics on Fields are the same 
   */
  public void assertFieldStatisticsEquals(String info, Fields leftFields, Fields rightFields) throws IOException {
    if (leftFields.size() != -1 && rightFields.size() != -1) {
      assertEquals(info, leftFields.size(), rightFields.size());
    }
  }

  /** 
   * Terms api equivalency 
   */
  public void assertTermsEquals(String info, IndexReader leftReader, Terms leftTerms, Terms rightTerms, boolean deep) throws IOException {
    if (leftTerms == null || rightTerms == null) {
      assertNull(info, leftTerms);
      assertNull(info, rightTerms);
      return;
    }
    assertTermsStatisticsEquals(info, leftTerms, rightTerms);
    assertEquals(leftTerms.hasOffsets(), rightTerms.hasOffsets());
    assertEquals(leftTerms.hasPositions(), rightTerms.hasPositions());
    assertEquals(leftTerms.hasPayloads(), rightTerms.hasPayloads());

    TermsEnum leftTermsEnum = leftTerms.iterator(null);
    TermsEnum rightTermsEnum = rightTerms.iterator(null);
    assertTermsEnumEquals(info, leftReader, leftTermsEnum, rightTermsEnum, true);
    
    assertTermsSeekingEquals(info, leftTerms, rightTerms);
    
    if (deep) {
      int numIntersections = atLeast(3);
      for (int i = 0; i < numIntersections; i++) {
        String re = AutomatonTestUtil.randomRegexp(random());
        CompiledAutomaton automaton = new CompiledAutomaton(new RegExp(re, RegExp.NONE).toAutomaton());
        if (automaton.type == CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
          // TODO: test start term too
          TermsEnum leftIntersection = leftTerms.intersect(automaton, null);
          TermsEnum rightIntersection = rightTerms.intersect(automaton, null);
          assertTermsEnumEquals(info, leftReader, leftIntersection, rightIntersection, rarely());
        }
      }
    }
  }

  /** 
   * checks collection-level statistics on Terms 
   */
  public void assertTermsStatisticsEquals(String info, Terms leftTerms, Terms rightTerms) throws IOException {
    assert leftTerms.getComparator() == rightTerms.getComparator();
    if (leftTerms.getDocCount() != -1 && rightTerms.getDocCount() != -1) {
      assertEquals(info, leftTerms.getDocCount(), rightTerms.getDocCount());
    }
    if (leftTerms.getSumDocFreq() != -1 && rightTerms.getSumDocFreq() != -1) {
      assertEquals(info, leftTerms.getSumDocFreq(), rightTerms.getSumDocFreq());
    }
    if (leftTerms.getSumTotalTermFreq() != -1 && rightTerms.getSumTotalTermFreq() != -1) {
      assertEquals(info, leftTerms.getSumTotalTermFreq(), rightTerms.getSumTotalTermFreq());
    }
    if (leftTerms.size() != -1 && rightTerms.size() != -1) {
      assertEquals(info, leftTerms.size(), rightTerms.size());
    }
  }

  private static class RandomBits implements Bits {
    FixedBitSet bits;
    
    RandomBits(int maxDoc, double pctLive, Random random) {
      bits = new FixedBitSet(maxDoc);
      for (int i = 0; i < maxDoc; i++) {
        if (random.nextDouble() <= pctLive) {        
          bits.set(i);
        }
      }
    }
    
    @Override
    public boolean get(int index) {
      return bits.get(index);
    }

    @Override
    public int length() {
      return bits.length();
    }
  }

  /** 
   * checks the terms enum sequentially
   * if deep is false, it does a 'shallow' test that doesnt go down to the docsenums
   */
  public void assertTermsEnumEquals(String info, IndexReader leftReader, TermsEnum leftTermsEnum, TermsEnum rightTermsEnum, boolean deep) throws IOException {
    BytesRef term;
    Bits randomBits = new RandomBits(leftReader.maxDoc(), random().nextDouble(), random());
    DocsAndPositionsEnum leftPositions = null;
    DocsAndPositionsEnum rightPositions = null;
    DocsEnum leftDocs = null;
    DocsEnum rightDocs = null;
    
    while ((term = leftTermsEnum.next()) != null) {
      assertEquals(info, term, rightTermsEnum.next());
      assertTermStatsEquals(info, leftTermsEnum, rightTermsEnum);
      if (deep) {
        assertDocsAndPositionsEnumEquals(info, leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions),
                                   rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions));
        assertDocsAndPositionsEnumEquals(info, leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions),
                                   rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions));

        assertPositionsSkippingEquals(info, leftReader, leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(null, leftPositions),
                                rightPositions = rightTermsEnum.docsAndPositions(null, rightPositions));
        assertPositionsSkippingEquals(info, leftReader, leftTermsEnum.docFreq(), 
                                leftPositions = leftTermsEnum.docsAndPositions(randomBits, leftPositions),
                                rightPositions = rightTermsEnum.docsAndPositions(randomBits, rightPositions));

        // with freqs:
        assertDocsEnumEquals(info, leftDocs = leftTermsEnum.docs(null, leftDocs),
            rightDocs = rightTermsEnum.docs(null, rightDocs),
            true);
        assertDocsEnumEquals(info, leftDocs = leftTermsEnum.docs(randomBits, leftDocs),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs),
            true);

        // w/o freqs:
        assertDocsEnumEquals(info, leftDocs = leftTermsEnum.docs(null, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(null, rightDocs, DocsEnum.FLAG_NONE),
            false);
        assertDocsEnumEquals(info, leftDocs = leftTermsEnum.docs(randomBits, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs, DocsEnum.FLAG_NONE),
            false);
        
        // with freqs:
        assertDocsSkippingEquals(info, leftReader, leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(null, leftDocs),
            rightDocs = rightTermsEnum.docs(null, rightDocs),
            true);
        assertDocsSkippingEquals(info, leftReader, leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(randomBits, leftDocs),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs),
            true);

        // w/o freqs:
        assertDocsSkippingEquals(info, leftReader, leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(null, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(null, rightDocs, DocsEnum.FLAG_NONE),
            false);
        assertDocsSkippingEquals(info, leftReader, leftTermsEnum.docFreq(), 
            leftDocs = leftTermsEnum.docs(randomBits, leftDocs, DocsEnum.FLAG_NONE),
            rightDocs = rightTermsEnum.docs(randomBits, rightDocs, DocsEnum.FLAG_NONE),
            false);
      }
    }
    assertNull(info, rightTermsEnum.next());
  }


  /**
   * checks docs + freqs + positions + payloads, sequentially
   */
  public void assertDocsAndPositionsEnumEquals(String info, DocsAndPositionsEnum leftDocs, DocsAndPositionsEnum rightDocs) throws IOException {
    if (leftDocs == null || rightDocs == null) {
      assertNull(leftDocs);
      assertNull(rightDocs);
      return;
    }
    assertEquals(info, -1, leftDocs.docID());
    assertEquals(info, -1, rightDocs.docID());
    int docid;
    while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(info, docid, rightDocs.nextDoc());
      int freq = leftDocs.freq();
      assertEquals(info, freq, rightDocs.freq());
      for (int i = 0; i < freq; i++) {
        assertEquals(info, leftDocs.nextPosition(), rightDocs.nextPosition());
        assertEquals(info, leftDocs.getPayload(), rightDocs.getPayload());
        assertEquals(info, leftDocs.startOffset(), rightDocs.startOffset());
        assertEquals(info, leftDocs.endOffset(), rightDocs.endOffset());
      }
    }
    assertEquals(info, DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
  }
  
  /**
   * checks docs + freqs, sequentially
   */
  public void assertDocsEnumEquals(String info, DocsEnum leftDocs, DocsEnum rightDocs, boolean hasFreqs) throws IOException {
    if (leftDocs == null) {
      assertNull(rightDocs);
      return;
    }
    assertEquals(info, -1, leftDocs.docID());
    assertEquals(info, -1, rightDocs.docID());
    int docid;
    while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      assertEquals(info, docid, rightDocs.nextDoc());
      if (hasFreqs) {
        assertEquals(info, leftDocs.freq(), rightDocs.freq());
      }
    }
    assertEquals(info, DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
  }
  
  /**
   * checks advancing docs
   */
  public void assertDocsSkippingEquals(String info, IndexReader leftReader, int docFreq, DocsEnum leftDocs, DocsEnum rightDocs, boolean hasFreqs) throws IOException {
    if (leftDocs == null) {
      assertNull(rightDocs);
      return;
    }
    int docid = -1;
    int averageGap = leftReader.maxDoc() / (1+docFreq);
    int skipInterval = 16;

    while (true) {
      if (random().nextBoolean()) {
        // nextDoc()
        docid = leftDocs.nextDoc();
        assertEquals(info, docid, rightDocs.nextDoc());
      } else {
        // advance()
        int skip = docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
        docid = leftDocs.advance(skip);
        assertEquals(info, docid, rightDocs.advance(skip));
      }
      
      if (docid == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      if (hasFreqs) {
        assertEquals(info, leftDocs.freq(), rightDocs.freq());
      }
    }
  }
  
  /**
   * checks advancing docs + positions
   */
  public void assertPositionsSkippingEquals(String info, IndexReader leftReader, int docFreq, DocsAndPositionsEnum leftDocs, DocsAndPositionsEnum rightDocs) throws IOException {
    if (leftDocs == null || rightDocs == null) {
      assertNull(leftDocs);
      assertNull(rightDocs);
      return;
    }
    
    int docid = -1;
    int averageGap = leftReader.maxDoc() / (1+docFreq);
    int skipInterval = 16;

    while (true) {
      if (random().nextBoolean()) {
        // nextDoc()
        docid = leftDocs.nextDoc();
        assertEquals(info, docid, rightDocs.nextDoc());
      } else {
        // advance()
        int skip = docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
        docid = leftDocs.advance(skip);
        assertEquals(info, docid, rightDocs.advance(skip));
      }
      
      if (docid == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      int freq = leftDocs.freq();
      assertEquals(info, freq, rightDocs.freq());
      for (int i = 0; i < freq; i++) {
        assertEquals(info, leftDocs.nextPosition(), rightDocs.nextPosition());
        assertEquals(info, leftDocs.getPayload(), rightDocs.getPayload());
      }
    }
  }

  
  private void assertTermsSeekingEquals(String info, Terms leftTerms, Terms rightTerms) throws IOException {
    TermsEnum leftEnum = null;
    TermsEnum rightEnum = null;

    // just an upper bound
    int numTests = atLeast(20);
    Random random = random();

    // collect this number of terms from the left side
    HashSet<BytesRef> tests = new HashSet<>();
    int numPasses = 0;
    while (numPasses < 10 && tests.size() < numTests) {
      leftEnum = leftTerms.iterator(leftEnum);
      BytesRef term = null;
      while ((term = leftEnum.next()) != null) {
        int code = random.nextInt(10);
        if (code == 0) {
          // the term
          tests.add(BytesRef.deepCopyOf(term));
        } else if (code == 1) {
          // truncated subsequence of term
          term = BytesRef.deepCopyOf(term);
          if (term.length > 0) {
            // truncate it
            term.length = random.nextInt(term.length);
          }
        } else if (code == 2) {
          // term, but ensure a non-zero offset
          byte newbytes[] = new byte[term.length+5];
          System.arraycopy(term.bytes, term.offset, newbytes, 5, term.length);
          tests.add(new BytesRef(newbytes, 5, term.length));
        } else if (code == 3) {
          switch (random().nextInt(3)) {
            case 0:
              tests.add(new BytesRef()); // before the first term
              break;
            case 1:
              tests.add(new BytesRef(new byte[] {(byte) 0xFF, (byte) 0xFF})); // past the last term
              break;
            case 2:
              tests.add(new BytesRef(TestUtil.randomSimpleString(random()))); // random term
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      numPasses++;
    }

    rightEnum = rightTerms.iterator(rightEnum);

    ArrayList<BytesRef> shuffledTests = new ArrayList<>(tests);
    Collections.shuffle(shuffledTests, random);

    for (BytesRef b : shuffledTests) {
      if (rarely()) {
        // reuse the enums
        leftEnum = leftTerms.iterator(leftEnum);
        rightEnum = rightTerms.iterator(rightEnum);
      }

      final boolean seekExact = random().nextBoolean();

      if (seekExact) {
        assertEquals(info, leftEnum.seekExact(b), rightEnum.seekExact(b));
      } else {
        SeekStatus leftStatus = leftEnum.seekCeil(b);
        SeekStatus rightStatus = rightEnum.seekCeil(b);
        assertEquals(info, leftStatus, rightStatus);
        if (leftStatus != SeekStatus.END) {
          assertEquals(info, leftEnum.term(), rightEnum.term());
          assertTermStatsEquals(info, leftEnum, rightEnum);
        }
      }
    }
  }
  
  /**
   * checks term-level statistics
   */
  public void assertTermStatsEquals(String info, TermsEnum leftTermsEnum, TermsEnum rightTermsEnum) throws IOException {
    assertEquals(info, leftTermsEnum.docFreq(), rightTermsEnum.docFreq());
    if (leftTermsEnum.totalTermFreq() != -1 && rightTermsEnum.totalTermFreq() != -1) {
      assertEquals(info, leftTermsEnum.totalTermFreq(), rightTermsEnum.totalTermFreq());
    }
  }
  
  /** 
   * checks that norms are the same across all fields 
   */
  public void assertNormsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    Fields leftFields = MultiFields.getFields(leftReader);
    Fields rightFields = MultiFields.getFields(rightReader);
    // Fields could be null if there are no postings,
    // but then it must be null for both
    if (leftFields == null || rightFields == null) {
      assertNull(info, leftFields);
      assertNull(info, rightFields);
      return;
    }
    
    for (String field : leftFields) {
      NumericDocValues leftNorms = MultiDocValues.getNormValues(leftReader, field);
      NumericDocValues rightNorms = MultiDocValues.getNormValues(rightReader, field);
      if (leftNorms != null && rightNorms != null) {
        assertDocValuesEquals(info, leftReader.maxDoc(), leftNorms, rightNorms);
      } else {
        assertNull(info, leftNorms);
        assertNull(info, rightNorms);
      }
    }
  }
  
  /** 
   * checks that stored fields of all documents are the same 
   */
  public void assertStoredFieldsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    assert leftReader.maxDoc() == rightReader.maxDoc();
    for (int i = 0; i < leftReader.maxDoc(); i++) {
      Document leftDoc = leftReader.document(i);
      Document rightDoc = rightReader.document(i);
      
      // TODO: I think this is bogus because we don't document what the order should be
      // from these iterators, etc. I think the codec/IndexReader should be free to order this stuff
      // in whatever way it wants (e.g. maybe it packs related fields together or something)
      // To fix this, we sort the fields in both documents by name, but
      // we still assume that all instances with same name are in order:
      Comparator<IndexableField> comp = new Comparator<IndexableField>() {
        @Override
        public int compare(IndexableField arg0, IndexableField arg1) {
          return arg0.name().compareTo(arg1.name());
        }        
      };
      Collections.sort(leftDoc.getFields(), comp);
      Collections.sort(rightDoc.getFields(), comp);

      Iterator<IndexableField> leftIterator = leftDoc.iterator();
      Iterator<IndexableField> rightIterator = rightDoc.iterator();
      while (leftIterator.hasNext()) {
        assertTrue(info, rightIterator.hasNext());
        assertStoredFieldEquals(info, leftIterator.next(), rightIterator.next());
      }
      assertFalse(info, rightIterator.hasNext());
    }
  }
  
  /** 
   * checks that two stored fields are equivalent 
   */
  public void assertStoredFieldEquals(String info, IndexableField leftField, IndexableField rightField) {
    assertEquals(info, leftField.name(), rightField.name());
    assertEquals(info, leftField.binaryValue(), rightField.binaryValue());
    assertEquals(info, leftField.stringValue(), rightField.stringValue());
    assertEquals(info, leftField.numericValue(), rightField.numericValue());
    // TODO: should we check the FT at all?
  }
  
  /** 
   * checks that term vectors across all fields are equivalent 
   */
  public void assertTermVectorsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    assert leftReader.maxDoc() == rightReader.maxDoc();
    for (int i = 0; i < leftReader.maxDoc(); i++) {
      Fields leftFields = leftReader.getTermVectors(i);
      Fields rightFields = rightReader.getTermVectors(i);
      assertFieldsEquals(info, leftReader, leftFields, rightFields, rarely());
    }
  }

  private static Set<String> getDVFields(IndexReader reader) {
    Set<String> fields = new HashSet<>();
    for(FieldInfo fi : MultiFields.getMergedFieldInfos(reader)) {
      if (fi.hasDocValues()) {
        fields.add(fi.name);
      }
    }

    return fields;
  }
  
  /**
   * checks that docvalues across all fields are equivalent
   */
  public void assertDocValuesEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    Set<String> leftFields = getDVFields(leftReader);
    Set<String> rightFields = getDVFields(rightReader);
    assertEquals(info, leftFields, rightFields);

    for (String field : leftFields) {
      // TODO: clean this up... very messy
      {
        NumericDocValues leftValues = MultiDocValues.getNumericValues(leftReader, field);
        NumericDocValues rightValues = MultiDocValues.getNumericValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          assertDocValuesEquals(info, leftReader.maxDoc(), leftValues, rightValues);
        } else {
          assertNull(info, leftValues);
          assertNull(info, rightValues);
        }
      }

      {
        BinaryDocValues leftValues = MultiDocValues.getBinaryValues(leftReader, field);
        BinaryDocValues rightValues = MultiDocValues.getBinaryValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          for(int docID=0;docID<leftReader.maxDoc();docID++) {
            final BytesRef left = BytesRef.deepCopyOf(leftValues.get(docID));
            final BytesRef right = rightValues.get(docID);
            assertEquals(info, left, right);
          }
        } else {
          assertNull(info, leftValues);
          assertNull(info, rightValues);
        }
      }
      
      {
        SortedDocValues leftValues = MultiDocValues.getSortedValues(leftReader, field);
        SortedDocValues rightValues = MultiDocValues.getSortedValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          // numOrds
          assertEquals(info, leftValues.getValueCount(), rightValues.getValueCount());
          // ords
          for (int i = 0; i < leftValues.getValueCount(); i++) {
            final BytesRef left = BytesRef.deepCopyOf(leftValues.lookupOrd(i));
            final BytesRef right = rightValues.lookupOrd(i);
            assertEquals(info, left, right);
          }
          // bytes
          for(int docID=0;docID<leftReader.maxDoc();docID++) {
            final BytesRef left = BytesRef.deepCopyOf(leftValues.get(docID));
            final BytesRef right = rightValues.get(docID);
            assertEquals(info, left, right);
          }
        } else {
          assertNull(info, leftValues);
          assertNull(info, rightValues);
        }
      }
      
      {
        SortedSetDocValues leftValues = MultiDocValues.getSortedSetValues(leftReader, field);
        SortedSetDocValues rightValues = MultiDocValues.getSortedSetValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          // numOrds
          assertEquals(info, leftValues.getValueCount(), rightValues.getValueCount());
          // ords
          for (int i = 0; i < leftValues.getValueCount(); i++) {
            final BytesRef left = BytesRef.deepCopyOf(leftValues.lookupOrd(i));
            final BytesRef right = rightValues.lookupOrd(i);
            assertEquals(info, left, right);
          }
          // ord lists
          for(int docID=0;docID<leftReader.maxDoc();docID++) {
            leftValues.setDocument(docID);
            rightValues.setDocument(docID);
            long ord;
            while ((ord = leftValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              assertEquals(info, ord, rightValues.nextOrd());
            }
            assertEquals(info, SortedSetDocValues.NO_MORE_ORDS, rightValues.nextOrd());
          }
        } else {
          assertNull(info, leftValues);
          assertNull(info, rightValues);
        }
      }
      
      {
        SortedNumericDocValues leftValues = MultiDocValues.getSortedNumericValues(leftReader, field);
        SortedNumericDocValues rightValues = MultiDocValues.getSortedNumericValues(rightReader, field);
        if (leftValues != null && rightValues != null) {
          for (int i = 0; i < leftReader.maxDoc(); i++) {
            leftValues.setDocument(i);
            long expected[] = new long[leftValues.count()];
            for (int j = 0; j < expected.length; j++) {
              expected[j] = leftValues.valueAt(j);
            }
            rightValues.setDocument(i);
            for (int j = 0; j < expected.length; j++) {
              assertEquals(info, expected[j], rightValues.valueAt(j));
            }
            assertEquals(info, expected.length, rightValues.count());
          }
        } else {
          assertNull(info, leftValues);
          assertNull(info, rightValues);
        }
      }
      
      {
        Bits leftBits = MultiDocValues.getDocsWithField(leftReader, field);
        Bits rightBits = MultiDocValues.getDocsWithField(rightReader, field);
        if (leftBits != null && rightBits != null) {
          assertEquals(info, leftBits.length(), rightBits.length());
          for (int i = 0; i < leftBits.length(); i++) {
            assertEquals(info, leftBits.get(i), rightBits.get(i));
          }
        } else {
          assertNull(info, leftBits);
          assertNull(info, rightBits);
        }
      }
    }
  }
  
  public void assertDocValuesEquals(String info, int num, NumericDocValues leftDocValues, NumericDocValues rightDocValues) throws IOException {
    assertNotNull(info, leftDocValues);
    assertNotNull(info, rightDocValues);
    for(int docID=0;docID<num;docID++) {
      assertEquals(leftDocValues.get(docID),
                   rightDocValues.get(docID));
    }
  }
  
  // TODO: this is kinda stupid, we don't delete documents in the test.
  public void assertDeletedDocsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    assert leftReader.numDeletedDocs() == rightReader.numDeletedDocs();
    Bits leftBits = MultiFields.getLiveDocs(leftReader);
    Bits rightBits = MultiFields.getLiveDocs(rightReader);
    
    if (leftBits == null || rightBits == null) {
      assertNull(info, leftBits);
      assertNull(info, rightBits);
      return;
    }
    
    assert leftReader.maxDoc() == rightReader.maxDoc();
    assertEquals(info, leftBits.length(), rightBits.length());
    for (int i = 0; i < leftReader.maxDoc(); i++) {
      assertEquals(info, leftBits.get(i), rightBits.get(i));
    }
  }
  
  public void assertFieldInfosEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
    FieldInfos leftInfos = MultiFields.getMergedFieldInfos(leftReader);
    FieldInfos rightInfos = MultiFields.getMergedFieldInfos(rightReader);
    
    // TODO: would be great to verify more than just the names of the fields!
    TreeSet<String> left = new TreeSet<>();
    TreeSet<String> right = new TreeSet<>();
    
    for (FieldInfo fi : leftInfos) {
      left.add(fi.name);
    }
    
    for (FieldInfo fi : rightInfos) {
      right.add(fi.name);
    }
    
    assertEquals(info, left, right);
  }

  /** Returns true if the file exists (can be opened), false
   *  if it cannot be opened, and (unlike Java's
   *  File.exists) throws IOException if there's some
   *  unexpected error. */
  public static boolean slowFileExists(Directory dir, String fileName) throws IOException {
    try {
      dir.openInput(fileName, IOContext.DEFAULT).close();
      return true;
    } catch (NoSuchFileException | FileNotFoundException e) {
      return false;
    }
  }

  /**
   * A base location for temporary files of a given test. Helps in figuring out
   * which tests left which files and where.
   */
  private static File tempDirBase;
  
  /**
   * Retry to create temporary file name this many times.
   */
  private static final int TEMP_NAME_RETRY_THRESHOLD = 9999;

  /**
   * This method is deprecated for a reason. Do not use it. Call {@link #createTempDir()}
   * or {@link #createTempDir(String)} or {@link #createTempFile(String, String)}.
   */
  @Deprecated
  public static File getBaseTempDirForTestClass() {
    synchronized (LuceneTestCase.class) {
      if (tempDirBase == null) {
        File directory = new File(System.getProperty("tempDir", System.getProperty("java.io.tmpdir")));
        assert directory.exists() && 
               directory.isDirectory() && 
               directory.canWrite();

        RandomizedContext ctx = RandomizedContext.current();
        Class<?> clazz = ctx.getTargetClass();
        String prefix = clazz.getName();
        prefix = prefix.replaceFirst("^org.apache.lucene.", "lucene.");
        prefix = prefix.replaceFirst("^org.apache.solr.", "solr.");

        int attempt = 0;
        File f;
        do {
          if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
            throw new RuntimeException(
                "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
                  + directory.getAbsolutePath());            
          }
          f = new File(directory, prefix + "-" + ctx.getRunnerSeedAsString() 
                + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
        } while (!f.mkdirs());

        tempDirBase = f;
        registerToRemoveAfterSuite(tempDirBase);
      }
    }
    return tempDirBase;
  }


  /**
   * Creates an empty, temporary folder (when the name of the folder is of no importance).
   * 
   * @see #createTempDir(String)
   */
  public static File createTempDir() {
    return createTempDir("tempDir");
  }

  /**
   * Creates an empty, temporary folder with the given name prefix under the 
   * test class's {@link #getBaseTempDirForTestClass()}.
   *  
   * <p>The folder will be automatically removed after the
   * test class completes successfully. The test should close any file handles that would prevent
   * the folder from being removed. 
   */
  public static File createTempDir(String prefix) {
    File base = getBaseTempDirForTestClass();

    int attempt = 0;
    File f;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
              + base.getAbsolutePath());            
      }
      f = new File(base, prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt));
    } while (!f.mkdirs());

    registerToRemoveAfterSuite(f);
    return f;
  }
  
  /**
   * Creates an empty file with the given prefix and suffix under the 
   * test class's {@link #getBaseTempDirForTestClass()}.
   * 
   * <p>The file will be automatically removed after the
   * test class completes successfully. The test should close any file handles that would prevent
   * the folder from being removed. 
   */
  public static File createTempFile(String prefix, String suffix) throws IOException {
    File base = getBaseTempDirForTestClass();

    int attempt = 0;
    File f;
    do {
      if (attempt++ >= TEMP_NAME_RETRY_THRESHOLD) {
        throw new RuntimeException(
            "Failed to get a temporary name too many times, check your temp directory and consider manually cleaning it: "
              + base.getAbsolutePath());            
      }
      f = new File(base, prefix + "-" + String.format(Locale.ENGLISH, "%03d", attempt) + suffix);
    } while (!f.createNewFile());

    registerToRemoveAfterSuite(f);
    return f;
  }

  /**
   * Creates an empty temporary file.
   * 
   * @see #createTempFile(String, String) 
   */
  public static File createTempFile() throws IOException {
    return createTempFile("tempFile", ".tmp");
  }

  /**
   * A queue of temporary resources to be removed after the
   * suite completes.
   * @see #registerToRemoveAfterSuite(File)
   */
  private final static List<File> cleanupQueue = new ArrayList<File>();

  /**
   * Register temporary folder for removal after the suite completes.
   */
  private static void registerToRemoveAfterSuite(File f) {
    assert f != null;

    if (LuceneTestCase.LEAVE_TEMPORARY) {
      System.err.println("INFO: Will leave temporary file: " + f.getAbsolutePath());
      return;
    }

    synchronized (cleanupQueue) {
      cleanupQueue.add(f);
    }
  }

  /**
   * Checks and cleans up temporary files.
   * 
   * @see LuceneTestCase#createTempDir()
   * @see LuceneTestCase#createTempFile()
   */
  private static class TemporaryFilesCleanupRule extends TestRuleAdapter {
    @Override
    protected void before() throws Throwable {
      super.before();
      assert tempDirBase == null;
    }

    @Override
    protected void afterAlways(List<Throwable> errors) throws Throwable {
      // Drain cleanup queue and clear it.
      final File [] everything;
      final String tempDirBasePath;
      synchronized (cleanupQueue) {
        tempDirBasePath = (tempDirBase != null ? tempDirBase.getAbsolutePath() : null);
        tempDirBase = null;

        Collections.reverse(cleanupQueue);
        everything = new File [cleanupQueue.size()];
        cleanupQueue.toArray(everything);
        cleanupQueue.clear();
      }

      // Only check and throw an IOException on un-removable files if the test
      // was successful. Otherwise just report the path of temporary files
      // and leave them there.
      if (LuceneTestCase.suiteFailureMarker.wasSuccessful()) {
        try {
          TestUtil.rm(everything);
        } catch (IOException e) {
          Class<?> suiteClass = RandomizedContext.current().getTargetClass();
          if (suiteClass.isAnnotationPresent(SuppressTempFileChecks.class)) {
            System.err.println("WARNING: Leftover undeleted temporary files (bugUrl: "
                + suiteClass.getAnnotation(SuppressTempFileChecks.class).bugUrl() + "): "
                + e.getMessage());
            return;
          }
          throw e;
        }
      } else {
        if (tempDirBasePath != null) {
          System.err.println("NOTE: leaving temporary files on disk at: " + tempDirBasePath);
        }
      }
    }
  }
}
