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

import java.io.*;
import java.lang.annotation.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.*;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.search.QueryUtils.FCInvisibleMultiReader;
import org.apache.lucene.store.*;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import com.carrotsearch.randomizedtesting.*;
import com.carrotsearch.randomizedtesting.annotations.*;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup.Group;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.rules.NoClassHooksShadowingRule;
import com.carrotsearch.randomizedtesting.rules.NoInstanceHooksOverridesRule;
import com.carrotsearch.randomizedtesting.rules.StaticFieldsInvariantRule;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesInvariantRule;

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
 * 
 * <p>There is a number of other facilities tests can use, like:
 * <ul>
 *   <li>{@link #closeAfterTest(Closeable)} and {@link #closeAfterSuite(Closeable)} to
 *   register resources to be closed after each scope (if close fails, the scope
 *   will fail too).</li>
 * </ul> 
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({
  LuceneJUnit3MethodProvider.class,
  JUnit4MethodProvider.class
})
@Listeners({
  RunListenerPrintReproduceInfo.class
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
public abstract class LuceneTestCase extends Assert {

  // --------------------------------------------------------------------
  // Test groups, system properties and other annotations modifying tests
  // --------------------------------------------------------------------

  public static final String SYSPROP_NIGHTLY = "tests.nightly";
  public static final String SYSPROP_WEEKLY = "tests.weekly";
  public static final String SYSPROP_AWAITSFIX = "tests.awaitsfix";
  public static final String SYSPROP_SLOW = "tests.slow";
  public static final String SYSPROP_BADAPPLES = "tests.badapples";

  /** @see #ignoreAfterMaxFailures*/
  private static final String SYSPROP_MAXFAILURES = "tests.maxfailures";

  /** @see #ignoreAfterMaxFailures*/
  private static final String SYSPROP_FAILFAST = "tests.failfast";

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
  public @interface BadApple {}

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
  
  // -----------------------------------------------------------------
  // Truly immutable fields and constants, initialized once and valid 
  // for all suites ever since.
  // -----------------------------------------------------------------

  /** 
   * Use this constant when creating Analyzers and any other version-dependent stuff.
   * <p><b>NOTE:</b> Change this when development starts for new Lucene version:
   */
  public static final Version TEST_VERSION_CURRENT = Version.LUCENE_43;

  /**
   * True if and only if tests are run in verbose mode. If this flag is false
   * tests are not expected to print any messages.
   */
  public static final boolean VERBOSE = systemPropertyAsBoolean("tests.verbose", false);

  /** TODO: javadoc? */
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

  /** Create indexes in this directory, optimally use a subdir, named after the test */
  public static final File TEMP_DIR;
  static {
    String s = System.getProperty("tempDir", System.getProperty("java.io.tmpdir"));
    if (s == null)
      throw new RuntimeException("To run tests, you need to define system property 'tempDir' or 'java.io.tmpdir'.");
    TEMP_DIR = new File(s);
    TEMP_DIR.mkdirs();
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
    CORE_DIRECTORIES = new ArrayList<String>(FS_DIRECTORIES);
    CORE_DIRECTORIES.add("RAMDirectory");
  };
  
  protected static final Set<String> doesntSupportOffsets = new HashSet<String>(Arrays.asList( 
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
   * @lucene.internal 
   */
  public static boolean PREFLEX_IMPERSONATION_IS_ACTIVE;


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
  public final static TestRuleMarkFailure suiteFailureMarker = 
      new TestRuleMarkFailure();

  /**
   * Ignore tests after hitting a designated number of initial failures.
   */
  final static TestRuleIgnoreAfterMaxFailures ignoreAfterMaxFailures; 
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

    ignoreAfterMaxFailures = new TestRuleIgnoreAfterMaxFailures(maxFailures);
  }

  /**
   * Max 10mb of static data stored in a test suite class after the suite is complete.
   * Prevents static data structures leaking and causing OOMs in subsequent tests.
   */
  private final static long STATIC_LEAK_THRESHOLD = 10 * 1024 * 1024;

  /** By-name list of ignored types like loggers etc. */
  private final static Set<String> STATIC_LEAK_IGNORED_TYPES = 
      Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
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
    .around(suiteFailureMarker)
    .around(new TestRuleAssertionsRequired())
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
    return _TestUtil.nextInt(random, min, max);
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
  public static <T> Set<T> asSet(T... args) {
    return new HashSet<T>(Arrays.asList(args));
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
  public static IndexWriterConfig newIndexWriterConfig(Version v, Analyzer a) {
    return newIndexWriterConfig(random(), v, a);
  }
  
  /** create a new index writer config with random defaults using the specified random */
  public static IndexWriterConfig newIndexWriterConfig(Random r, Version v, Analyzer a) {
    IndexWriterConfig c = new IndexWriterConfig(v, a);
    c.setSimilarity(classEnvRule.similarity);
    if (r.nextBoolean()) {
      c.setMergeScheduler(new SerialMergeScheduler());
    }
    if (r.nextBoolean()) {
      if (rarely(r)) {
        // crazy value
        c.setMaxBufferedDocs(_TestUtil.nextInt(r, 2, 15));
      } else {
        // reasonable value
        c.setMaxBufferedDocs(_TestUtil.nextInt(r, 16, 1000));
      }
    }
    if (r.nextBoolean()) {
      if (rarely(r)) {
        // crazy value
        c.setTermIndexInterval(r.nextBoolean() ? _TestUtil.nextInt(r, 1, 31) : _TestUtil.nextInt(r, 129, 1000));
      } else {
        // reasonable value
        c.setTermIndexInterval(_TestUtil.nextInt(r, 32, 128));
      }
    }
    if (r.nextBoolean()) {
      int maxNumThreadStates = rarely(r) ? _TestUtil.nextInt(r, 5, 20) // crazy value
          : _TestUtil.nextInt(r, 1, 4); // reasonable value

      Method setIndexerThreadPoolMethod = null;
      try {
        // Retrieve the package-private setIndexerThreadPool
        // method:
        for(Method m : IndexWriterConfig.class.getDeclaredMethods()) {
          if (m.getName().equals("setIndexerThreadPool")) {
            m.setAccessible(true);
            setIndexerThreadPoolMethod = m;
            break;
          }
        }
      } catch (Exception e) {
        // Should not happen?
        throw new RuntimeException(e);
      }

      if (setIndexerThreadPoolMethod == null) {
        throw new RuntimeException("failed to lookup IndexWriterConfig.setIndexerThreadPool method");
      }

      try {
        if (rarely(r)) {
          Class<?> clazz = Class.forName("org.apache.lucene.index.RandomDocumentsWriterPerThreadPool");
          Constructor<?> ctor = clazz.getConstructor(int.class, Random.class);
          ctor.setAccessible(true);
          // random thread pool
          setIndexerThreadPoolMethod.invoke(c, ctor.newInstance(maxNumThreadStates, r));
        } else {
          // random thread pool
          c.setMaxThreadStates(maxNumThreadStates);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    if (rarely(r)) {
      c.setMergePolicy(new MockRandomMergePolicy(r));
    } else if (r.nextBoolean()) {
      c.setMergePolicy(newTieredMergePolicy());
    } else if (r.nextInt(5) == 0) { 
      c.setMergePolicy(newAlcoholicMergePolicy());
    } else {
      c.setMergePolicy(newLogMergePolicy());
    }
    c.setReaderPooling(r.nextBoolean());
    c.setReaderTermsIndexDivisor(_TestUtil.nextInt(r, 1, 4));
    return c;
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
    logmp.setUseCompoundFile(r.nextBoolean());
    logmp.setCalibrateSizeByDeletes(r.nextBoolean());
    if (rarely(r)) {
      logmp.setMergeFactor(_TestUtil.nextInt(r, 2, 9));
    } else {
      logmp.setMergeFactor(_TestUtil.nextInt(r, 10, 50));
    }
    logmp.setUseCompoundFile(r.nextBoolean());
    logmp.setNoCFSRatio(0.1 + r.nextDouble()*0.8);
    if (rarely()) {
      logmp.setMaxCFSSegmentSizeMB(0.2 + r.nextDouble() * 2.0);
    }
    return logmp;
  }

  public static TieredMergePolicy newTieredMergePolicy(Random r) {
    TieredMergePolicy tmp = new TieredMergePolicy();
    if (rarely(r)) {
      tmp.setMaxMergeAtOnce(_TestUtil.nextInt(r, 2, 9));
      tmp.setMaxMergeAtOnceExplicit(_TestUtil.nextInt(r, 2, 9));
    } else {
      tmp.setMaxMergeAtOnce(_TestUtil.nextInt(r, 10, 50));
      tmp.setMaxMergeAtOnceExplicit(_TestUtil.nextInt(r, 10, 50));
    }
    if (rarely(r)) {
      tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
    } else {
      tmp.setMaxMergedSegmentMB(r.nextDouble() * 100);
    }
    tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
    tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
    if (rarely(r)) {
      tmp.setSegmentsPerTier(_TestUtil.nextInt(r, 2, 20));
    } else {
      tmp.setSegmentsPerTier(_TestUtil.nextInt(r, 10, 50));
    }
    tmp.setUseCompoundFile(r.nextBoolean());
    tmp.setNoCFSRatio(0.1 + r.nextDouble()*0.8);
    if (rarely()) {
      tmp.setMaxCFSSegmentSizeMB(0.2 + r.nextDouble() * 2.0);
    }
    tmp.setReclaimDeletesWeight(r.nextDouble()*4);
    return tmp;
  }

  public static LogMergePolicy newLogMergePolicy(boolean useCFS) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setUseCompoundFile(useCFS);
    return logmp;
  }

  public static LogMergePolicy newLogMergePolicy(boolean useCFS, int mergeFactor) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setUseCompoundFile(useCFS);
    logmp.setMergeFactor(mergeFactor);
    return logmp;
  }

  public static LogMergePolicy newLogMergePolicy(int mergeFactor) {
    LogMergePolicy logmp = newLogMergePolicy();
    logmp.setMergeFactor(mergeFactor);
    return logmp;
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
      throw new RuntimeException(e);
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
    if (rarely(random)) {
      directory = new NRTCachingDirectory(directory, random.nextDouble(), random.nextDouble());
    }
    
    if (rarely(random)) { 
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
  
  public static Field newField(Random random, String name, String value, FieldType type) {
    name = new String(name);
    if (usually(random) || !type.indexed()) {
      // most of the time, don't modify the params
      return new Field(name, value, type);
    }

    // TODO: once all core & test codecs can index
    // offsets, sometimes randomly turn on offsets if we are
    // already indexing positions...

    FieldType newType = new FieldType(type);
    if (!newType.stored() && random.nextBoolean()) {
      newType.setStored(true); // randomly store it
    }

    if (!newType.storeTermVectors() && random.nextBoolean()) {
      newType.setStoreTermVectors(true);
      if (!newType.storeTermVectorOffsets()) {
        newType.setStoreTermVectorOffsets(random.nextBoolean());
      }
      if (!newType.storeTermVectorPositions()) {
        newType.setStoreTermVectorPositions(random.nextBoolean());
        
        if (newType.storeTermVectorPositions() && !newType.storeTermVectorPayloads() && !PREFLEX_IMPERSONATION_IS_ACTIVE) {
          newType.setStoreTermVectorPayloads(random.nextBoolean());
        }
      }
    }

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
      d = FSDirectory.open(file);
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
        final File dir = _TestUtil.getTempDir("index");
        dir.mkdirs(); // ensure it's created so we 'have' it.
        return newFSDirectoryImpl(clazz.asSubclass(FSDirectory.class), dir);
      }

      // try empty ctor
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
            final List<String> allFields = new ArrayList<String>();
            for (FieldInfo fi : ar.getFieldInfos()) {
              allFields.add(fi.name);
            }
            Collections.shuffle(allFields, random);
            final int end = allFields.isEmpty() ? 0 : random.nextInt(allFields.size());
            final Set<String> fields = new HashSet<String>(allFields.subList(0, end));
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
      return new IOContext(new MergeInfo(randomNumDocs, Math.max(oldContext.mergeInfo.estimatedMergeBytes, size), random.nextBoolean(), _TestUtil.nextInt(random, 1, 100)));
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
   * threads. if <code>maybeWrap</code> is true, this searcher might wrap the
   * reader with one that returns null for getSequentialSubReaders.
   */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) {
    Random random = random();
    if (usually()) {
      if (maybeWrap) {
        try {
          r = maybeWrapReader(r);
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
      // TODO: this whole check is a coverage hack, we should move it to tests for various filterreaders.
      // ultimately whatever you do will be checkIndex'd at the end anyway. 
      if (random.nextInt(500) == 0 && r instanceof AtomicReader) {
        // TODO: not useful to check DirectoryReader (redundant with checkindex)
        // but maybe sometimes run this on the other crazy readers maybeWrapReader creates?
        try {
          _TestUtil.checkReader(r);
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
      IndexSearcher ret = random.nextBoolean() ? new AssertingIndexSearcher(random, r) : new AssertingIndexSearcher(random, r.getContext());
      ret.setSimilarity(classEnvRule.similarity);
      return ret;
    } else {
      int threads = 0;
      final ThreadPoolExecutor ex;
      if (random.nextBoolean()) {
        ex = null;
      } else {
        threads = _TestUtil.nextInt(random, 1, 8);
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
           _TestUtil.shutdownExecutorService(ex);
         }
       });
      }
      IndexSearcher ret = random.nextBoolean() 
          ? new AssertingIndexSearcher(random, r, ex)
          : new AssertingIndexSearcher(random, r.getContext(), ex);
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
    HashSet<BytesRef> tests = new HashSet<BytesRef>();
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
              tests.add(new BytesRef(_TestUtil.randomSimpleString(random()))); // random term
              break;
            default:
              throw new AssertionError();
          }
        }
      }
      numPasses++;
    }

    rightEnum = rightTerms.iterator(rightEnum);

    ArrayList<BytesRef> shuffledTests = new ArrayList<BytesRef>(tests);
    Collections.shuffle(shuffledTests, random);

    for (BytesRef b : shuffledTests) {
      if (rarely()) {
        // reuse the enums
        leftEnum = leftTerms.iterator(leftEnum);
        rightEnum = rightTerms.iterator(rightEnum);
      }

      final boolean useCache = random().nextBoolean();
      final boolean seekExact = random().nextBoolean();

      if (seekExact) {
        assertEquals(info, leftEnum.seekExact(b, useCache), rightEnum.seekExact(b, useCache));
      } else {
        SeekStatus leftStatus = leftEnum.seekCeil(b, useCache);
        SeekStatus rightStatus = rightEnum.seekCeil(b, useCache);
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
    Set<String> fields = new HashSet<String>();
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
          BytesRef scratchLeft = new BytesRef();
          BytesRef scratchRight = new BytesRef();
          for(int docID=0;docID<leftReader.maxDoc();docID++) {
            leftValues.get(docID, scratchLeft);
            rightValues.get(docID, scratchRight);
            assertEquals(info, scratchLeft, scratchRight);
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
          BytesRef scratchLeft = new BytesRef();
          BytesRef scratchRight = new BytesRef();
          for (int i = 0; i < leftValues.getValueCount(); i++) {
            leftValues.lookupOrd(i, scratchLeft);
            rightValues.lookupOrd(i, scratchRight);
            assertEquals(info, scratchLeft, scratchRight);
          }
          // bytes
          for(int docID=0;docID<leftReader.maxDoc();docID++) {
            leftValues.get(docID, scratchLeft);
            rightValues.get(docID, scratchRight);
            assertEquals(info, scratchLeft, scratchRight);
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
          BytesRef scratchLeft = new BytesRef();
          BytesRef scratchRight = new BytesRef();
          for (int i = 0; i < leftValues.getValueCount(); i++) {
            leftValues.lookupOrd(i, scratchLeft);
            rightValues.lookupOrd(i, scratchRight);
            assertEquals(info, scratchLeft, scratchRight);
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
    TreeSet<String> left = new TreeSet<String>();
    TreeSet<String> right = new TreeSet<String>();
    
    for (FieldInfo fi : leftInfos) {
      left.add(fi.name);
    }
    
    for (FieldInfo fi : rightInfos) {
      right.add(fi.name);
    }
    
    assertEquals(info, left, right);
  }
}
