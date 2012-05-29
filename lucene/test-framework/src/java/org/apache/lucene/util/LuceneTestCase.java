package org.apache.lucene.util;

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

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsInt;

import java.io.*;
import java.lang.annotation.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.ReaderClosedListener;
import org.apache.lucene.search.*;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.search.QueryUtils.FCInvisibleMultiReader;
import org.apache.lucene.store.*;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.*;
import com.carrotsearch.randomizedtesting.annotations.*;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesInvariantRule;

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
@ThreadLeaks(failTestIfLeaking = false)
public abstract class LuceneTestCase extends Assert {

  // -----------------------------------------------------------------
  // Test groups and other annotations modifying tests' behavior.
  // -----------------------------------------------------------------

  /**
   * Annotation for tests that should only be run during nightly builds.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = "tests.nightly")
  public @interface Nightly {}

  /**
   * Annotation for tests that should only be run during weekly builds
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = "tests.weekly")
  public @interface Weekly {}

  /**
   * Annotation for tests which exhibit a known issue and are temporarily disabled.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = "tests.awaitsfix")
  public @interface AwaitsFix {
    /** Point to JIRA entry. */
    public String bugUrl();
  }

  /**
   * Annotation for tests that are really slow and should be run only when specifically 
   * asked to run.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @TestGroup(enabled = false, sysProperty = "tests.slow")
  public @interface Slow {}

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
  public static final Version TEST_VERSION_CURRENT = Version.LUCENE_50;

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

  /** Gets the directory to run tests with */
  public static final String TEST_DIRECTORY = System.getProperty("tests.directory", "random");

  /** the line file used by LineFileDocs */
  public static final String TEST_LINE_DOCS_FILE = System.getProperty("tests.linedocsfile", DEFAULT_LINE_DOCS_FILE);

  /** Whether or not @nightly tests should run. */
  public static final boolean TEST_NIGHTLY = systemPropertyAsBoolean("tests.nightly", false);

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
    "user.timezone"
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
  public static TestRuleMarkFailure suiteFailureMarker;
  
  /**
   * This controls how suite-level rules are nested. It is important that _all_ rules declared
   * in {@link LuceneTestCase} are executed in proper order if they depend on each 
   * other.
   */
  @ClassRule
  public static TestRule classRules = RuleChain
    .outerRule(new TestRuleIgnoreTestSuites())
    .around(suiteFailureMarker = new TestRuleMarkFailure())
    .around(new TestRuleAssertionsRequired())
    .around(new TestRuleNoStaticHooksShadowing())
    .around(new TestRuleNoInstanceHooksOverrides())
    .around(new SystemPropertiesInvariantRule(IGNORED_INVARIANT_PROPERTIES))
    .around(new TestRuleIcuHack())
    .around(classNameRule = new TestRuleStoreClassName())
    .around(new TestRuleReportUncaughtExceptions())
    .around(classEnvRule = new TestRuleSetupAndRestoreClassEnv());


  // -----------------------------------------------------------------
  // Test level rules.
  // -----------------------------------------------------------------

  /** Enforces {@link #setUp()} and {@link #tearDown()} calls are chained. */
  private TestRuleSetupTeardownChained parentChainCallRule = new TestRuleSetupTeardownChained();

  /** Save test thread and name. */
  private TestRuleThreadAndTestName threadAndTestNameRule = new TestRuleThreadAndTestName();

  /** Taint test failures. */
  private TestRuleMarkFailure testFailureMarker = new TestRuleMarkFailure(suiteFailureMarker); 
  
  /**
   * This controls how individual test rules are nested. It is important that
   * _all_ rules declared in {@link LuceneTestCase} are executed in proper order
   * if they depend on each other.
   */
  @Rule
  public final TestRule ruleChain = RuleChain
    .outerRule(testFailureMarker)
    .around(threadAndTestNameRule)
    .around(new TestRuleReportUncaughtExceptions())
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
    IndexReader[] subReaders = reader.getSequentialSubReaders();
    if (subReaders.length != 1)
      throw new IllegalArgumentException(reader + " has " + subReaders.length + " segments instead of exactly one");
    assertTrue(subReaders[0] instanceof SegmentReader);
    return (SegmentReader) subReaders[0];
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
    int p = TEST_NIGHTLY ? 10 : 5;
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
          // random thread pool
          setIndexerThreadPoolMethod.invoke(c, new RandomDocumentsWriterPerThreadPool(maxNumThreadStates, r));
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
   * The Directory is wrapped with {@link MockDirectoryWrapper}.
   * By default this means it will be picky, such as ensuring that you
   * properly close it and all open files in your test. It will emulate
   * some features of Windows, such as not allowing open files to be
   * overwritten.
   */
  public static MockDirectoryWrapper newDirectory() throws IOException {
    return newDirectory(random());
  }

  /**
   * Returns a new Directory instance, using the specified random.
   * See {@link #newDirectory()} for more information.
   */
  public static MockDirectoryWrapper newDirectory(Random r) throws IOException {
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    MockDirectoryWrapper dir = new MockDirectoryWrapper(r, maybeNRTWrap(r, impl));
    closeAfterSuite(new CloseableDirectory(dir, suiteFailureMarker));

    dir.setThrottling(TEST_THROTTLING);
    if (VERBOSE) {
      System.out.println("NOTE: LuceneTestCase.newDirectory: returning " + dir);
    }
    return dir;
   }

  /**
   * Returns a new Directory instance, with contents copied from the
   * provided directory. See {@link #newDirectory()} for more
   * information.
   */
  public static MockDirectoryWrapper newDirectory(Directory d) throws IOException {
    return newDirectory(random(), d);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static MockDirectoryWrapper newFSDirectory(File f) throws IOException {
    return newFSDirectory(f, null);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static MockDirectoryWrapper newFSDirectory(File f, LockFactory lf) throws IOException {
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
      MockDirectoryWrapper dir = new MockDirectoryWrapper(
          random(), maybeNRTWrap(random(), fsdir));
      if (lf != null) {
        dir.setLockFactory(lf);
      }
      closeAfterSuite(new CloseableDirectory(dir, suiteFailureMarker));
      dir.setThrottling(TEST_THROTTLING);
      return dir;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a new Directory instance, using the specified random
   * with contents copied from the provided directory. See 
   * {@link #newDirectory()} for more information.
   */
  public static MockDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    for (String file : d.listAll()) {
     d.copy(impl, file, file, newIOContext(r));
    }
    MockDirectoryWrapper dir = new MockDirectoryWrapper(r, maybeNRTWrap(r, impl));
    closeAfterSuite(new CloseableDirectory(dir, suiteFailureMarker));
    dir.setThrottling(TEST_THROTTLING);
    return dir;
  }
  
  private static Directory maybeNRTWrap(Random random, Directory directory) {
    if (rarely(random)) {
      return new NRTCachingDirectory(directory, random.nextDouble(), random.nextDouble());
    } else {
      return directory;
    }
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
        switch(random.nextInt(4)) {
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
    final int randomNumDocs = random.nextInt(4192);
    final int size = random.nextInt(512) * randomNumDocs;
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

  /**
   * Create a new searcher over the reader. This searcher might randomly use
   * threads.
   */
  public static IndexSearcher newSearcher(IndexReader r) throws IOException {
    return newSearcher(r, true);
  }
  
  /**
   * Create a new searcher over the reader. This searcher might randomly use
   * threads. if <code>maybeWrap</code> is true, this searcher might wrap the
   * reader with one that returns null for getSequentialSubReaders.
   */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) throws IOException {
    Random random = random();
    if (usually()) {
      if (maybeWrap) {
        r = maybeWrapReader(r);
      }
      IndexSearcher ret = random.nextBoolean() ? new AssertingIndexSearcher(random, r) : new AssertingIndexSearcher(random, r.getTopReaderContext());
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
          : new AssertingIndexSearcher(random, r.getTopReaderContext(), ex);
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

  /**
   * @see SuppressCodecs 
   */
  static boolean shouldAvoidCodec(String codec) {
    return classEnvRule.shouldAvoidCodec(codec);
  }
}
