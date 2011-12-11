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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.appending.AppendingCodec;
import org.apache.lucene.index.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.index.codecs.preflexrw.PreFlexRWCodec;
import org.apache.lucene.index.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.search.AssertingIndexSearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.RandomSimilarityProvider;
import org.apache.lucene.search.similarities.SimilarityProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.junit.*;
import org.junit.rules.MethodRule;
import org.junit.rules.TestWatchman;
import org.junit.runner.RunWith;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * Base class for all Lucene unit tests, Junit3 or Junit4 variant.
 * <p>
 * </p>
 * <p>
 * If you
 * override either <code>setUp()</code> or
 * <code>tearDown()</code> in your unit test, make sure you
 * call <code>super.setUp()</code> and
 * <code>super.tearDown()</code>
 * </p>
 *
 * <code>@After</code> - replaces setup
 * <code>@Before</code> - replaces teardown
 * <code>@Test</code> - any public method with this annotation is a test case, regardless
 * of its name
 * <p>
 * <p>
 * See Junit4 <a href="http://junit.org/junit/javadoc/4.7/">documentation</a> for a complete list of features.
 * <p>
 * Import from org.junit rather than junit.framework.
 * <p>
 * You should be able to use this class anywhere you used LuceneTestCase
 * if you annotate your derived class correctly with the annotations above
 * @see #assertSaneFieldCaches(String)
 */

@RunWith(LuceneTestCaseRunner.class)
public abstract class LuceneTestCase extends Assert {

  /**
   * true iff tests are run in verbose mode. Note: if it is false, tests are not
   * expected to print any messages.
   */
  public static final boolean VERBOSE = Boolean.getBoolean("tests.verbose");
  
  public static final boolean INFOSTREAM = Boolean.parseBoolean(System.getProperty("tests.infostream", Boolean.toString(VERBOSE)));

  /** Use this constant when creating Analyzers and any other version-dependent stuff.
   * <p><b>NOTE:</b> Change this when development starts for new Lucene version:
   */
  public static final Version TEST_VERSION_CURRENT = Version.LUCENE_40;

  /**
   * If this is set, it is the only method that should run.
   */
  static final String TEST_METHOD;

  /** Create indexes in this directory, optimally use a subdir, named after the test */
  public static final File TEMP_DIR;
  static {
    String method = System.getProperty("testmethod", "").trim();
    TEST_METHOD = method.length() == 0 ? null : method;
    String s = System.getProperty("tempDir", System.getProperty("java.io.tmpdir"));
    if (s == null)
      throw new RuntimeException("To run tests, you need to define system property 'tempDir' or 'java.io.tmpdir'.");
    TEMP_DIR = new File(s);
    TEMP_DIR.mkdirs();
  }
  
  /** set of directories we created, in afterclass we try to clean these up */
  private static final Map<File, StackTraceElement[]> tempDirs = Collections.synchronizedMap(new HashMap<File, StackTraceElement[]>());

  // by default we randomly pick a different codec for
  // each test case (non-J4 tests) and each test class (J4
  // tests)
  /** Gets the codec to run tests with. */
  public static final String TEST_CODEC = System.getProperty("tests.codec", "random");
  /** Gets the postingsFormat to run tests with. */
  public static final String TEST_POSTINGSFORMAT = System.getProperty("tests.postingsformat", "random");
  /** Gets the locale to run tests with */
  public static final String TEST_LOCALE = System.getProperty("tests.locale", "random");
  /** Gets the timezone to run tests with */
  public static final String TEST_TIMEZONE = System.getProperty("tests.timezone", "random");
  /** Gets the directory to run tests with */
  public static final String TEST_DIRECTORY = System.getProperty("tests.directory", "random");
  /** Get the number of times to run tests */
  public static final int TEST_ITER = Integer.parseInt(System.getProperty("tests.iter", "1"));
  /** Get the minimum number of times to run tests until a failure happens */
  public static final int TEST_ITER_MIN = Integer.parseInt(System.getProperty("tests.iter.min", Integer.toString(TEST_ITER)));
  /** Get the random seed for tests */
  public static final String TEST_SEED = System.getProperty("tests.seed", "random");
  /** whether or not @nightly tests should run */
  public static final boolean TEST_NIGHTLY = Boolean.parseBoolean(System.getProperty("tests.nightly", "false"));
  /** whether or not @weekly tests should run */
  public static final boolean TEST_WEEKLY = Boolean.parseBoolean(System.getProperty("tests.weekly", "false"));
  /** whether or not @slow tests should run */
  public static final boolean TEST_SLOW = Boolean.parseBoolean(System.getProperty("tests.slow", "false"));
  /** the line file used by LineFileDocs */
  public static final String TEST_LINE_DOCS_FILE = System.getProperty("tests.linedocsfile", "europarl.lines.txt.gz");
  /** whether or not to clean threads between test invocations: "false", "perMethod", "perClass" */
  public static final String TEST_CLEAN_THREADS = System.getProperty("tests.cleanthreads", "perClass");
  /** whether or not to clean threads between test invocations: "false", "perMethod", "perClass" */
  public static final Throttling TEST_THROTTLING = TEST_NIGHTLY ? Throttling.SOMETIMES : Throttling.NEVER;

  /**
   * A random multiplier which you should use when writing random tests:
   * multiply it by the number of iterations
   */
  public static final int RANDOM_MULTIPLIER = Integer.parseInt(System.getProperty("tests.multiplier", "1"));

  /** @lucene.internal */
  public static boolean PREFLEX_IMPERSONATION_IS_ACTIVE;

  private int savedBoolMaxClauseCount = BooleanQuery.getMaxClauseCount();

  private volatile Thread.UncaughtExceptionHandler savedUncaughtExceptionHandler = null;

  /** Used to track if setUp and tearDown are called correctly from subclasses */
  private static State state = State.INITIAL;

  private static enum State {
    INITIAL, // no tests ran yet
    SETUP,   // test has called setUp()
    RANTEST, // test is running
    TEARDOWN // test has called tearDown()
  }
  
  /**
   * Some tests expect the directory to contain a single segment, and want to do tests on that segment's reader.
   * This is an utility method to help them.
   */
  public static SegmentReader getOnlySegmentReader(IndexReader reader) {
    if (reader instanceof SegmentReader)
      return (SegmentReader) reader;

    IndexReader[] subReaders = reader.getSequentialSubReaders();
    if (subReaders.length != 1)
      throw new IllegalArgumentException(reader + " has " + subReaders.length + " segments instead of exactly one");

    return (SegmentReader) subReaders[0];
  }

  private static class UncaughtExceptionEntry {
    public final Thread thread;
    public final Throwable exception;

    public UncaughtExceptionEntry(Thread thread, Throwable exception) {
      this.thread = thread;
      this.exception = exception;
    }
  }
  private List<UncaughtExceptionEntry> uncaughtExceptions = Collections.synchronizedList(new ArrayList<UncaughtExceptionEntry>());

  // default codec
  private static Codec savedCodec;
  
  private static InfoStream savedInfoStream;

  private static SimilarityProvider similarityProvider;

  private static Locale locale;
  private static Locale savedLocale;
  private static TimeZone timeZone;
  private static TimeZone savedTimeZone;

  protected static Map<MockDirectoryWrapper,StackTraceElement[]> stores;

  /** @deprecated (4.0) until we fix no-fork problems in solr tests */
  @Deprecated
  static List<String> testClassesRun = new ArrayList<String>();

  private static void initRandom() {
    assert !random.initialized;
    staticSeed = "random".equals(TEST_SEED) ? seedRand.nextLong() : ThreeLongs.fromString(TEST_SEED).l1;
    random.setSeed(staticSeed);
    random.initialized = true;
  }
  
  @Deprecated
  private static boolean icuTested = false;

  @BeforeClass
  public static void beforeClassLuceneTestCaseJ4() {
    initRandom();
    state = State.INITIAL;
    tempDirs.clear();
    stores = Collections.synchronizedMap(new IdentityHashMap<MockDirectoryWrapper,StackTraceElement[]>());
    
    // enable this by default, for IDE consistency with ant tests (as its the default from ant)
    // TODO: really should be in solr base classes, but some extend LTC directly.
    // we do this in beforeClass, because some tests currently disable it
    if (System.getProperty("solr.directoryFactory") == null) {
      System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockDirectoryFactory");
    }
    
    // if verbose: print some debugging stuff about which codecs are loaded
    if (VERBOSE) {
      Set<String> codecs = Codec.availableCodecs();
      for (String codec : codecs) {
        System.out.println("Loaded codec: '" + codec + "': " + Codec.forName(codec).getClass().getName());
      }
      
      Set<String> postingsFormats = PostingsFormat.availablePostingsFormats();
      for (String postingsFormat : postingsFormats) {
        System.out.println("Loaded postingsFormat: '" + postingsFormat + "': " + PostingsFormat.forName(postingsFormat).getClass().getName());
      }
    }
    
    savedInfoStream = InfoStream.getDefault();
    if (INFOSTREAM) {
      // consume random for consistency
      random.nextBoolean();
      InfoStream.setDefault(new PrintStreamInfoStream(System.out));
    } else {
      if (random.nextBoolean()) {
        InfoStream.setDefault(new NullInfoStream());
      }
    }

    PREFLEX_IMPERSONATION_IS_ACTIVE = false;
    savedCodec = Codec.getDefault();
    final Codec codec;
    int randomVal = random.nextInt(10);
    
    if ("Lucene3x".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal < 2)) { // preflex-only setup
      codec = new PreFlexRWCodec();
      PREFLEX_IMPERSONATION_IS_ACTIVE = true;
    } else if ("SimpleText".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 9)) {
      codec = new SimpleTextCodec();
    } else if ("Appending".equals(TEST_CODEC) || ("random".equals(TEST_CODEC) && randomVal == 8)) {
      codec = new AppendingCodec();
    } else if (!"random".equals(TEST_CODEC)) {
      codec = Codec.forName(TEST_CODEC);
    } else if ("random".equals(TEST_POSTINGSFORMAT)) {
      codec = new RandomCodec(random, useNoMemoryExpensiveCodec);
    } else {
      codec = new Lucene40Codec() {
        private final PostingsFormat format = PostingsFormat.forName(TEST_POSTINGSFORMAT);
        
        @Override
        public PostingsFormat getPostingsFormatForField(String field) {
          return format;
        }

        @Override
        public String toString() {
          return super.toString() + ": " + format.toString();
        }
      };
    }

    Codec.setDefault(codec);
    
    savedLocale = Locale.getDefault();
    
    // START hack to init ICU safely before we randomize locales.
    // ICU fails during classloading when a special Java7-only locale is the default
    // see: http://bugs.icu-project.org/trac/ticket/8734
    if (!icuTested) {
      icuTested = true;
      try {
        Locale.setDefault(Locale.US);
        Class.forName("com.ibm.icu.util.ULocale");
      } catch (ClassNotFoundException cnfe) {
        // ignore if no ICU is in classpath
      }
    }
    // END hack
    
    locale = TEST_LOCALE.equals("random") ? randomLocale(random) : localeForName(TEST_LOCALE);
    Locale.setDefault(locale);
    savedTimeZone = TimeZone.getDefault();
    timeZone = TEST_TIMEZONE.equals("random") ? randomTimeZone(random) : TimeZone.getTimeZone(TEST_TIMEZONE);
    TimeZone.setDefault(timeZone);
    similarityProvider = new RandomSimilarityProvider(random);
    testsFailed = false;
  }

  @AfterClass
  public static void afterClassLuceneTestCaseJ4() {
    State oldState = state; // capture test execution state
    state = State.INITIAL; // set the state for subsequent tests
    
    Throwable problem = null;
    try {
      if (!testsFailed) {
        assertTrue("ensure your setUp() calls super.setUp() and your tearDown() calls super.tearDown()!!!", 
          oldState == State.INITIAL || oldState == State.TEARDOWN);
      }
    } catch (Throwable t) {
      if (problem == null) problem = t;
    }
    
    if (! "false".equals(TEST_CLEAN_THREADS)) {
      int rogueThreads = threadCleanup("test class");
      if (rogueThreads > 0) {
        // TODO: fail here once the leaks are fixed.
        System.err.println("RESOURCE LEAK: test class left " + rogueThreads + " thread(s) running");
      }
    }
    
    String codecDescription = Codec.getDefault().toString();
    Codec.setDefault(savedCodec);
    InfoStream.setDefault(savedInfoStream);
    Locale.setDefault(savedLocale);
    TimeZone.setDefault(savedTimeZone);
    System.clearProperty("solr.solr.home");
    System.clearProperty("solr.data.dir");
    
    try {
      // now look for unclosed resources
      if (!testsFailed) {
        checkResourcesAfterClass();
      }
    } catch (Throwable t) {
      if (problem == null) problem = t;
    }
    
    stores = null;

    try {
      // clear out any temp directories if we can
      if (!testsFailed) {
        clearTempDirectoriesAfterClass();
      }
    } catch (Throwable t) {
      if (problem == null) problem = t;
    }

    // if we had afterClass failures, get some debugging information
    if (problem != null) {
      reportPartialFailureInfo();      
    }
    
    // if verbose or tests failed, report some information back
    if (VERBOSE || testsFailed || problem != null) {
      printDebuggingInformation(codecDescription);
    }
    
    // reset seed
    random.setSeed(0L);
    random.initialized = false;
    
    if (problem != null) {
      throw new RuntimeException(problem);
    }
  }
  
  /** print some useful debugging information about the environment */
  private static void printDebuggingInformation(String codecDescription) {
    System.err.println("NOTE: test params are: codec=" + codecDescription +
        ", sim=" + similarityProvider +
        ", locale=" + locale +
        ", timezone=" + (timeZone == null ? "(null)" : timeZone.getID()));
    System.err.println("NOTE: all tests run in this JVM:");
    System.err.println(Arrays.toString(testClassesRun.toArray()));
    System.err.println("NOTE: " + System.getProperty("os.name") + " "
        + System.getProperty("os.version") + " "
        + System.getProperty("os.arch") + "/"
        + System.getProperty("java.vendor") + " "
        + System.getProperty("java.version") + " "
        + (Constants.JRE_IS_64BIT ? "(64-bit)" : "(32-bit)") + "/"
        + "cpus=" + Runtime.getRuntime().availableProcessors() + ","
        + "threads=" + Thread.activeCount() + ","
        + "free=" + Runtime.getRuntime().freeMemory() + ","
        + "total=" + Runtime.getRuntime().totalMemory());
  }
  
  /** check that directories and their resources were closed */
  private static void checkResourcesAfterClass() {
    for (MockDirectoryWrapper d : stores.keySet()) {
      if (d.isOpen()) {
        StackTraceElement elements[] = stores.get(d);
        // Look for the first class that is not LuceneTestCase that requested
        // a Directory. The first two items are of Thread's, so skipping over
        // them.
        StackTraceElement element = null;
        for (int i = 2; i < elements.length; i++) {
          StackTraceElement ste = elements[i];
          if (ste.getClassName().indexOf("LuceneTestCase") == -1) {
            element = ste;
            break;
          }
        }
        fail("directory of test was not closed, opened from: " + element);
      }
    }
  }
  
  /** clear temp directories: this will fail if its not successful */
  private static void clearTempDirectoriesAfterClass() {
    for (Entry<File, StackTraceElement[]> entry : tempDirs.entrySet()) {
      try {
        _TestUtil.rmDir(entry.getKey());
      } catch (IOException e) {
        e.printStackTrace();
        System.err.println("path " + entry.getKey() + " allocated from");
        // first two STE's are Java's
        StackTraceElement[] elements = entry.getValue();
        for (int i = 2; i < elements.length; i++) {
          StackTraceElement ste = elements[i];            
          // print only our code's stack information
          if (ste.getClassName().indexOf("org.apache.lucene") == -1) break; 
          System.err.println("\t" + ste);
        }
        fail("could not remove temp dir: " + entry.getKey());
      }
    }
  }

  protected static boolean testsFailed; /* true if any tests failed */

  // This is how we get control when errors occur.
  // Think of this as start/end/success/failed
  // events.
  @Rule
  public final TestWatchman intercept = new TestWatchman() {

    @Override
    public void failed(Throwable e, FrameworkMethod method) {
      // org.junit.internal.AssumptionViolatedException in older releases
      // org.junit.Assume.AssumptionViolatedException in recent ones
      if (e.getClass().getName().endsWith("AssumptionViolatedException")) {
        if (e.getCause() instanceof _TestIgnoredException)
          e = e.getCause();
        System.err.print("NOTE: Assume failed in '" + method.getName() + "' (ignored):");
        if (VERBOSE) {
          System.err.println();
          e.printStackTrace(System.err);
        } else {
          System.err.print(" ");
          System.err.println(e.getMessage());
        }
      } else {
        testsFailed = true;
        reportAdditionalFailureInfo();
      }
      super.failed(e, method);
    }

    @Override
    public void starting(FrameworkMethod method) {
      // set current method name for logging
      LuceneTestCase.this.name = method.getName();
      State s = state; // capture test execution state
      state = State.RANTEST; // set the state for subsequent tests
      if (!testsFailed) {
        assertTrue("ensure your setUp() calls super.setUp()!!!", s == State.SETUP);
      }
      super.starting(method);
    }
  };
  
  /** 
   * The thread executing the current test case.
   * @see #isTestThread()
   */
  volatile Thread testCaseThread;

  /** @see #testCaseThread */
  @Rule
  public final MethodRule setTestThread = new MethodRule() {
    public Statement apply(final Statement s, FrameworkMethod fm, Object target) {
      return new Statement() {
        public void evaluate() throws Throwable {
          try {
            LuceneTestCase.this.testCaseThread = Thread.currentThread();
            s.evaluate();
          } finally {
            LuceneTestCase.this.testCaseThread = null;
          }
        }
      };
    }
  };

  @Before
  public void setUp() throws Exception {
    seed = "random".equals(TEST_SEED) ? seedRand.nextLong() : ThreeLongs.fromString(TEST_SEED).l2;
    random.setSeed(seed);
    State s = state; // capture test execution state
    state = State.SETUP; // set the state for subsequent tests
   
    savedUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        testsFailed = true;
        uncaughtExceptions.add(new UncaughtExceptionEntry(t, e));
        if (savedUncaughtExceptionHandler != null)
          savedUncaughtExceptionHandler.uncaughtException(t, e);
        }
    });

    savedBoolMaxClauseCount = BooleanQuery.getMaxClauseCount();

    if (!testsFailed) {
      assertTrue("ensure your tearDown() calls super.tearDown()!!!", (s == State.INITIAL || s == State.TEARDOWN));
    }
    
    if (useNoMemoryExpensiveCodec) {
      String defFormat = _TestUtil.getPostingsFormat("thisCodeMakesAbsolutelyNoSenseCanWeDeleteIt");
      // Stupid: assumeFalse in setUp() does not print any information, because
      // TestWatchman does not watch test during setUp() - getName() is also not defined...
      // => print info directly and use assume without message:
      if ("SimpleText".equals(defFormat) || "Memory".equals(defFormat)) {
        System.err.println("NOTE: A test method in " + getClass().getSimpleName() + " was ignored, as it uses too much memory with " + defFormat + ".");
        Assume.assumeTrue(false);
      }
    }
  }

  /**
   * Forcible purges all cache entries from the FieldCache.
   * <p>
   * This method will be called by tearDown to clean up FieldCache.DEFAULT.
   * If a (poorly written) test has some expectation that the FieldCache
   * will persist across test methods (ie: a static IndexReader) this
   * method can be overridden to do nothing.
   * </p>
   *
   * @see FieldCache#purgeAllCaches()
   */
  protected void purgeFieldCache(final FieldCache fc) {
    fc.purgeAllCaches();
  }

  protected String getTestLabel() {
    return getClass().getName() + "." + getName();
  }

  /**
   * Returns true if and only if the calling thread is the primary thread 
   * executing the test case. 
   */
  protected boolean isTestThread() {
    assertNotNull("Test case thread not set?", testCaseThread);
    return Thread.currentThread() == testCaseThread;
  }

  @After
  public void tearDown() throws Exception {
    State oldState = state; // capture test execution state
    state = State.TEARDOWN; // set the state for subsequent tests
    
    // NOTE: with junit 4.7, we don't get a reproduceWith because our Watchman
    // does not know if something fails in tearDown. so we ensure this happens ourselves for now.
    // we can remove this if we upgrade to 4.8
    Throwable problem = null;
    
    try {
      if (!testsFailed) {
        // Note: we allow a test to go straight from SETUP -> TEARDOWN (without ever entering the RANTEST state)
        // because if you assume() inside setUp(), it skips the test and the TestWatchman has no way to know...
        assertTrue("ensure your setUp() calls super.setUp()!!!", oldState == State.RANTEST || oldState == State.SETUP);
      }
    } catch (Throwable t) {
      if (problem == null) problem = t;
    }

    BooleanQuery.setMaxClauseCount(savedBoolMaxClauseCount);

    // this won't throw any exceptions or fail the test
    // if we change this, then change this logic
    checkRogueThreadsAfter();
    // restore the default uncaught exception handler
    Thread.setDefaultUncaughtExceptionHandler(savedUncaughtExceptionHandler);
    
    try {
      checkUncaughtExceptionsAfter();
    } catch (Throwable t) {
      if (problem == null) problem = t;
    }
    
    try {
      // calling assertSaneFieldCaches here isn't as useful as having test
      // classes call it directly from the scope where the index readers
      // are used, because they could be gc'ed just before this tearDown
      // method is called.
      //
      // But it's better then nothing.
      //
      // If you are testing functionality that you know for a fact
      // "violates" FieldCache sanity, then you should either explicitly
      // call purgeFieldCache at the end of your test method, or refactor
      // your Test class so that the inconsistant FieldCache usages are
      // isolated in distinct test methods
      assertSaneFieldCaches(getTestLabel());
    } catch (Throwable t) {
      if (problem == null) problem = t;
    }
    
    purgeFieldCache(FieldCache.DEFAULT);
    
    if (problem != null) {
      testsFailed = true;
      reportAdditionalFailureInfo();
      throw new RuntimeException(problem);
    }
  }
  
  /** check if the test still has threads running, we don't want them to 
   *  fail in a subsequent test and pass the blame to the wrong test */
  private void checkRogueThreadsAfter() {
    if ("perMethod".equals(TEST_CLEAN_THREADS)) {
      int rogueThreads = threadCleanup("test method: '" + getName() + "'");
      if (!testsFailed && rogueThreads > 0) {
        System.err.println("RESOURCE LEAK: test method: '" + getName()
            + "' left " + rogueThreads + " thread(s) running");
        // TODO: fail, but print seed for now
        if (uncaughtExceptions.isEmpty()) {
          reportAdditionalFailureInfo();
        }
      }
    }
  }
  
  /** see if any other threads threw uncaught exceptions, and fail the test if so */
  private void checkUncaughtExceptionsAfter() {
    if (!uncaughtExceptions.isEmpty()) {
      System.err.println("The following exceptions were thrown by threads:");
      for (UncaughtExceptionEntry entry : uncaughtExceptions) {
        System.err.println("*** Thread: " + entry.thread.getName() + " ***");
        entry.exception.printStackTrace(System.err);
      }
      fail("Some threads threw uncaught exceptions!");
    }
  }

  private final static int THREAD_STOP_GRACE_MSEC = 50;
  // jvm-wide list of 'rogue threads' we found, so they only get reported once.
  private final static IdentityHashMap<Thread,Boolean> rogueThreads = new IdentityHashMap<Thread,Boolean>();

  static {
    // just a hack for things like eclipse test-runner threads
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      rogueThreads.put(t, true);
    }
    
    if (TEST_ITER > 1) {
      System.out.println("WARNING: you are using -Dtests.iter=n where n > 1, not all tests support this option.");
      System.out.println("Some may crash or fail: this is not a bug.");
    }
  }

  /**
   * Looks for leftover running threads, trying to kill them off,
   * so they don't fail future tests.
   * returns the number of rogue threads that it found.
   */
  private static int threadCleanup(String context) {
    // educated guess
    Thread[] stillRunning = new Thread[Thread.activeCount()+1];
    int threadCount = 0;
    int rogueCount = 0;

    if ((threadCount = Thread.enumerate(stillRunning)) > 1) {
      while (threadCount == stillRunning.length) {
        // truncated response
        stillRunning = new Thread[stillRunning.length*2];
        threadCount = Thread.enumerate(stillRunning);
      }

      for (int i = 0; i < threadCount; i++) {
        Thread t = stillRunning[i];

        if (t.isAlive() &&
            !rogueThreads.containsKey(t) &&
            t != Thread.currentThread() &&
            /* its ok to keep your searcher across test cases */
            (t.getName().startsWith("LuceneTestCase") && context.startsWith("test method")) == false) {
          System.err.println("WARNING: " + context  + " left thread running: " + t);
          rogueThreads.put(t, true);
          rogueCount++;
          if (t.getName().startsWith("LuceneTestCase")) {
            System.err.println("PLEASE CLOSE YOUR INDEXSEARCHERS IN YOUR TEST!!!!");
            continue;
          } else {
            // wait on the thread to die of natural causes
            try {
              t.join(THREAD_STOP_GRACE_MSEC);
            } catch (InterruptedException e) { e.printStackTrace(); }
          }
          // try to stop the thread:
          t.setUncaughtExceptionHandler(null);
          Thread.setDefaultUncaughtExceptionHandler(null);
          if (!t.getName().startsWith("SyncThread")) // avoid zookeeper jre crash
            t.interrupt();
        }
      }
    }
    return rogueCount;
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
  protected void assertSaneFieldCaches(final String msg) {
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
    int min = (TEST_NIGHTLY ? 3*i : i) * RANDOM_MULTIPLIER;
    int max = min+(min/2);
    return _TestUtil.nextInt(random, min, max);
  }
  
  public static int atLeast(int i) {
    return atLeast(random, i);
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
    return rarely(random);
  }
  
  public static boolean usually(Random random) {
    return !rarely(random);
  }
  
  public static boolean usually() {
    return usually(random);
  }

  public static void assumeTrue(String msg, boolean b) {
    Assume.assumeNoException(b ? null : new _TestIgnoredException(msg));
  }

  public static void assumeFalse(String msg, boolean b) {
    assumeTrue(msg, !b);
  }

  public static void assumeNoException(String msg, Exception e) {
    Assume.assumeNoException(e == null ? null : new _TestIgnoredException(msg, e));
  }

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
    return newIndexWriterConfig(random, v, a);
  }
  
  /** create a new index writer config with random defaults using the specified random */
  public static IndexWriterConfig newIndexWriterConfig(Random r, Version v, Analyzer a) {
    IndexWriterConfig c = new IndexWriterConfig(v, a);
    c.setSimilarityProvider(similarityProvider);
    if (r.nextBoolean()) {
      c.setMergeScheduler(new SerialMergeScheduler());
    }
    if (r.nextBoolean()) {
      if (rarely(r)) {
        // crazy value
        c.setMaxBufferedDocs(_TestUtil.nextInt(r, 2, 7));
      } else {
        // reasonable value
        c.setMaxBufferedDocs(_TestUtil.nextInt(r, 8, 1000));
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
      c.setIndexerThreadPool(new ThreadAffinityDocumentsWriterThreadPool(_TestUtil.nextInt(r, 1, 20)));
    }

    if (r.nextBoolean()) {
      c.setMergePolicy(newTieredMergePolicy());
    } else if (r.nextBoolean()) {
      c.setMergePolicy(newLogMergePolicy());
    } else {
      c.setMergePolicy(new MockRandomMergePolicy(r));
    }

    c.setReaderPooling(r.nextBoolean());
    c.setReaderTermsIndexDivisor(_TestUtil.nextInt(r, 1, 4));
    return c;
  }

  public static LogMergePolicy newLogMergePolicy() {
    return newLogMergePolicy(random);
  }

  public static TieredMergePolicy newTieredMergePolicy() {
    return newTieredMergePolicy(random);
  }

  public static LogMergePolicy newLogMergePolicy(Random r) {
    LogMergePolicy logmp = r.nextBoolean() ? new LogDocMergePolicy() : new LogByteSizeMergePolicy();
    logmp.setUseCompoundFile(r.nextBoolean());
    logmp.setCalibrateSizeByDeletes(r.nextBoolean());
    if (rarely(r)) {
      logmp.setMergeFactor(_TestUtil.nextInt(r, 2, 4));
    } else {
      logmp.setMergeFactor(_TestUtil.nextInt(r, 5, 50));
    }
    return logmp;
  }

  public static TieredMergePolicy newTieredMergePolicy(Random r) {
    TieredMergePolicy tmp = new TieredMergePolicy();
    if (rarely(r)) {
      tmp.setMaxMergeAtOnce(_TestUtil.nextInt(r, 2, 4));
      tmp.setMaxMergeAtOnceExplicit(_TestUtil.nextInt(r, 2, 4));
    } else {
      tmp.setMaxMergeAtOnce(_TestUtil.nextInt(r, 5, 50));
      tmp.setMaxMergeAtOnceExplicit(_TestUtil.nextInt(r, 5, 50));
    }
    tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
    tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
    tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
    tmp.setSegmentsPerTier(_TestUtil.nextInt(r, 2, 20));
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
    return newDirectory(random);
  }

  /**
   * Returns a new Directory instance, using the specified random.
   * See {@link #newDirectory()} for more information.
   */
  public static MockDirectoryWrapper newDirectory(Random r) throws IOException {
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    MockDirectoryWrapper dir = new MockDirectoryWrapper(r, impl);
    stores.put(dir, Thread.currentThread().getStackTrace());
    dir.setThrottling(TEST_THROTTLING);
    return dir;
   }

  /**
   * Returns a new Directory instance, with contents copied from the
   * provided directory. See {@link #newDirectory()} for more
   * information.
   */
  public static MockDirectoryWrapper newDirectory(Directory d) throws IOException {
    return newDirectory(random, d);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static MockDirectoryWrapper newFSDirectory(File f) throws IOException {
    return newFSDirectory(f, null);
  }

  /** Returns a new FSDirectory instance over the given file, which must be a folder. */
  public static MockDirectoryWrapper newFSDirectory(File f, LockFactory lf) throws IOException {
    String fsdirClass = TEST_DIRECTORY;
    if (fsdirClass.equals("random")) {
      fsdirClass = FS_DIRECTORIES[random.nextInt(FS_DIRECTORIES.length)];
    }

    if (fsdirClass.indexOf(".") == -1) {// if not fully qualified, assume .store
      fsdirClass = "org.apache.lucene.store." + fsdirClass;
    }

    Class<? extends FSDirectory> clazz;
    try {
      try {
        clazz = Class.forName(fsdirClass).asSubclass(FSDirectory.class);
      } catch (ClassCastException e) {
        // TEST_DIRECTORY is not a sub-class of FSDirectory, so draw one at random
        fsdirClass = FS_DIRECTORIES[random.nextInt(FS_DIRECTORIES.length)];

        if (fsdirClass.indexOf(".") == -1) {// if not fully qualified, assume .store
          fsdirClass = "org.apache.lucene.store." + fsdirClass;
        }

        clazz = Class.forName(fsdirClass).asSubclass(FSDirectory.class);
      }
      MockDirectoryWrapper dir = new MockDirectoryWrapper(random, newFSDirectoryImpl(clazz, f));
      if (lf != null) {
        dir.setLockFactory(lf);
      }
      stores.put(dir, Thread.currentThread().getStackTrace());
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
    MockDirectoryWrapper dir = new MockDirectoryWrapper(r, impl);
    stores.put(dir, Thread.currentThread().getStackTrace());
    dir.setThrottling(TEST_THROTTLING);
    return dir;
  }
  
  public static Field newField(String name, String value, FieldType type) {
    return newField(random, name, value, type);
  }
  
  public static Field newField(Random random, String name, String value, FieldType type) {
    if (usually(random) || !type.indexed()) {
      // most of the time, don't modify the params
      return new Field(name, value, type);
    }

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
  
  /** return a random Locale from the available locales on the system */
  public static Locale randomLocale(Random random) {
    Locale locales[] = Locale.getAvailableLocales();
    return locales[random.nextInt(locales.length)];
  }

  /** return a random TimeZone from the available timezones on the system */
  public static TimeZone randomTimeZone(Random random) {
    String tzIds[] = TimeZone.getAvailableIDs();
    return TimeZone.getTimeZone(tzIds[random.nextInt(tzIds.length)]);
  }

  /** return a Locale object equivalent to its programmatic name */
  public static Locale localeForName(String localeName) {
    String elements[] = localeName.split("\\_");
    switch(elements.length) {
      case 3: return new Locale(elements[0], elements[1], elements[2]);
      case 2: return new Locale(elements[0], elements[1]);
      case 1: return new Locale(elements[0]);
      default: throw new IllegalArgumentException("Invalid Locale: " + localeName);
    }
  }

  private static final String FS_DIRECTORIES[] = {
    "SimpleFSDirectory",
    "NIOFSDirectory",
    "MMapDirectory"
  };

  private static final String CORE_DIRECTORIES[] = {
    "RAMDirectory",
    FS_DIRECTORIES[0], FS_DIRECTORIES[1], FS_DIRECTORIES[2]
  };

  public static String randomDirectory(Random random) {
    if (rarely(random)) {
      return CORE_DIRECTORIES[random.nextInt(CORE_DIRECTORIES.length)];
    } else {
      return "RAMDirectory";
    }
  }

  private static Directory newFSDirectoryImpl(
      Class<? extends FSDirectory> clazz, File file)
      throws IOException {
    FSDirectory d = null;
    try {
      // Assuming every FSDirectory has a ctor(File), but not all may take a
      // LockFactory too, so setting it afterwards.
      Constructor<? extends FSDirectory> ctor = clazz.getConstructor(File.class);
      d = ctor.newInstance(file);
    } catch (Exception e) {
      d = FSDirectory.open(file);
    }
    return d;
  }

  /**
   * Registers a temp directory that will be deleted when tests are done. This
   * is used by {@link _TestUtil#getTempDir(String)} and
   * {@link _TestUtil#unzip(File, File)}, so you should call these methods when
   * possible.
   */
  static void registerTempDir(File tmpFile) {
    tempDirs.put(tmpFile.getAbsoluteFile(), Thread.currentThread().getStackTrace());
  }
  
  static Directory newDirectoryImpl(Random random, String clazzName) {
    if (clazzName.equals("random"))
      clazzName = randomDirectory(random);
    if (clazzName.indexOf(".") == -1) // if not fully qualified, assume .store
      clazzName = "org.apache.lucene.store." + clazzName;
    try {
      final Class<? extends Directory> clazz = Class.forName(clazzName).asSubclass(Directory.class);
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

  /** create a new searcher over the reader.
   * This searcher might randomly use threads. */
  public static IndexSearcher newSearcher(IndexReader r) throws IOException {
    return newSearcher(r, true);
  }
  
  /** create a new searcher over the reader.
   * This searcher might randomly use threads.
   * if <code>maybeWrap</code> is true, this searcher might wrap the reader
   * with one that returns null for getSequentialSubReaders.
   */
  public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) throws IOException {
    if (random.nextBoolean()) {
      if (maybeWrap && rarely()) {
        r = new SlowMultiReaderWrapper(r);
      }
      IndexSearcher ret = random.nextBoolean() ? new AssertingIndexSearcher(random, r) : new AssertingIndexSearcher(random, r.getTopReaderContext());
      ret.setSimilarityProvider(similarityProvider);
      return ret;
    } else {
      int threads = 0;
      final ExecutorService ex = (random.nextBoolean()) ? null
          : Executors.newFixedThreadPool(threads = _TestUtil.nextInt(random, 1, 8),
                      new NamedThreadFactory("LuceneTestCase"));
      if (ex != null && VERBOSE) {
        System.out.println("NOTE: newSearcher using ExecutorService with " + threads + " threads");
      }
      IndexSearcher ret = random.nextBoolean() ? 
        new AssertingIndexSearcher(random, r, ex) {
          @Override
          public void close() throws IOException {
            super.close();
            shutdownExecutorService(ex);
          }
        } : new AssertingIndexSearcher(random, r.getTopReaderContext(), ex) {
          @Override
          public void close() throws IOException {
            super.close();
            shutdownExecutorService(ex);
          }
        };
      ret.setSimilarityProvider(similarityProvider);
      return ret;
    }
  }
  
  static void shutdownExecutorService(ExecutorService ex) {
    if (ex != null) {
      ex.shutdown();
      try {
        ex.awaitTermination(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public String getName() {
    return this.name;
  }

  /** Gets a resource from the classpath as {@link File}. This method should only be used,
   * if a real file is needed. To get a stream, code should prefer
   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
   */

  protected File getDataFile(String name) throws IOException {
    try {
      return new File(this.getClass().getResource(name).toURI());
    } catch (Exception e) {
      throw new IOException("Cannot find resource: " + name);
    }
  }

  // We get here from InterceptTestCaseEvents on the 'failed' event....
  public static void reportPartialFailureInfo() {
    System.err.println("NOTE: reproduce with (hopefully): ant test -Dtestcase=" + testClassesRun.get(testClassesRun.size()-1)
        + " -Dtests.seed=" + new ThreeLongs(staticSeed, 0L, LuceneTestCaseRunner.runnerSeed)
        + reproduceWithExtraParams());
  }
  
  // We get here from InterceptTestCaseEvents on the 'failed' event....
  public void reportAdditionalFailureInfo() {
    System.err.println("NOTE: reproduce with: ant test -Dtestcase=" + getClass().getSimpleName()
        + " -Dtestmethod=" + getName() + " -Dtests.seed=" + new ThreeLongs(staticSeed, seed, LuceneTestCaseRunner.runnerSeed)
        + reproduceWithExtraParams());
  }

  // extra params that were overridden needed to reproduce the command
  private static String reproduceWithExtraParams() {
    StringBuilder sb = new StringBuilder();
    if (!TEST_CODEC.equals("random")) sb.append(" -Dtests.codec=").append(TEST_CODEC);
    if (!TEST_POSTINGSFORMAT.equals("random")) sb.append(" -Dtests.postingsformat=").append(TEST_POSTINGSFORMAT);
    if (!TEST_LOCALE.equals("random")) sb.append(" -Dtests.locale=").append(TEST_LOCALE);
    if (!TEST_TIMEZONE.equals("random")) sb.append(" -Dtests.timezone=").append(TEST_TIMEZONE);
    if (!TEST_DIRECTORY.equals("random")) sb.append(" -Dtests.directory=").append(TEST_DIRECTORY);
    if (RANDOM_MULTIPLIER > 1) sb.append(" -Dtests.multiplier=").append(RANDOM_MULTIPLIER);
    if (TEST_NIGHTLY) sb.append(" -Dtests.nightly=true");
    // TODO we can't randomize this yet (it drives ant crazy) but this makes tests reproduceable
    // in case machines have different default charsets...
    sb.append(" -Dargs=\"-Dfile.encoding=" + System.getProperty("file.encoding") + "\"");
    return sb.toString();
  }

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
  
  // initialized by the TestRunner
  static boolean useNoMemoryExpensiveCodec;
  
  // recorded seed: for beforeClass
  private static long staticSeed;
  // seed for individual test methods, changed in @before
  private long seed;

  static final Random seedRand = new Random();
  protected static final SmartRandom random = new SmartRandom(0);

  private String name = "<unknown>";

  /**
   * Annotation for tests that should only be run during nightly builds.
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Nightly {}

  /**
   * Annotation for tests that should only be run during weekly builds
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Weekly{}

  /**
   * Annotation for tests that are slow and should be run only when specifically asked to run
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Slow{}

  /**
   * Annotation for test classes that should only use codecs that are not memory expensive (avoid SimpleText, MemoryCodec).
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface UseNoMemoryExpensiveCodec {}

  @Ignore("just a hack")
  public final void alwaysIgnoredTestMethod() {}
}
