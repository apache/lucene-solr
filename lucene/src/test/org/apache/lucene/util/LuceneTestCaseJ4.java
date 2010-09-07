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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.mockintblock.MockFixedIntBlockCodec;
import org.apache.lucene.index.codecs.mockintblock.MockVariableIntBlockCodec;
import org.apache.lucene.index.codecs.mocksep.MockSepCodec;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.preflexrw.PreFlexRWCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatchman;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.WeakHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Base class for all Lucene unit tests, Junit4 variant.
 * Replaces LuceneTestCase.
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
 * @After - replaces setup
 * @Before - replaces teardown
 * @Test - any public method with this annotation is a test case, regardless
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


// If we really need functionality in runBare override from LuceneTestCase,
// we can introduce RunBareWrapper and override runChild, and add the
// @RunWith annotation as below. runChild will be called for
// every test. But the functionality we used to
// get from that override is provided by InterceptTestCaseEvents
//@RunWith(RunBareWrapper.class)
@RunWith(LuceneTestCaseJ4.LuceneTestCaseRunner.class)
public abstract class LuceneTestCaseJ4 {

  /**
   * true iff tests are run in verbose mode. Note: if it is false, tests are not
   * expected to print any messages.
   */
  public static final boolean VERBOSE = Boolean.getBoolean("tests.verbose");

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

  // by default we randomly pick a different codec for
  // each test case (non-J4 tests) and each test class (J4
  // tests)
  /** Gets the codec to run tests with. */
  static final String TEST_CODEC = System.getProperty("tests.codec", "random");
  /** Gets the locale to run tests with */
  static final String TEST_LOCALE = System.getProperty("tests.locale", "random");
  /** Gets the timezone to run tests with */
  static final String TEST_TIMEZONE = System.getProperty("tests.timezone", "random");
  /** Gets the directory to run tests with */
  static final String TEST_DIRECTORY = System.getProperty("tests.directory", "random");
  /** Get the number of times to run tests */
  static final int TEST_ITER = Integer.parseInt(System.getProperty("tests.iter", "1"));
  
  private static final Pattern codecWithParam = Pattern.compile("(.*)\\(\\s*(\\d+)\\s*\\)");

  /**
   * A random multiplier which you should use when writing random tests:
   * multiply it by the number of iterations
   */
  public static final int RANDOM_MULTIPLIER = Integer.parseInt(System.getProperty("tests.multiplier", "1"));
  
  private int savedBoolMaxClauseCount;

  private volatile Thread.UncaughtExceptionHandler savedUncaughtExceptionHandler = null;
  
  /** Used to track if setUp and tearDown are called correctly from subclasses */
  private boolean setup;

  private static class UncaughtExceptionEntry {
    public final Thread thread;
    public final Throwable exception;
    
    public UncaughtExceptionEntry(Thread thread, Throwable exception) {
      this.thread = thread;
      this.exception = exception;
    }
  }
  private List<UncaughtExceptionEntry> uncaughtExceptions = Collections.synchronizedList(new ArrayList<UncaughtExceptionEntry>());
  
  // checks if class correctly annotated
  private static final Object PLACEHOLDER = new Object();
  private static final Map<Class<? extends LuceneTestCaseJ4>,Object> checkedClasses =
    Collections.synchronizedMap(new WeakHashMap<Class<? extends LuceneTestCaseJ4>,Object>());
  
  // saves default codec: we do this statically as many build indexes in @beforeClass
  private static String savedDefaultCodec;
  private static Codec codec;
  
  private static Locale locale;
  private static Locale savedLocale;
  private static TimeZone timeZone;
  private static TimeZone savedTimeZone;
  
  private static Map<MockDirectoryWrapper,StackTraceElement[]> stores;
  
  private static final String[] TEST_CODECS = new String[] {"MockSep", "MockFixedIntBlock", "MockVariableIntBlock"};

  private static void swapCodec(Codec c) {
    final CodecProvider cp = CodecProvider.getDefault();
    Codec prior = null;
    try {
      prior = cp.lookup(c.name);
    } catch (IllegalArgumentException iae) {
    }
    if (prior != null) {
      cp.unregister(prior);
    }
    cp.register(c);
  }

  // returns current default codec
  static Codec installTestCodecs() {
    final CodecProvider cp = CodecProvider.getDefault();

    savedDefaultCodec = CodecProvider.getDefaultCodec();
    String codec = TEST_CODEC;

    final boolean codecHasParam;
    int codecParam = 0;
    if (codec.equals("random")) {
      codec = pickRandomCodec(seedRnd);
      codecHasParam = false;
    } else {
      Matcher m = codecWithParam.matcher(codec);
      if (m.matches()) {
        // codec has a fixed param
        codecHasParam = true;
        codec = m.group(1);
        codecParam = Integer.parseInt(m.group(2));
      } else {
        codecHasParam = false;
      }
    }

    CodecProvider.setDefaultCodec(codec);

    if (codec.equals("PreFlex")) {
      // If we're running w/ PreFlex codec we must swap in the
      // test-only PreFlexRW codec (since core PreFlex can
      // only read segments):
      swapCodec(new PreFlexRWCodec());
    }

    swapCodec(new MockSepCodec());
    swapCodec(new PulsingCodec(codecHasParam && "Pulsing".equals(codec) ? codecParam : _TestUtil.nextInt(seedRnd, 1, 20)));
    swapCodec(new MockFixedIntBlockCodec(codecHasParam && "MockFixedIntBlock".equals(codec) ? codecParam : _TestUtil.nextInt(seedRnd, 1, 2000)));
    // baseBlockSize cannot be over 127:
    swapCodec(new MockVariableIntBlockCodec(codecHasParam && "MockVariableIntBlock".equals(codec) ? codecParam : _TestUtil.nextInt(seedRnd, 1, 127)));

    return cp.lookup(codec);
  }

  // returns current PreFlex codec
  static void removeTestCodecs(Codec codec) {
    final CodecProvider cp = CodecProvider.getDefault();
    if (codec.name.equals("PreFlex")) {
      final Codec preFlex = cp.lookup("PreFlex");
      if (preFlex != null) {
        cp.unregister(preFlex);
      }
      cp.register(new PreFlexCodec());
    }
    cp.unregister(cp.lookup("MockSep"));
    cp.unregister(cp.lookup("MockFixedIntBlock"));
    cp.unregister(cp.lookup("MockVariableIntBlock"));
    swapCodec(new PulsingCodec(1));
    CodecProvider.setDefaultCodec(savedDefaultCodec);
  }

  // randomly picks from core and test codecs
  static String pickRandomCodec(Random rnd) {
    int idx = rnd.nextInt(CodecProvider.CORE_CODECS.length + 
                          TEST_CODECS.length);
    if (idx < CodecProvider.CORE_CODECS.length) {
      return CodecProvider.CORE_CODECS[idx];
    } else {
      return TEST_CODECS[idx - CodecProvider.CORE_CODECS.length];
    }
  }

  @BeforeClass
  public static void beforeClassLuceneTestCaseJ4() {
    stores = Collections.synchronizedMap(new IdentityHashMap<MockDirectoryWrapper,StackTraceElement[]>());
    codec = installTestCodecs();
    savedLocale = Locale.getDefault();
    locale = TEST_LOCALE.equals("random") ? randomLocale(seedRnd) : localeForName(TEST_LOCALE);
    Locale.setDefault(locale);
    savedTimeZone = TimeZone.getDefault();
    timeZone = TEST_TIMEZONE.equals("random") ? randomTimeZone(seedRnd) : TimeZone.getTimeZone(TEST_TIMEZONE);
    TimeZone.setDefault(timeZone);
  }
  
  @AfterClass
  public static void afterClassLuceneTestCaseJ4() {
    removeTestCodecs(codec);
    Locale.setDefault(savedLocale);
    TimeZone.setDefault(savedTimeZone);
    System.clearProperty("solr.solr.home");
    System.clearProperty("solr.data.dir");
    // now look for unclosed resources
    for (MockDirectoryWrapper d : stores.keySet()) {
      if (d.isOpen()) {
        StackTraceElement elements[] = stores.get(d);
        StackTraceElement element = (elements.length > 1) ? elements[1] : null;
        fail("directory of test was not closed, opened from: " + element);
      }
    }
    stores = null;
  }

  // This is how we get control when errors occur.
  // Think of this as start/end/success/failed
  // events.
  @Rule
  public final TestWatchman intercept = new TestWatchman() {

    @Override
    public void failed(Throwable e, FrameworkMethod method) {
      reportAdditionalFailureInfo();
      super.failed(e, method);
    }

    @Override
    public void starting(FrameworkMethod method) {
      // set current method name for logging
      LuceneTestCaseJ4.this.name = method.getName();
      // check if the current test's class annotated all test* methods with @Test
      final Class<? extends LuceneTestCaseJ4> clazz = LuceneTestCaseJ4.this.getClass();
      if (!checkedClasses.containsKey(clazz)) {
        checkedClasses.put(clazz, PLACEHOLDER);
        for (Method m : clazz.getMethods()) {
          if (m.getName().startsWith("test") && m.getAnnotation(Test.class) == null) {
            fail("In class '" + clazz.getName() + "' the method '" + m.getName() + "' is not annotated with @Test.");
          }
        }
      }
      super.starting(method);
    }
    
  };

  @Before
  public void setUp() throws Exception {
    Assert.assertFalse("ensure your tearDown() calls super.tearDown()!!!", setup);
    setup = true;
    savedUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        uncaughtExceptions.add(new UncaughtExceptionEntry(t, e));
        if (savedUncaughtExceptionHandler != null)
          savedUncaughtExceptionHandler.uncaughtException(t, e);
      }
    });
    
    ConcurrentMergeScheduler.setTestMode();
    savedBoolMaxClauseCount = BooleanQuery.getMaxClauseCount();
    seed = null;
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

  @After
  public void tearDown() throws Exception {
    Assert.assertTrue("ensure your setUp() calls super.setUp()!!!", setup);
    setup = false;
    BooleanQuery.setMaxClauseCount(savedBoolMaxClauseCount);
    try {

      if (!uncaughtExceptions.isEmpty()) {
        System.err.println("The following exceptions were thrown by threads:");
        for (UncaughtExceptionEntry entry : uncaughtExceptions) {
          System.err.println("*** Thread: " + entry.thread.getName() + " ***");
          entry.exception.printStackTrace(System.err);
        }
        fail("Some threads threw uncaught exceptions!");
      }

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

      if (ConcurrentMergeScheduler.anyUnhandledExceptions()) {
        // Clear the failure so that we don't just keep
        // failing subsequent test cases
        ConcurrentMergeScheduler.clearUnhandledExceptions();
        fail("ConcurrentMergeScheduler hit unhandled exceptions");
      }
    } finally {
      purgeFieldCache(FieldCache.DEFAULT);
    }
    
    Thread.setDefaultUncaughtExceptionHandler(savedUncaughtExceptionHandler);
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
   * @see FieldCacheSanityChecker
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
   * Convinience method for logging an iterator.
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
   * Convinience method for logging an array.  Wraps the array in an iterator and delegates
   *
   * @see #dumpIterator(String,Iterator,PrintStream)
   */
  public static void dumpArray(String label, Object[] objs,
                               PrintStream stream) {
    Iterator<?> iter = (null == objs) ? null : Arrays.asList(objs).iterator();
    dumpIterator(label, iter, stream);
  }

  /**
   * Returns a {@link Random} instance for generating random numbers during the test.
   * The random seed is logged during test execution and printed to System.out on any failure
   * for reproducing the test using {@link #newRandom(long)} with the recorded seed
   * .
   */
  public Random newRandom() {
    if (seed != null) {
      throw new IllegalStateException("please call LuceneTestCaseJ4.newRandom only once per test");
    }
    this.seed = Long.valueOf(seedRnd.nextLong());
    if (VERBOSE) {
      System.out.println("NOTE: random seed of testcase '" + getName() + "' is: " + this.seed);
    }
    return new Random(seed);
  }

  /**
   * Returns a {@link Random} instance for generating random numbers during the test.
   * If an error occurs in the test that is not reproducible, you can use this method to
   * initialize the number generator with the seed that was printed out during the failing test.
   */
  public Random newRandom(long seed) {
    if (this.seed != null) {
      throw new IllegalStateException("please call LuceneTestCaseJ4.newRandom only once per test");
    }
    System.out.println("WARNING: random seed of testcase '" + getName() + "' is fixed to: " + seed);
    this.seed = Long.valueOf(seed);
    return new Random(seed);
  }

  private static final Map<Class<? extends LuceneTestCaseJ4>,Long> staticSeeds =
    Collections.synchronizedMap(new WeakHashMap<Class<? extends LuceneTestCaseJ4>,Long>());

  /**
   * Returns a {@link Random} instance for generating random numbers from a beforeclass
   * annotated method.
   * The random seed is logged during test execution and printed to System.out on any failure
   * for reproducing the test using {@link #newStaticRandom(Class, long)} with the recorded seed
   * .
   */
  public static Random newStaticRandom(Class<? extends LuceneTestCaseJ4> clazz) {
    Long seed = seedRnd.nextLong();
    staticSeeds.put(clazz, seed);
    return new Random(seed);
  }
  
  /**
   * Returns a {@link Random} instance for generating random numbers from a beforeclass
   * annotated method.
   * If an error occurs in the test that is not reproducible, you can use this method to
   * initialize the number generator with the seed that was printed out during the failing test.
   */
  public static Random newStaticRandom(Class<? extends LuceneTestCaseJ4> clazz, long seed) {
    staticSeeds.put(clazz, Long.valueOf(seed));
    System.out.println("WARNING: random static seed of testclass '" + clazz + "' is fixed to: " + seed);
    return new Random(seed);
  }

  /** create a new index writer config with random defaults */
  public static IndexWriterConfig newIndexWriterConfig(Random r, Version v, Analyzer a) {
    IndexWriterConfig c = new IndexWriterConfig(v, a);
    if (r.nextBoolean()) {
      c.setMergePolicy(new LogDocMergePolicy());
    }
    if (r.nextBoolean()) {
      c.setMergeScheduler(new SerialMergeScheduler());
    }
    if (r.nextBoolean()) {
      c.setMaxBufferedDocs(_TestUtil.nextInt(r, 2, 1000));
    }
    if (r.nextBoolean()) {
      c.setTermIndexInterval(_TestUtil.nextInt(r, 1, 1000));
    }
    if (r.nextBoolean()) {
      c.setMaxThreadStates(_TestUtil.nextInt(r, 1, 20));
    }
    
    if (c.getMergePolicy() instanceof LogMergePolicy) {
      LogMergePolicy logmp = (LogMergePolicy) c.getMergePolicy();
      logmp.setUseCompoundDocStore(r.nextBoolean());
      logmp.setUseCompoundFile(r.nextBoolean());
      logmp.setCalibrateSizeByDeletes(r.nextBoolean());
      logmp.setMergeFactor(_TestUtil.nextInt(r, 2, 20));
    }
    
    c.setReaderPooling(r.nextBoolean());
    c.setReaderTermsIndexDivisor(_TestUtil.nextInt(r, 1, 4));
    return c;
  }

  /**
   * Returns a new Dictionary instance. Use this when the test does not
   * care about the specific Directory implementation (most tests).
   * <p>
   * The Directory is wrapped with {@link MockDirectoryWrapper}.
   * By default this means it will be picky, such as ensuring that you
   * properly close it and all open files in your test. It will emulate
   * some features of Windows, such as not allowing open files to be
   * overwritten.
   */
  public static MockDirectoryWrapper newDirectory(Random r) throws IOException {
    StackTraceElement[] stack = new Exception().getStackTrace();
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    MockDirectoryWrapper dir = new MockDirectoryWrapper(impl);
    stores.put(dir, stack);
    return dir;
  }
  
  /**
   * Returns a new Dictionary instance, with contents copied from the
   * provided directory. See {@link #newDirectory(Random)} for more
   * information.
   */
  public static MockDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
    StackTraceElement[] stack = new Exception().getStackTrace();
    Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
    for (String file : d.listAll()) {
     d.copy(impl, file, file);
    }
    MockDirectoryWrapper dir = new MockDirectoryWrapper(impl);
    stores.put(dir, stack);
    return dir;
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

  private static String CORE_DIRECTORIES[] = {
    "RAMDirectory",
    "SimpleFSDirectory",
    "NIOFSDirectory",
    "MMapDirectory"
  };
  
  public static String randomDirectory(Random random) {
    if (random.nextInt(10) == 0) {
      return CORE_DIRECTORIES[random.nextInt(CORE_DIRECTORIES.length)];
    } else {
      return "RAMDirectory";
    }
  }
  
  static Directory newDirectoryImpl(Random random, String clazzName) {
    if (clazzName.equals("random"))
      clazzName = randomDirectory(random);
    if (clazzName.indexOf(".") == -1) // if not fully qualified, assume .store
      clazzName = "org.apache.lucene.store." + clazzName;
    try {
      final Class<? extends Directory> clazz = Class.forName(clazzName).asSubclass(Directory.class);
      try {
        // try empty ctor
        return clazz.newInstance();
      } catch (Exception e) {
        final File tmpFile = File.createTempFile("test", "tmp", TEMP_DIR);
        tmpFile.delete();
        tmpFile.mkdir();
        try {
          Constructor<? extends Directory> ctor = clazz.getConstructor(File.class);
          Directory d = ctor.newInstance(tmpFile);
          // try not to enable this hack unless we must.
          if (d instanceof MMapDirectory && Constants.WINDOWS && MMapDirectory.UNMAP_SUPPORTED)
            ((MMapDirectory)d).setUseUnmap(true);
          return d;
        } catch (Exception e2) {
          // try .open(File)
          Method method = clazz.getMethod("open", new Class[] { File.class });
          return (Directory) method.invoke(null, tmpFile);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
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
  public void reportAdditionalFailureInfo() {
    Long staticSeed = staticSeeds.get(getClass());
    if (staticSeed != null) {
      System.out.println("NOTE: random static seed of testclass '" + getName() + "' was: " + staticSeed);
    }
    
    System.out.println("NOTE: random codec of testcase '" + getName() + "' was: " + codec);
    if (TEST_LOCALE.equals("random"))
      System.out.println("NOTE: random locale of testcase '" + getName() + "' was: " + locale);
    if (TEST_TIMEZONE.equals("random")) // careful to not deliver NPE here in case they forgot super.setUp
      System.out.println("NOTE: random timezone of testcase '" + getName() + "' was: " + (timeZone == null ? "(null)" : timeZone.getID()));
    if (seed != null) {
      System.out.println("NOTE: random seed of testcase '" + getName() + "' was: " + seed);
    }
  }

  // recorded seed
  protected Long seed = null;

  // static members
  private static final Random seedRnd = new Random();

  private String name = "<unknown>";
  
  /** optionally filters the tests to be run by TEST_METHOD */
  public static class LuceneTestCaseRunner extends BlockJUnit4ClassRunner {

    @Override
    protected void runChild(FrameworkMethod arg0, RunNotifier arg1) {
      for (int i = 0; i < TEST_ITER; i++)
        super.runChild(arg0, arg1);
    }

    public LuceneTestCaseRunner(Class<?> clazz) throws InitializationError {
      super(clazz);
      Filter f = new Filter() {

        @Override
        public String describe() { return "filters according to TEST_METHOD"; }

        @Override
        public boolean shouldRun(Description d) {
          return TEST_METHOD == null || d.getMethodName().equals(TEST_METHOD);
        }     
      };
      
      try {
        f.apply(this);
      } catch (NoTestsRemainException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
