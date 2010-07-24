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

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;

import java.io.File;
import java.io.PrintStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Collections;
import java.lang.reflect.Method;

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
public class LuceneTestCaseJ4 {

  /**
   * true iff tests are run in verbose mode. Note: if it is false, tests are not
   * expected to print any messages.
   */
  public static final boolean VERBOSE = Boolean.getBoolean("tests.verbose");

  /** Use this constant when creating Analyzers and any other version-dependent stuff.
   * <p><b>NOTE:</b> Change this when development starts for new Lucene version:
   */
  public static final Version TEST_VERSION_CURRENT = Version.LUCENE_40;

  /** Create indexes in this directory, optimally use a subdir, named after the test */
  public static final File TEMP_DIR;
  static {
    String s = System.getProperty("tempDir", System.getProperty("java.io.tmpdir"));
    if (s == null)
      throw new RuntimeException("To run tests, you need to define system property 'tempDir' or 'java.io.tmpdir'.");
    TEMP_DIR = new File(s);
  }

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
    
    if (seed != null) {
      System.out.println("NOTE: random seed of testcase '" + getName() + "' was: " + seed);
    }
  }

  // recorded seed
  protected Long seed = null;

  // static members
  private static final Random seedRnd = new Random();

  private String name = "<unknown>";

}
