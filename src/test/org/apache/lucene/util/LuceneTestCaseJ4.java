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
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestWatchman;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

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
 * <p/>
 * <p/>
 * See Junit4 documentation for a complete list of features at
 * http://junit.org/junit/javadoc/4.7/
 * <p/>
 * Import from org.junit rather than junit.framework.
 * <p/>
 * You should be able to use this class anywhere you used LuceneTestCase
 * if you annotate your derived class correctly with the annotations above
 * @see assertSaneFieldCaches
 *      <p/>
 */


// If we really need functionality in runBare override from LuceneTestCase,
// we can introduce RunBareWrapper and override runChild, and add the
// @RunWith annotation as below. runChild will be called for
// every test. But the functionality we used to
// get from that override is provided by InterceptTestCaseEvents
//@RunWith(RunBareWrapper.class)
public class LuceneTestCaseJ4 extends TestWatchman {

  /** Change this when development starts for new Lucene version: */
  public static final Version TEST_VERSION_CURRENT = Version.LUCENE_31;

  private int savedBoolMaxClauseCount;

  // This is how we get control when errors occur.
  // Think of this as start/end/success/failed
  // events.
  @Rule
  public InterceptTestCaseEvents intercept = new InterceptTestCaseEvents(this);

  public LuceneTestCaseJ4() {
  }

  public LuceneTestCaseJ4(String name) {
    this.name = name;
  }

  @Before
  public void setUp() throws Exception {
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
    BooleanQuery.setMaxClauseCount(savedBoolMaxClauseCount);
    try {
      // this isn't as useful as calling directly from the scope where the
      // index readers are used, because they could be gc'ed just before
      // tearDown is called.
      // But it's better then nothing.
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
  public static void dumpIterator(String label, Iterator iter,
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
   * @see dumpIterator(String,Iterator,PrintStream)
   */
  public static void dumpArray(String label, Object[] objs,
                               PrintStream stream) {
    Iterator iter = (null == objs) ? null : Arrays.asList(objs).iterator();
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
    return newRandom(seedRnd.nextLong());
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
    this.seed = Long.valueOf(seed);
    return new Random(seed);
  }

  public String getName() {
    return this.name;
  }

  // We get here fro InterceptTestCaseEvents on the 'failed' event.... 
  public void reportAdditionalFailureInfo() {
    if (seed != null) {
      System.out.println("NOTE: random seed of testcase '" + getName() + "' was: " + seed);
    }
  }

  // recorded seed
  protected Long seed = null;

  // static members
  private static final Random seedRnd = new Random();

  private String name = "";

}
