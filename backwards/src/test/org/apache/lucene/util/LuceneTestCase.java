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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import junit.framework.TestCase;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.CacheEntry;
import org.apache.lucene.util.FieldCacheSanityChecker.Insanity;

/** 
 * Base class for all Lucene unit tests.  
 * <p>
 * Currently the
 * only added functionality over JUnit's TestCase is
 * asserting that no unhandled exceptions occurred in
 * threads launched by ConcurrentMergeScheduler and asserting sane
 * FieldCache usage athe moment of tearDown.
 * </p>
 * <p>
 * If you
 * override either <code>setUp()</code> or
 * <code>tearDown()</code> in your unit test, make sure you
 * call <code>super.setUp()</code> and
 * <code>super.tearDown()</code>
 * </p>
 * @see #assertSaneFieldCaches
 */
public abstract class LuceneTestCase extends TestCase {


  private int savedBoolMaxClauseCount;
  
  private volatile Thread.UncaughtExceptionHandler savedUncaughtExceptionHandler = null;
  
  private static class UncaughtExceptionEntry {
    public final Thread thread;
    public final Throwable exception;
    
    public UncaughtExceptionEntry(Thread thread, Throwable exception) {
      this.thread = thread;
      this.exception = exception;
    }
  }
  private List<UncaughtExceptionEntry> uncaughtExceptions = Collections.synchronizedList(new ArrayList<UncaughtExceptionEntry>());

  public LuceneTestCase() {
    super();
  }

  public LuceneTestCase(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    
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
  }

  /**
   * Forcible purges all cache entries from the FieldCache.
   * <p>
   * This method will be called by tearDown to clean up FieldCache.DEFAULT.
   * If a (poorly written) test has some expectation that the FieldCache
   * will persist across test methods (ie: a static IndexReader) this 
   * method can be overridden to do nothing.
   * </p>
   * @see FieldCache#purgeAllCaches()
   */
  protected void purgeFieldCache(final FieldCache fc) {
    fc.purgeAllCaches();
  }

  protected String getTestLabel() {
    return getClass().getName() + "." + getName();
  }

  @Override
  protected void tearDown() throws Exception {
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
    
    Thread.setDefaultUncaughtExceptionHandler(savedUncaughtExceptionHandler);
    if (!uncaughtExceptions.isEmpty()) {
      System.err.println("The following exceptions were thrown by threads:");
      for (UncaughtExceptionEntry entry : uncaughtExceptions) {
        System.err.println("*** Thread: " + entry.thread.getName() + " ***");
        entry.exception.printStackTrace(System.err);
      }
      fail("Some threads throwed uncaught exceptions!");
    }
    
    super.tearDown();
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
   * @see FieldCacheSanityChecker
   */
  protected void assertSaneFieldCaches(final String msg) {
    final CacheEntry[] entries = FieldCache.DEFAULT.getCacheEntries();
    Insanity[] insanity = null;
    try {
      try {
        insanity = FieldCacheSanityChecker.checkSanity(entries);
      } catch (RuntimeException e) {
        dumpArray(msg+ ": FieldCache", entries, System.err);
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
   * @param label String logged before/after the items in the iterator
   * @param iter Each next() is toString()ed and logged on it's own line. If iter is null this is logged differnetly then an empty iterator.
   * @param stream Stream to log messages to.
   */
  public static void dumpIterator(String label, Iterator iter, 
                                  PrintStream stream) {
    stream.println("*** BEGIN "+label+" ***");
    if (null == iter) {
      stream.println(" ... NULL ...");
    } else {
      while (iter.hasNext()) {
        stream.println(iter.next().toString());
      }
    }
    stream.println("*** END "+label+" ***");
  }

  /** 
   * Convinience method for logging an array.  Wraps the array in an iterator and delegates
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
   *.
   */
  public Random newRandom() {
    if (seed != null) {
      throw new IllegalStateException("please call LuceneTestCase.newRandom only once per test");
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
      throw new IllegalStateException("please call LuceneTestCase.newRandom only once per test");
    }
    this.seed = Long.valueOf(seed);
    return new Random(seed);
  }
  
  @Override
  public void runBare() throws Throwable {
    //long t0 = System.currentTimeMillis();
    try {
      seed = null;
      super.runBare();
    } catch (Throwable e) {
      if (seed != null) {
        System.out.println("NOTE: random seed of testcase '" + getName() + "' was: " + seed);
      }
      throw e;
    }
    //long t = System.currentTimeMillis() - t0;
    //System.out.println(t + " msec for " + getName());
  }
  
  // recorded seed
  protected Long seed = null;
  
  // static members
  private static final Random seedRnd = new Random();
}
