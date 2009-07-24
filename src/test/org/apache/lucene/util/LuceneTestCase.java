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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import junit.framework.TestCase;
import java.util.Random;

/** Base class for all Lucene unit tests.  Currently the
 *  only added functionality over JUnit's TestCase is
 *  asserting that no unhandled exceptions occurred in
 *  threads launched by ConcurrentMergeScheduler.  If you
 *  override either <code>setUp()</code> or
 *  <code>tearDown()</code> in your unit test, make sure you
 *  call <code>super.setUp()</code> and
 *  <code>super.tearDown()</code>.
 */

public abstract class LuceneTestCase extends TestCase {

  public LuceneTestCase() {
    super();
  }

  public LuceneTestCase(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    ConcurrentMergeScheduler.setTestMode();
  }

  protected void tearDown() throws Exception {
    if (ConcurrentMergeScheduler.anyUnhandledExceptions()) {
      // Clear the failure so that we don't just keep
      // failing subsequent test cases
      ConcurrentMergeScheduler.clearUnhandledExceptions();
      fail("ConcurrentMergeScheduler hit unhandled exceptions");
    }
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
    this.seed = new Long(seed);
    return new Random(seed);
  }
  
  protected void runTest() throws Throwable {
    try {
      seed = null;
      super.runTest();
    } catch (Throwable e) {
      if (seed != null) {
        System.out.println("NOTE: random seed of testcase '" + getName() + "' was: " + seed);
      }
      throw e;
    }
  }
  
  // recorded seed
  protected Long seed = null;
  
  // static members
  private static final Random seedRnd = new Random();
}
