package org.apache.lucene.util;

import org.junit.Assert;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Repeat;

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

/**
 * This rule keeps a count of failed tests (suites) and will result in an
 * {@link AssumptionViolatedException} after a given number of failures for all
 * tests following this condition.
 * 
 * <p>
 * Aborting quickly on failed tests can be useful when used in combination with
 * test repeats (via the {@link Repeat} annotation or system property).
 */
public final class TestRuleIgnoreAfterMaxFailures implements TestRule {
  /**
   * Maximum failures. Package scope for tests.
   */
  int maxFailures;

  /**
   * Current count of failures. Package scope for tests.
   */
  int failuresSoFar;
  
  /**
   * @param maxFailures
   *          The number of failures after which all tests are ignored. Must be
   *          greater or equal 1.
   */
  public TestRuleIgnoreAfterMaxFailures(int maxFailures) {
    Assert.assertTrue("maxFailures must be >= 1: " + maxFailures, maxFailures >= 1);
    this.maxFailures = maxFailures;
  }

  @Override
  public Statement apply(final Statement s, final Description d) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (failuresSoFar >= maxFailures) {
          RandomizedTest.assumeTrue("Ignored, failures limit reached (" + 
              failuresSoFar + " >= " + maxFailures + ").", false);
        }

        try {
          s.evaluate();
        } catch (Throwable t) {
          if (!TestRuleMarkFailure.isAssumption(t)) {
            failuresSoFar++;
          }
          throw t;
        }
      }
    };
  }
}
