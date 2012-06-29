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

import org.apache.lucene.util.junitcompat.WithNestedTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.*;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesInvariantRule;

/**
 * @see TestRuleIgnoreAfterMaxFailures
 * @see SystemPropertiesInvariantRule
 */
public class TestMaxFailuresRule extends WithNestedTests {
  public TestMaxFailuresRule() {
    super(true);
  }

  public static class Nested extends WithNestedTests.AbstractNestedTest {
    @Repeat(iterations = 100)
    public void testFailSometimes() {
      assertFalse(random().nextInt(5) == 0);
    }
  }

  @Test
  public void testMaxFailures() {
    int maxFailures = LuceneTestCase.ignoreAfterMaxFailures.getMaxFailures();
    try {
      LuceneTestCase.ignoreAfterMaxFailures.setMaxFailures(2);

      JUnitCore core = new JUnitCore();
      final int [] assumptions = new int [1];
      core.addListener(new RunListener() {
        @Override
        public void testAssumptionFailure(Failure failure) {
          assumptions[0]++; 
        }
      });

      Result result = core.run(Nested.class);
      Assert.assertEquals(2, result.getFailureCount());
      Assert.assertEquals(0, result.getIgnoreCount());
      Assert.assertEquals(100, result.getRunCount());
      // JUnit doesn't pass back the number of successful tests, just make sure
      // we did have enough assumption-failures.
      Assert.assertTrue(assumptions[0] > 50);
    } finally {
      LuceneTestCase.ignoreAfterMaxFailures.setMaxFailures(maxFailures);
    }
  }
}
