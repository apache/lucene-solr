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
package org.apache.lucene.util;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;

/**
 * @see TestRuleIgnoreAfterMaxFailures
 */
public class TestMaxFailuresRule extends WithNestedTests {
  public TestMaxFailuresRule() {
    super(true);
  }

  public static class Nested extends WithNestedTests.AbstractNestedTest {
    public static final int TOTAL_ITERS = 500;
    public static final int DESIRED_FAILURES = TOTAL_ITERS / 10;
    private int numFails = 0;
    private int numIters = 0;

    @Repeat(iterations = TOTAL_ITERS)
    public void testFailSometimes() {
      numIters++;
      boolean fail = random().nextInt(5) == 0;
      if (fail) numFails++;
      // some seeds are really lucky ... so cheat.
      if (numFails < DESIRED_FAILURES && 
          DESIRED_FAILURES <= TOTAL_ITERS - numIters) {
        fail = true;
      }
      assertFalse(fail);
    }
  }

  @Test
  public void testMaxFailures() {
    LuceneTestCase.replaceMaxFailureRule(new TestRuleIgnoreAfterMaxFailures(2));
    JUnitCore core = new JUnitCore();
    final StringBuilder results = new StringBuilder();
    core.addListener(new RunListener() {
      char lastTest;

      @Override
      public void testStarted(Description description) throws Exception {
        lastTest = 'S'; // success.
      }

      @Override
      public void testAssumptionFailure(Failure failure) {
        lastTest = 'A'; // assumption failure.
      }

      @Override
      public void testFailure(Failure failure) throws Exception {
        lastTest = 'F'; // failure
      }

      @Override
      public void testFinished(Description description) throws Exception {
        results.append(lastTest);
      }
    });

    Result result = core.run(Nested.class);
    Assert.assertEquals(500, result.getRunCount());
    Assert.assertEquals(0, result.getIgnoreCount());
    Assert.assertEquals(2, result.getFailureCount());

    // Make sure we had exactly two failures followed by assumption-failures
    // resulting from ignored tests.
    Assert.assertTrue(results.toString(), 
        results.toString().matches("(S*F){2}A+"));
  }

  @ThreadLeakZombies(Consequence.IGNORE_REMAINING_TESTS)
  @ThreadLeakAction({ThreadLeakAction.Action.WARN})
  @ThreadLeakScope(Scope.TEST)
  @ThreadLeakLingering(linger = 500)
  public static class Nested2 extends WithNestedTests.AbstractNestedTest {
    public static final int TOTAL_ITERS = 10;
    public static CountDownLatch die;
    public static Thread zombie;
    public static int testNum;
    
    @BeforeClass
    public static void setup() {
      assert zombie == null;
      die = new CountDownLatch(1);
      testNum = 0;
    }

    @Repeat(iterations = TOTAL_ITERS)
    public void testLeaveZombie() {
      if (++testNum == 2) {
        zombie = new Thread() {
          @Override
          public void run() {
            while (true) {
              try {
                die.await();
                return;
              } catch (Exception e) { /* ignore */ }
            }
          }
        };
        zombie.start();
      }
    }
  }

  @Test
  public void testZombieThreadFailures() throws Exception {
    LuceneTestCase.replaceMaxFailureRule(new TestRuleIgnoreAfterMaxFailures(1));
    JUnitCore core = new JUnitCore();
    final StringBuilder results = new StringBuilder();
    core.addListener(new RunListener() {
      char lastTest;

      @Override
      public void testStarted(Description description) throws Exception {
        lastTest = 'S'; // success.
      }

      @Override
      public void testAssumptionFailure(Failure failure) {
        lastTest = 'A'; // assumption failure.
      }

      @Override
      public void testFailure(Failure failure) throws Exception {
        lastTest = 'F'; // failure
        System.out.println(failure.getMessage());
      }

      @Override
      public void testFinished(Description description) throws Exception {
        results.append(lastTest);
      }
    });

    Result result = core.run(Nested2.class);
    if (Nested2.die != null) {
      Nested2.die.countDown();
      Nested2.zombie.join();
    }
    
    super.prevSysOut.println(results.toString());
    Assert.assertEquals(Nested2.TOTAL_ITERS, result.getRunCount());
    Assert.assertEquals(results.toString(), "SFAAAAAAAA", results.toString());
  }  
}
