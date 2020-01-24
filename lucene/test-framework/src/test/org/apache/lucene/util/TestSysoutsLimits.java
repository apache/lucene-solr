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

import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

/**
 * @see TestRuleLimitSysouts
 */
public class TestSysoutsLimits extends WithNestedTests {
  public TestSysoutsLimits() {
    super(false);
  }

  public static class ParentNestedTest extends LuceneTestCase
    implements TestRuleIgnoreTestSuites.NestedTestSuite {
    @BeforeClass
    public static void onlyWhenNested() {
      assumeTrue("Only runs when nested", TestRuleIgnoreTestSuites.isRunningNested());
    }
  }

  @TestRuleLimitSysouts.Limit(bytes = 10)
  public static class OverSoftLimit extends ParentNestedTest {
    public void testWrite() {
      System.out.print(RandomizedTest.randomAsciiLettersOfLength(10));
    }
  }

  @Test
  public void testOverSoftLimit() {
    JUnitCore core = new JUnitCore();
    Result result = core.run(OverSoftLimit.class);

    String msg = result.getFailures().stream()
        .map(failure -> failure.getMessage())
        .collect(Collectors.joining("\n"));

    Assert.assertTrue(msg, msg.contains("The test or suite printed 10 bytes"));
  }

  @TestRuleLimitSysouts.Limit(bytes = 10)
  public static class UnderLimit extends ParentNestedTest {
    public void testWrite() {
      System.out.print(RandomizedTest.randomAsciiLettersOfLength(9));
    }
  }

  @Test
  public void testUnderLimit() {
    JUnitCore core = new JUnitCore();
    Result result = core.run(UnderLimit.class);

    String msg = result.getFailures().stream()
        .map(failure -> failure.getMessage())
        .collect(Collectors.joining("\n"));

    Assert.assertTrue(msg, msg.isEmpty());
  }

  @TestRuleLimitSysouts.Limit(bytes = 10, hardLimit = 20)
  public static class OverHardLimit extends ParentNestedTest {
    public void testWrite() {
      System.out.print("1234567890");
      System.out.print("-marker1-");
      System.out.print("-marker2-"); System.out.flush();
      System.out.print("-marker3-"); System.out.flush();
      System.out.print("-marker4-"); System.out.flush();
    }
  }

  @Test
  public void OverHardLimit() {
    JUnitCore core = new JUnitCore();
    Result result = core.run(OverHardLimit.class);

    String msg = result.getFailures().stream()
        .map(failure -> failure.getMessage())
        .collect(Collectors.joining("\n"));

    Assert.assertTrue(msg, msg.contains("Hard limit was enforced"));
    Assert.assertTrue(msg, msg.contains("The test or suite printed 46 bytes"));
  }
}
