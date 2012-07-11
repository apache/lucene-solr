package org.apache.lucene.util.junitcompat;

import java.util.*;

import org.apache.lucene.util._TestUtil;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

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

public class TestSameRandomnessLocalePassedOrNot extends WithNestedTests {
  @ClassRule
  public static TestRule solrClassRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  public TestSameRandomnessLocalePassedOrNot() {
    super(true);
  }
  
  public static class Nested extends WithNestedTests.AbstractNestedTest {
    public static String pickString;
    public static Locale defaultLocale;
    public static TimeZone defaultTimeZone;
    public static String seed;

    @BeforeClass
    public static void setup() {
      seed = RandomizedContext.current().getRunnerSeedAsString();

      Random rnd = random();
      pickString = _TestUtil.randomSimpleString(rnd);
      
      defaultLocale = Locale.getDefault();
      defaultTimeZone = TimeZone.getDefault();
    }

    public void testPassed() {
      System.out.println("Picked locale: " + defaultLocale);
      System.out.println("Picked timezone: " + defaultTimeZone.getID());
    }
  }
  
  @Test
  public void testSetupWithoutLocale() {
    Result runClasses = JUnitCore.runClasses(Nested.class);
    Assert.assertEquals(0, runClasses.getFailureCount());

    String s1 = Nested.pickString;
    System.setProperty("tests.seed", Nested.seed);
    System.setProperty("tests.timezone", Nested.defaultTimeZone.getID());
    System.setProperty("tests.locale", Nested.defaultLocale.toString());
    JUnitCore.runClasses(Nested.class);
    String s2 = Nested.pickString;

    Assert.assertEquals(s1, s2);
  }
}
