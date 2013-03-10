package org.apache.lucene.util.junitcompat;

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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleIgnoreTestSuites;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;

/**
 * An abstract test class that prepares nested test classes to run.
 * A nested test class will assume it's executed under control of this
 * class and be ignored otherwise. 
 * 
 * <p>The purpose of this is so that nested test suites don't run from
 * IDEs like Eclipse (where they are automatically detected).
 * 
 * <p>This class cannot extend {@link LuceneTestCase} because in case
 * there's a nested {@link LuceneTestCase} afterclass hooks run twice and
 * cause havoc (static fields).
 */
public abstract class WithNestedTests {
  public static abstract class AbstractNestedTest extends LuceneTestCase 
    implements TestRuleIgnoreTestSuites.NestedTestSuite {
    protected static boolean isRunningNested() {
      return TestRuleIgnoreTestSuites.isRunningNested();
    }
  }

  private boolean suppressOutputStreams;

  protected WithNestedTests(boolean suppressOutputStreams) {
    this.suppressOutputStreams = suppressOutputStreams;
  }
  
  protected PrintStream prevSysErr;
  protected PrintStream prevSysOut;
  private ByteArrayOutputStream sysout;
  private ByteArrayOutputStream syserr;

  /**
   * Restore properties after test.
   */
  @Rule
  public final TestRule rules;
  {
    final TestRuleMarkFailure marker = new TestRuleMarkFailure();
    rules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule())
      .around(new TestRuleAdapter() {
        @Override
        protected void afterAlways(List<Throwable> errors) throws Throwable {
          if (marker.hadFailures() && suppressOutputStreams) {
            System.out.println("sysout from nested test: " + getSysOut() + "\n");
            System.out.println("syserr from nested test: " + getSysErr());
          }
        }
      })
      .around(marker);
  }
      
  @Before
  public final void before() {
    if (suppressOutputStreams) {
      prevSysOut = System.out;
      prevSysErr = System.err;

      try {
        sysout = new ByteArrayOutputStream();
        System.setOut(new PrintStream(sysout, true, "UTF-8"));
        syserr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(syserr, true, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    System.setProperty(TestRuleIgnoreTestSuites.PROPERTY_RUN_NESTED, "true");
  }

  @After
  public final void after() {
    if (suppressOutputStreams) {
      System.out.flush();
      System.err.flush();

      System.setOut(prevSysOut);
      System.setErr(prevSysErr);
    }
  }

  protected String getSysOut() {
    Assert.assertTrue(suppressOutputStreams);
    System.out.flush();
    try {
      return new String(sysout.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  protected String getSysErr() {
    Assert.assertTrue(suppressOutputStreams);
    System.err.flush();
    try {
      return new String(syserr.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }  
}
