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

import java.util.Arrays;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runners.model.Statement;

/**
 * Test reproduce message is right.
 */
public class TestReproduceMessage extends WithNestedTests {
  public static SorePoint where;
  public static SoreType  type;
  
  public static class Nested extends AbstractNestedTest {
    @BeforeClass
    public static void beforeClass() {
      if (isRunningNested()) {
        triggerOn(SorePoint.BEFORE_CLASS);
      }
    }

    @Rule
    public TestRule rule = new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            triggerOn(SorePoint.RULE);
            base.evaluate();
          }
        };
      }
    };

    /** Class initializer block/ default constructor. */
    public Nested() {
      triggerOn(SorePoint.INITIALIZER);
    }

    @Before
    public void before() {
      triggerOn(SorePoint.BEFORE);
    }    

    @Test
    public void test() {
      triggerOn(SorePoint.TEST);
    }
    
    @After
    public void after() {
      triggerOn(SorePoint.AFTER);
    }    

    @AfterClass
    public static void afterClass() {
      if (isRunningNested()) {
        triggerOn(SorePoint.AFTER_CLASS);
      }
    }    

    /** */
    private static void triggerOn(SorePoint pt) {
      if (pt == where) {
        switch (type) {
          case ASSUMPTION:
            LuceneTestCase.assumeTrue(pt.toString(), false);
            throw new RuntimeException("unreachable");
          case ERROR:
            throw new RuntimeException(pt.toString());
          case FAILURE:
            Assert.assertTrue(pt.toString(), false);
            throw new RuntimeException("unreachable");
        }
      }
    }
  }

  /*
   * ASSUMPTIONS.
   */
  
  public TestReproduceMessage() {
    super(true);
  }

  @Test
  public void testAssumeBeforeClass() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.BEFORE_CLASS;
    Assert.assertTrue(runAndReturnSyserr().isEmpty());
  }

  @Test
  public void testAssumeInitializer() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.INITIALIZER;
    Assert.assertTrue(runAndReturnSyserr().isEmpty());
  }

  @Test
  public void testAssumeRule() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.RULE;
    Assert.assertEquals("", runAndReturnSyserr());
  }

  @Test
  public void testAssumeBefore() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.BEFORE;
    Assert.assertTrue(runAndReturnSyserr().isEmpty());
  }

  @Test
  public void testAssumeTest() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.TEST;
    Assert.assertTrue(runAndReturnSyserr().isEmpty());
  }

  @Test
  public void testAssumeAfter() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.AFTER;
    Assert.assertTrue(runAndReturnSyserr().isEmpty());
  }

  @Test
  public void testAssumeAfterClass() throws Exception { 
    type = SoreType.ASSUMPTION; 
    where = SorePoint.AFTER_CLASS;
    Assert.assertTrue(runAndReturnSyserr().isEmpty());
  }

  /*
   * FAILURES
   */
  
  @Test
  public void testFailureBeforeClass() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.BEFORE_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureInitializer() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.INITIALIZER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testFailureRule() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.RULE;

    final String syserr = runAndReturnSyserr();
    
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testFailureBefore() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.BEFORE;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testFailureTest() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.TEST;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testFailureAfter() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.AFTER;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testFailureAfterClass() throws Exception { 
    type = SoreType.FAILURE; 
    where = SorePoint.AFTER_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  /*
   * ERRORS
   */
  
  @Test
  public void testErrorBeforeClass() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.BEFORE_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorInitializer() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.INITIALIZER;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  @Test
  public void testErrorRule() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.RULE;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testErrorBefore() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.BEFORE;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testErrorTest() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.TEST;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testErrorAfter() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.AFTER;
    final String syserr = runAndReturnSyserr();
    Assert.assertTrue(syserr.contains("NOTE: reproduce with:"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtests.method=test"));
    Assert.assertTrue(Arrays.asList(syserr.split("\\s")).contains("-Dtestcase=" + Nested.class.getSimpleName()));
  }

  @Test
  public void testErrorAfterClass() throws Exception { 
    type = SoreType.ERROR; 
    where = SorePoint.AFTER_CLASS;
    Assert.assertTrue(runAndReturnSyserr().contains("NOTE: reproduce with:"));
  }

  private String runAndReturnSyserr() {
    JUnitCore.runClasses(Nested.class);

    String err = getSysErr();
    // super.prevSysErr.println("Type: " + type + ", point: " + where + " resulted in:\n" + err);
    // super.prevSysErr.println("---");
    return err;
  }
}
