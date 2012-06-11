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

import java.util.Properties;

import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesInvariantRule;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

/**
 * @see SystemPropertiesRestoreRule
 * @see SystemPropertiesInvariantRule
 */
public class TestSystemPropertiesInvariantRule extends WithNestedTests {
  public static final String PROP_KEY1 = "new-property-1";
  public static final String VALUE1 = "new-value-1";
  
  public TestSystemPropertiesInvariantRule() {
    super(true);
  }
  
  public static class Base extends WithNestedTests.AbstractNestedTest {
    public void testEmpty() {}
  }
  
  public static class InBeforeClass extends Base {
    @BeforeClass
    public static void beforeClass() {
      System.setProperty(PROP_KEY1, VALUE1);
    }
  }
  
  public static class InAfterClass extends Base {
    @AfterClass
    public static void afterClass() {
      System.setProperty(PROP_KEY1, VALUE1);
    }
  }
  
  public static class InTestMethod extends Base {
    public void testMethod1() {
      if (System.getProperty(PROP_KEY1) != null) {
        throw new RuntimeException("Shouldn't be here.");
      }
      System.setProperty(PROP_KEY1, VALUE1);
    }
    
    public void testMethod2() {
      testMethod1();
    }
  }

  public static class NonStringProperties extends Base {
    public void testMethod1() {
      if (System.getProperties().get(PROP_KEY1) != null) {
        throw new RuntimeException("Will pass.");
      }

      Properties properties = System.getProperties();
      properties.put(PROP_KEY1, new Object());
      Assert.assertTrue(System.getProperties().get(PROP_KEY1) != null);
    }

    public void testMethod2() {
      testMethod1();
    }

    @AfterClass
    public static void cleanup() {
      System.getProperties().remove(PROP_KEY1);
    }
  }

  public static class IgnoredProperty {
    @Rule
    public TestRule invariant = new SystemPropertiesInvariantRule(PROP_KEY1);

    @Test
    public void testMethod1() {
      System.setProperty(PROP_KEY1, VALUE1);
    }
  }

  @Before
  @After
  public void cleanup() {
    System.clearProperty(PROP_KEY1);
  }
  
  @Test
  public void testRuleInvariantBeforeClass() {
    Result runClasses = JUnitCore.runClasses(InBeforeClass.class);
    Assert.assertEquals(1, runClasses.getFailureCount());
    Assert.assertTrue(runClasses.getFailures().get(0).getMessage()
        .contains(PROP_KEY1));
    Assert.assertNull(System.getProperty(PROP_KEY1));
  }
  
  @Test
  public void testRuleInvariantAfterClass() {
    Result runClasses = JUnitCore.runClasses(InAfterClass.class);
    Assert.assertEquals(1, runClasses.getFailureCount());
    Assert.assertTrue(runClasses.getFailures().get(0).getMessage()
        .contains(PROP_KEY1));
    Assert.assertNull(System.getProperty(PROP_KEY1));
  }
  
  @Test
  public void testRuleInvariantInTestMethod() {
    Result runClasses = JUnitCore.runClasses(InTestMethod.class);
    Assert.assertEquals(2, runClasses.getFailureCount());
    for (Failure f : runClasses.getFailures()) {
      Assert.assertTrue(f.getMessage().contains(PROP_KEY1));
    }
    Assert.assertNull(System.getProperty(PROP_KEY1));
  }
  
  @Test
  public void testNonStringProperties() {
    Result runClasses = JUnitCore.runClasses(NonStringProperties.class);
    Assert.assertEquals(1, runClasses.getFailureCount());
    Assert.assertTrue(runClasses.getFailures().get(0).getMessage().contains("Will pass"));
    Assert.assertEquals(3, runClasses.getRunCount());
  }
  
  @Test
  public void testIgnoredProperty() {
    System.clearProperty(PROP_KEY1);
    try {
      Result runClasses = JUnitCore.runClasses(IgnoredProperty.class);
      Assert.assertEquals(0, runClasses.getFailureCount());
      Assert.assertEquals(VALUE1, System.getProperty(PROP_KEY1));
    } finally {
      System.clearProperty(PROP_KEY1);
    }
  }
}
