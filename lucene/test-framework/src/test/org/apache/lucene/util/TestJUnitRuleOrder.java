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
import java.util.Stack;

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
 * This verifies that JUnit {@link Rule}s are invoked before 
 * {@link Before} and {@link  After} hooks. This should be the
 * case from JUnit 4.10 on.
 */
public class TestJUnitRuleOrder extends WithNestedTests {
  static Stack<String> stack;

  public TestJUnitRuleOrder() {
    super(true);
  }
  
  public static class Nested extends WithNestedTests.AbstractNestedTest {
    @Before
    public void before() {
      stack.push("@Before");
    }
    
    @After
    public void after() {
      stack.push("@After");
    }

    @Rule
    public TestRule testRule = new TestRule() {
      @Override
      public Statement apply(final Statement base, Description description) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            stack.push("@Rule before");
            base.evaluate();
            stack.push("@Rule after");
          }
        };
      }
    };

    @Test
    public void test() {/* empty */}

    @BeforeClass
    public static void beforeClassCleanup() {
      stack = new Stack<>();
    }

    @AfterClass
    public static void afterClassCheck() {
      stack.push("@AfterClass");
    }    
  }

  @Test
  public void testRuleOrder() {
    JUnitCore.runClasses(Nested.class);
    Assert.assertEquals(
        Arrays.toString(stack.toArray()), "[@Rule before, @Before, @After, @Rule after, @AfterClass]");
  }
}
