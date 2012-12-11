package org.apache.lucene.util.junitcompat;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

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

public class TestBeforeAfterOverrides extends WithNestedTests {
  public TestBeforeAfterOverrides() {
    super(true);
  }

  public static class Before1 extends WithNestedTests.AbstractNestedTest {
    @Before
    public void before() {}
    
    public void testEmpty() {}
  }
  public static class Before2 extends Before1 {}
  public static class Before3 extends Before2 {
    @Override
    @Before
    public void before() {}
  }

  public static class After1 extends WithNestedTests.AbstractNestedTest {
    @After
    public void after() {}
    
    public void testEmpty() {}
  }
  public static class After2 extends Before1 {}
  public static class After3 extends Before2 {
    @After
    public void after() {}
  }

  @Test
  public void testBefore() {
    Result result = JUnitCore.runClasses(Before3.class);
    Assert.assertEquals(1, result.getFailureCount());
    Assert.assertTrue(result.getFailures().get(0).getTrace().contains("There are overridden methods"));
  }
  
  @Test
  public void testAfter() {
    Result result = JUnitCore.runClasses(Before3.class);
    Assert.assertEquals(1, result.getFailureCount());
    Assert.assertTrue(result.getFailures().get(0).getTrace().contains("There are overridden methods"));
  }  
}
