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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Ensures proper functions of {@link LuceneTestCase#setUp()}
 * and {@link LuceneTestCase#tearDown()}.
 */
public class TestSetupTeardownChaining extends WithNestedTests {
  public static class NestedSetupChain extends AbstractNestedTest {
    @Override
    public void setUp() throws Exception {
      // missing call.
      System.out.println("Hello.");
    }

    @Test
    public void testMe() {
    }
  }

  public static class NestedTeardownChain extends AbstractNestedTest {
    @Override
    public void tearDown() throws Exception {
      // missing call.
    }

    @Test
    public void testMe() {
    }
  }

  public TestSetupTeardownChaining() {
    super(true);
  }
  
  /**
   * Verify super method calls on {@link LuceneTestCase#setUp()}.
   */
  @Test
  public void testSetupChaining() {
    Result result = JUnitCore.runClasses(NestedSetupChain.class);
    Assert.assertEquals(1, result.getFailureCount());
    Failure failure = result.getFailures().get(0);
    Assert.assertTrue(failure.getMessage()
        .contains("One of the overrides of setUp does not propagate the call."));
  }
  
  /**
   * Verify super method calls on {@link LuceneTestCase#tearDown()}.
   */
  @Test
  public void testTeardownChaining() {
    Result result = JUnitCore.runClasses(NestedTeardownChain.class);
    Assert.assertEquals(1, result.getFailureCount());
    Failure failure = result.getFailures().get(0);
    Assert.assertTrue(failure.getMessage()
        .contains("One of the overrides of tearDown does not propagate the call."));
  }
}
