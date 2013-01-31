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

import org.apache.lucene.store.Directory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestFailIfDirectoryNotClosed extends WithNestedTests {
  public TestFailIfDirectoryNotClosed() {
    super(true);
  }
  
  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    public void testDummy() throws Exception {
      Directory dir = newDirectory();
      System.out.println(dir.toString());
    }
  }

  @Test
  public void testFailIfDirectoryNotClosed() {
    Result r = JUnitCore.runClasses(Nested1.class);
    for (Failure f : r.getFailures()) {
      System.out.println("Failure: " + f);
    }
    Assert.assertEquals(1, r.getFailureCount());
    Assert.assertTrue(r.getFailures().get(0).toString().contains("Resource in scope SUITE failed to close"));
  }
}
