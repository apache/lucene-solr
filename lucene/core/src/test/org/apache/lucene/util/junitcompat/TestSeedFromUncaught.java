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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Check that uncaught exceptions result in seed info being dumped to
 * console. 
 */
public class TestSeedFromUncaught extends WithNestedTests {
  public static class ThrowInUncaught extends AbstractNestedTest {
    @Test
    public void testFoo() throws Exception {
      Thread t = new Thread() {
        @Override
        public void run() {
          throw new RuntimeException("foobar");
        }
      };
      t.start();
      t.join();
    }
  }

  public TestSeedFromUncaught() {
    super(/* suppress normal output. */ true);
  }

  /**
   * Verify super method calls on {@link LuceneTestCase#setUp()}.
   */
  @Test
  public void testUncaughtDumpsSeed() {
    Result result = JUnitCore.runClasses(ThrowInUncaught.class);
    Assert.assertEquals(1, result.getFailureCount());
    Failure f = result.getFailures().get(0);
    String trace = f.getTrace();
    Assert.assertTrue(trace.contains("SeedInfo.seed("));
    Assert.assertTrue(trace.contains("foobar"));
  }
}
