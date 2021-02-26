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

import org.apache.lucene.codecs.Codec;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

public class TestCodecReported extends WithNestedTests {
  public TestCodecReported() {
    super(true);
  }

  public static class Nested1 extends WithNestedTests.AbstractNestedTest {
    public static String codecName;

    public void testDummy() {
      codecName = Codec.getDefault().getName();
      fail();
    }
  }

  @Test
  public void testCorrectCodecReported() {
    Result r = JUnitCore.runClasses(Nested1.class);
    Assert.assertEquals(1, r.getFailureCount());
    Assert.assertTrue(super.getSysErr(), super.getSysErr().contains("codec=" + Nested1.codecName));
  }
}
