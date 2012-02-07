/**
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

public class TestCloseableThreadLocal extends LuceneTestCase {
  public static final String TEST_VALUE = "initvaluetest";
  
  public void testInitValue() {
    InitValueThreadLocal tl = new InitValueThreadLocal();
    String str = (String)tl.get();
    assertEquals(TEST_VALUE, str);
  }

  public void testNullValue() throws Exception {
    // Tests that null can be set as a valid value (LUCENE-1805). This
    // previously failed in get().
    CloseableThreadLocal<Object> ctl = new CloseableThreadLocal<Object>();
    ctl.set(null);
    assertNull(ctl.get());
  }

  public void testDefaultValueWithoutSetting() throws Exception {
    // LUCENE-1805: make sure default get returns null,
    // twice in a row
    CloseableThreadLocal<Object> ctl = new CloseableThreadLocal<Object>();
    assertNull(ctl.get());
  }

  public class InitValueThreadLocal extends CloseableThreadLocal<Object> {
    @Override
    protected Object initialValue() {
      return TEST_VALUE;
    } 
  }
}
