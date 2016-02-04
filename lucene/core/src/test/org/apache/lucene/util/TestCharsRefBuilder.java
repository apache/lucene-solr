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


public class TestCharsRefBuilder extends LuceneTestCase {

  public void testAppend() {
    final String s = TestUtil.randomUnicodeString(random(), 100);
    CharsRefBuilder builder = new CharsRefBuilder();
    while (builder.length() < s.length()) {
      if (random().nextBoolean()) {
        builder.append(s.charAt(builder.length()));
      } else {
        final int start = builder.length();
        final int end = TestUtil.nextInt(random(), start, s.length());
        if (random().nextBoolean()) {
          builder.append(s.substring(start, end));
        } else {
          builder.append(s, start, end);
        }
      }
    }
    assertEquals(s, builder.toString());
  }

  public void testAppendNull() {
    CharsRefBuilder builder = new CharsRefBuilder();
    builder.append(null);
    builder.append((CharSequence) null, 1, 3);
    assertEquals("nullnull", builder.toString());
  }

}
