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

public class TestVersion extends LuceneTestCase {

  public void test() {
    for (Version v : Version.values()) {
      assertTrue("LUCENE_CURRENT must be always onOrAfter("+v+")", Version.LUCENE_CURRENT.onOrAfter(v));
    }
    assertTrue(Version.LUCENE_50.onOrAfter(Version.LUCENE_40));
    assertFalse(Version.LUCENE_40.onOrAfter(Version.LUCENE_50));
  }

  public void testParseLeniently() {
    assertEquals(Version.LUCENE_40, Version.parseLeniently("4.0"));
    assertEquals(Version.LUCENE_40, Version.parseLeniently("LUCENE_40"));
    assertEquals(Version.LUCENE_CURRENT, Version.parseLeniently("LUCENE_CURRENT"));
  }
  
  public void testDeprecations() throws Exception {
    Version values[] = Version.values();
    // all but the latest version should be deprecated
    for (int i = 0; i < values.length; i++) {
      if (i + 1 == values.length) {
        assertSame("Last constant must be LUCENE_CURRENT", Version.LUCENE_CURRENT, values[i]);
      }
      // TODO: Use isAnnotationPresent once bug in Java 8 is fixed (LUCENE-4808)
      final Deprecated ann = Version.class.getField(values[i].name()).getAnnotation(Deprecated.class);
      if (i + 2 != values.length) {
        assertNotNull(values[i].name() + " should be deprecated", ann);
      } else {
        assertNull(values[i].name() + " should not be deprecated", ann);
      }
    }
  }
}
