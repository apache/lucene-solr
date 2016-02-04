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


public class TestStringHelper extends LuceneTestCase {
  
  public void testBytesDifference() {
    BytesRef left = new BytesRef("foobar");
    BytesRef right = new BytesRef("foozo");
    assertEquals(3, StringHelper.bytesDifference(left, right));
  }
  
  public void testStartsWith() {
    BytesRef ref = new BytesRef("foobar");
    BytesRef slice = new BytesRef("foo");
    assertTrue(StringHelper.startsWith(ref, slice));
  }
  
  public void testEndsWith() {
    BytesRef ref = new BytesRef("foobar");
    BytesRef slice = new BytesRef("bar");
    assertTrue(StringHelper.endsWith(ref, slice));
  }

  public void testStartsWithWhole() {
    BytesRef ref = new BytesRef("foobar");
    BytesRef slice = new BytesRef("foobar");
    assertTrue(StringHelper.startsWith(ref, slice));
  }
  
  public void testEndsWithWhole() {
    BytesRef ref = new BytesRef("foobar");
    BytesRef slice = new BytesRef("foobar");
    assertTrue(StringHelper.endsWith(ref, slice));
  }

  public void testMurmurHash3() throws Exception {
    // Hashes computed using murmur3_32 from https://code.google.com/p/pyfasthash
    assertEquals(0xf6a5c420, StringHelper.murmurhash3_x86_32(new BytesRef("foo"), 0));
    assertEquals(0xcd018ef6, StringHelper.murmurhash3_x86_32(new BytesRef("foo"), 16));
    assertEquals(0x111e7435, StringHelper.murmurhash3_x86_32(new BytesRef("You want weapons? We're in a library! Books! The best weapons in the world!"), 0));
    assertEquals(0x2c628cd0, StringHelper.murmurhash3_x86_32(new BytesRef("You want weapons? We're in a library! Books! The best weapons in the world!"), 3476));
  }
  
  public void testSortKeyLength() throws Exception {
    assertEquals(3, StringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("for")));
    assertEquals(3, StringHelper.sortKeyLength(new BytesRef("foo1234"), new BytesRef("for1234")));
    assertEquals(2, StringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("fz")));
    assertEquals(1, StringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("g")));
    assertEquals(4, StringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("food")));
  }
}
