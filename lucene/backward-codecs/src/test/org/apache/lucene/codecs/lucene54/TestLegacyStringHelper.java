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
package org.apache.lucene.codecs.lucene54;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestLegacyStringHelper extends LuceneTestCase {
  
  public void testBytesDifference() {
    BytesRef left = new BytesRef("foobar");
    BytesRef right = new BytesRef("foozo");
    assertEquals(3, LegacyStringHelper.bytesDifference(left, right));
  }
  
  public void testSortKeyLength() throws Exception {
    assertEquals(3, LegacyStringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("for")));
    assertEquals(3, LegacyStringHelper.sortKeyLength(new BytesRef("foo1234"), new BytesRef("for1234")));
    assertEquals(2, LegacyStringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("fz")));
    assertEquals(1, LegacyStringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("g")));
    assertEquals(4, LegacyStringHelper.sortKeyLength(new BytesRef("foo"), new BytesRef("food")));
  }
}
