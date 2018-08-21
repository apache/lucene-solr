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

package org.apache.lucene.search;

import org.apache.lucene.util.LuceneTestCase;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class TestDocIdSetIterator extends LuceneTestCase {
  public void testRangeBasic() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 8);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(6, disi.nextDoc());
    assertEquals(7, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }

  public void testInvalidRange() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {DocIdSetIterator.range(5, 4);});
  }

  public void testInvalidMin() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {DocIdSetIterator.range(-1, 4);});
  }

  public void testEmpty() throws Exception {
    expectThrows(IllegalArgumentException.class, () -> {DocIdSetIterator.range(7, 7);});
  }

  public void testAdvance() throws Exception {
    DocIdSetIterator disi = DocIdSetIterator.range(5, 20);
    assertEquals(-1, disi.docID());
    assertEquals(5, disi.nextDoc());
    assertEquals(17, disi.advance(17));
    assertEquals(18, disi.nextDoc());
    assertEquals(19, disi.nextDoc());
    assertEquals(NO_MORE_DOCS, disi.nextDoc());
  }
}
