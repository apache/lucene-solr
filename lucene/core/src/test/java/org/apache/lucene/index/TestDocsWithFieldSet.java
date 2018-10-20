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
package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestDocsWithFieldSet extends LuceneTestCase {

  public void testDense() throws IOException {
    DocsWithFieldSet set = new DocsWithFieldSet();
    DocIdSetIterator it = set.iterator();
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    set.add(0);
    it = set.iterator();
    assertEquals(0, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());

    long ramBytesUsed = set.ramBytesUsed();
    for (int i = 1; i < 1000; ++i) {
      set.add(i);
    }
    assertEquals(ramBytesUsed, set.ramBytesUsed());
    it = set.iterator();
    for (int i = 0; i < 1000; ++i) {
      assertEquals(i, it.nextDoc());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  public void testSparse() throws IOException {
    DocsWithFieldSet set = new DocsWithFieldSet();
    int doc = random().nextInt(10000);
    set.add(doc);
    DocIdSetIterator it = set.iterator();
    assertEquals(doc, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
    int doc2 = doc + TestUtil.nextInt(random(), 1, 100);
    set.add(doc2);
    it = set.iterator();
    assertEquals(doc, it.nextDoc());
    assertEquals(doc2, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

  public void testDenseThenSparse() throws IOException {
    int denseCount = random().nextInt(10000);
    int nextDoc = denseCount + random().nextInt(10000);
    DocsWithFieldSet set = new DocsWithFieldSet();
    for (int i = 0; i < denseCount; ++i) {
      set.add(i);
    }
    set.add(nextDoc);
    DocIdSetIterator it = set.iterator();
    for (int i = 0; i < denseCount; ++i) {
      assertEquals(i, it.nextDoc());
    }
    assertEquals(nextDoc, it.nextDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
  }

}
