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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.index.FrozenBufferedUpdates.TermDocsIterator;
import org.apache.lucene.util.TestUtil;

public class TestFrozenBufferedUpdates extends LuceneTestCase {

  public void testTermDocsIterator() throws IOException {
    for (int j = 0; j < 5; j++) {
      try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
        boolean duplicates = random().nextBoolean();
        boolean nonMatches = random().nextBoolean();
        BytesRefArray array = new BytesRefArray(Counter.newCounter());
        int numDocs = 10 + random().nextInt(1000);
        Set<BytesRef> randomIds = new HashSet<>();
        for (int i = 0; i < numDocs; i++) {
          BytesRef id;
          do {
             id = new BytesRef(TestUtil.randomRealisticUnicodeString(random()));
          } while (randomIds.add(id) == false);
        }
        List<BytesRef> asList = new ArrayList<>(randomIds);
        for (BytesRef ref : randomIds) {
          Document doc = new Document();
          doc.add(new StringField("field", ref, Field.Store.NO));
          array.append(ref);
          if (duplicates && rarely()) {
            array.append(RandomPicks.randomFrom(random(), asList));
          }
          if (nonMatches && rarely()) {
            BytesRef id;
            do {
              id = new BytesRef(TestUtil.randomRealisticUnicodeString(random()));
            } while (randomIds.contains(id));
            array.append(id);
          }
          writer.addDocument(doc);
        }
        writer.forceMerge(1);
        writer.commit();
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          boolean sorted = random().nextBoolean();
          BytesRefIterator values = sorted ? array.iterator(Comparator.naturalOrder()) : array.iterator();
          assertEquals(1, reader.leaves().size());
          TermDocsIterator iterator = new TermDocsIterator(reader.leaves().get(0).reader(), sorted);
          FixedBitSet bitSet = new FixedBitSet(reader.maxDoc());
          BytesRef ref;
          while ((ref = values.next()) != null) {
            DocIdSetIterator docIdSetIterator = iterator.nextTerm("field", ref);
            if (nonMatches == false) {
              assertNotNull(docIdSetIterator);
            }
            if (docIdSetIterator != null) {
              int doc;
              while ((doc = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (duplicates == false) {
                  assertFalse(bitSet.get(doc));
                }
                bitSet.set(doc);
              }
            }
          }
          assertEquals(reader.maxDoc(), bitSet.cardinality());
        }
      }
    }
  }
}
