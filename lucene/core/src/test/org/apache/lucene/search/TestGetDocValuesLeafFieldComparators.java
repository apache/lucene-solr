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

import java.io.EOFException;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestGetDocValuesLeafFieldComparators extends LuceneTestCase {
  private IndexSearcher is;
  private IndexReader ir;
  private Directory dir;
  private final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG));
  private Set set = new HashSet<>();

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    int numDocs = TestUtil.nextInt(random(), 5, 15);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      long val = TestUtil.nextLong(random(), 15, 200);
      doc.add(new NumericDocValuesField("ndv1", val));
      set.add(val);
      iw.addDocument(doc);
    }
    ir = iw.getReader();
    iw.close();
    is = newSearcher(ir);
  }

  @Override
  public void tearDown() throws Exception {
    ir.close();
    dir.close();
    super.tearDown();
  }

  public void testValues() throws Exception {
    Sort[] sortArray = new Sort[] { sort, new Sort() };
    for(int i = 0; i < sortArray.length; i++) {
      int count = 0;
      Query q = new MatchAllDocsQuery();
      TopFieldCollector tdc = TopFieldCollector.create(sortArray[i], 10, Integer.MAX_VALUE);
      is.search(q, tdc);

      assert tdc.pq instanceof FieldValueHitQueue;
      ScoreDoc[] scoreDocs = tdc.topDocs().scoreDocs;

      for (ScoreDoc scoreDoc : scoreDocs) {

        FieldValueHitQueue queue = (FieldValueHitQueue) tdc.pq;
        FieldComparator[] comparators = queue.getComparators();

        for (FieldComparator comparator : comparators) {
          for (LeafReaderContext context : ir.leaves()) {
            try {
              Object value = comparator.getLeafComparator(context).getDocValue(scoreDoc.doc);

              if (value instanceof Long) {
                long castedValue = (long) value;
                assertNotNull(set.contains(castedValue));
                ++count;
              } else if (value instanceof Float) {
                float castedValue = (float) value;
                assertNotNull(set.contains(castedValue));
                ++count;
              }
            } catch (EOFException e) {
              // Next segment
            }
          }
        }
      }

      assertEquals(scoreDocs.length, count);
    }
  }
}
