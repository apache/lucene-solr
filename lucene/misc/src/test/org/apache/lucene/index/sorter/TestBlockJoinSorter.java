package org.apache.lucene.index.sorter;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

@SuppressCodecs("Lucene3x")
public class TestBlockJoinSorter extends LuceneTestCase {

  private static class FixedBitSetCachingWrapperFilter extends CachingWrapperFilter {

    public FixedBitSetCachingWrapperFilter(Filter filter) {
      super(filter);
    }

    @Override
    protected DocIdSet cacheImpl(DocIdSetIterator iterator, AtomicReader reader)
        throws IOException {
      final FixedBitSet cached = new FixedBitSet(reader.maxDoc());
      cached.or(iterator);
      return cached;
    }

  }

  public void test() throws IOException {
    final int numParents = atLeast(200);
    IndexWriterConfig cfg = newIndexWriterConfig(new MockAnalyzer(random()));
    cfg.setMergePolicy(newLogMergePolicy());
    final RandomIndexWriter writer = new RandomIndexWriter(random(), newDirectory(), cfg);
    final Document parentDoc = new Document();
    final NumericDocValuesField parentVal = new NumericDocValuesField("parent_val", 0L);
    parentDoc.add(parentVal);
    final StringField parent = new StringField("parent", "true", Store.YES);
    parentDoc.add(parent);
    for (int i = 0; i < numParents; ++i) {
      List<Document> documents = new ArrayList<>();
      final int numChildren = random().nextInt(10);
      for (int j = 0; j < numChildren; ++j) {
        final Document childDoc = new Document();
        childDoc.add(new NumericDocValuesField("child_val", random().nextInt(5)));
        documents.add(childDoc);
      }
      parentVal.setLongValue(random().nextInt(50));
      documents.add(parentDoc);
      writer.addDocuments(documents);
    }
    writer.forceMerge(1);
    final DirectoryReader indexReader = writer.getReader();
    writer.close();

    final AtomicReader reader = getOnlySegmentReader(indexReader);
    final Filter parentsFilter = new FixedBitSetCachingWrapperFilter(new QueryWrapperFilter(new TermQuery(new Term("parent", "true"))));
    final FixedBitSet parentBits = (FixedBitSet) parentsFilter.getDocIdSet(reader.getContext(), null);
    final NumericDocValues parentValues = reader.getNumericDocValues("parent_val");

    final NumericDocValues childValues = reader.getNumericDocValues("child_val");

    final Sort parentSort = new Sort(new SortField("parent_val", SortField.Type.LONG));
    final Sort childSort = new Sort(new SortField("child_val", SortField.Type.LONG));

    final Sort sort = new Sort(new SortField("custom", new BlockJoinComparatorSource(parentsFilter, parentSort, childSort)));
    final Sorter sorter = new Sorter(sort);
    final Sorter.DocMap docMap = sorter.sort(reader);
    assertEquals(reader.maxDoc(), docMap.size());

    int[] children = new int[1];
    int numChildren = 0;
    int previousParent = -1;
    for (int i = 0; i < docMap.size(); ++i) {
      final int oldID = docMap.newToOld(i);
      if (parentBits.get(oldID)) {
        // check that we have the right children
        for (int j = 0; j < numChildren; ++j) {
          assertEquals(oldID, parentBits.nextSetBit(children[j]));
        }
        // check that children are sorted
        for (int j = 1; j < numChildren; ++j) {
          final int doc1 = children[j-1];
          final int doc2 = children[j];
          if (childValues.get(doc1) == childValues.get(doc2)) {
            assertTrue(doc1 < doc2); // sort is stable
          } else {
            assertTrue(childValues.get(doc1) < childValues.get(doc2));
          }
        }
        // check that parents are sorted
        if (previousParent != -1) {
          if (parentValues.get(previousParent) == parentValues.get(oldID)) {
            assertTrue(previousParent < oldID);
          } else {
            assertTrue(parentValues.get(previousParent) < parentValues.get(oldID));
          }
        }
        // reset
        previousParent = oldID;
        numChildren = 0;
      } else {
        children = ArrayUtil.grow(children, numChildren+1);
        children[numChildren++] = oldID;
      }
    }
    indexReader.close();
    writer.w.getDirectory().close();
  }

}
