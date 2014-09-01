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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestSortingMergePolicy extends LuceneTestCase {

  private List<String> terms;
  private Directory dir1, dir2;
  private Sort sort;
  private IndexReader reader;
  private IndexReader sortedReader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    sort = new Sort(new SortField("ndv", SortField.Type.LONG));
    createRandomIndexes();
  }

  private Document randomDocument() {
    final Document doc = new Document();
    doc.add(new NumericDocValuesField("ndv", random().nextLong()));
    doc.add(new StringField("s", RandomPicks.randomFrom(random(), terms), Store.YES));
    return doc;
  }

  static MergePolicy newSortingMergePolicy(Sort sort) {
    // usually create a MP with a low merge factor so that many merges happen
    MergePolicy mp;
    int thingToDo = random().nextInt(3);
    if (thingToDo == 0) {
      TieredMergePolicy tmp = newTieredMergePolicy(random());
      final int numSegs = TestUtil.nextInt(random(), 3, 5);
      tmp.setSegmentsPerTier(numSegs);
      tmp.setMaxMergeAtOnce(TestUtil.nextInt(random(), 2, numSegs));
      mp = tmp;
    } else if (thingToDo == 1) {
      LogMergePolicy lmp = newLogMergePolicy(random());
      lmp.setMergeFactor(TestUtil.nextInt(random(), 3, 5));
      mp = lmp;
    } else {
      // just a regular random one from LTC (could be alcoholic etc)
      mp = newMergePolicy();
    }
    // wrap it with a sorting mp
    return new SortingMergePolicy(mp, sort);
  }

  private void createRandomIndexes() throws IOException {
    dir1 = newDirectory();
    dir2 = newDirectory();
    final int numDocs = atLeast(150);
    final int numTerms = TestUtil.nextInt(random(), 1, numDocs / 5);
    Set<String> randomTerms = new HashSet<>();
    while (randomTerms.size() < numTerms) {
      randomTerms.add(TestUtil.randomSimpleString(random()));
    }
    terms = new ArrayList<>(randomTerms);
    final long seed = random().nextLong();
    final IndexWriterConfig iwc1 = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    final IndexWriterConfig iwc2 = newIndexWriterConfig(new MockAnalyzer(new Random(seed)));
    iwc2.setMergePolicy(newSortingMergePolicy(sort));
    final RandomIndexWriter iw1 = new RandomIndexWriter(new Random(seed), dir1, iwc1);
    final RandomIndexWriter iw2 = new RandomIndexWriter(new Random(seed), dir2, iwc2);
    for (int i = 0; i < numDocs; ++i) {
      if (random().nextInt(5) == 0 && i != numDocs - 1) {
        final String term = RandomPicks.randomFrom(random(), terms);
        iw1.deleteDocuments(new Term("s", term));
        iw2.deleteDocuments(new Term("s", term));
      }
      final Document doc = randomDocument();
      iw1.addDocument(doc);
      iw2.addDocument(doc);
      if (random().nextInt(8) == 0) {
        iw1.commit();
        iw2.commit();
      }
    }
    // Make sure we have something to merge
    iw1.commit();
    iw2.commit();
    final Document doc = randomDocument();
    // NOTE: don't use RIW.addDocument directly, since it sometimes commits
    // which may trigger a merge, at which case forceMerge may not do anything.
    // With field updates this is a problem, since the updates can go into the
    // single segment in the index, and threefore the index won't be sorted.
    // This hurts the assumption of the test later on, that the index is sorted
    // by SortingMP.
    iw1.w.addDocument(doc);
    iw2.w.addDocument(doc);

    // update NDV of docs belonging to one term (covers many documents)
    final long value = random().nextLong();
    final String term = RandomPicks.randomFrom(random(), terms);
    iw1.w.updateNumericDocValue(new Term("s", term), "ndv", value);
    iw2.w.updateNumericDocValue(new Term("s", term), "ndv", value);
    
    iw1.forceMerge(1);
    iw2.forceMerge(1);
    iw1.close();
    iw2.close();
    reader = DirectoryReader.open(dir1);
    sortedReader = DirectoryReader.open(dir2);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    sortedReader.close();
    dir1.close();
    dir2.close();
    super.tearDown();
  }

  private static void assertSorted(AtomicReader reader) throws IOException {
    final NumericDocValues ndv = reader.getNumericDocValues("ndv");
    for (int i = 1; i < reader.maxDoc(); ++i) {
      assertTrue("ndv(" + (i-1) + ")=" + ndv.get(i-1) + ",ndv(" + i + ")=" + ndv.get(i), ndv.get(i-1) <= ndv.get(i));
    }
  }

  public void testSortingMP() throws IOException {
    final AtomicReader sortedReader1 = SortingAtomicReader.wrap(SlowCompositeReaderWrapper.wrap(reader), sort);
    final AtomicReader sortedReader2 = SlowCompositeReaderWrapper.wrap(sortedReader);

    assertSorted(sortedReader1);
    assertSorted(sortedReader2);
    
    assertReaderEquals("", sortedReader1, sortedReader2);
  }
  
  public void testBadSort() throws Exception {
    try {
      new SortingMergePolicy(newMergePolicy(), Sort.RELEVANCE);
      fail("Didn't get expected exception");
    } catch (IllegalArgumentException e) {
      assertEquals("Cannot sort an index with a Sort that refers to the relevance score", e.getMessage());
    }
  }

}
