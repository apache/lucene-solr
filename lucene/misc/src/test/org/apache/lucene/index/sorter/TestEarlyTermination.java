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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util._TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

@SuppressCodecs("Lucene3x")
public class TestEarlyTermination extends LuceneTestCase {

  private int numDocs;
  private List<String> terms;
  private Directory dir;
  private Sorter sorter;
  private RandomIndexWriter iw;
  private IndexReader reader;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    sorter = new NumericDocValuesSorter("ndv1");
  }

  private Document randomDocument() {
    final Document doc = new Document();
    doc.add(new NumericDocValuesField("ndv1", random().nextInt(10)));
    doc.add(new NumericDocValuesField("ndv2", random().nextInt(10)));
    doc.add(new StringField("s", RandomPicks.randomFrom(random(), terms), Store.YES));
    return doc;
  }

  private void createRandomIndexes(int maxSegments) throws IOException {
    dir = newDirectory();
    numDocs = atLeast(150);
    final int numTerms = _TestUtil.nextInt(random(), 1, numDocs / 5);
    Set<String> randomTerms = new HashSet<String>();
    while (randomTerms.size() < numTerms) {
      randomTerms.add(_TestUtil.randomSimpleString(random()));
    }
    terms = new ArrayList<String>(randomTerms);
    final long seed = random().nextLong();
    final IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(new Random(seed)));
    iwc.setMergePolicy(TestSortingMergePolicy.newSortingMergePolicy(sorter));
    iw = new RandomIndexWriter(new Random(seed), dir, iwc);
    for (int i = 0; i < numDocs; ++i) {
      final Document doc = randomDocument();
      iw.addDocument(doc);
      if (i == numDocs / 2 || (i != numDocs - 1 && random().nextInt(8) == 0)) {
        iw.commit();
      }
      if (random().nextInt(15) == 0) {
        final String term = RandomPicks.randomFrom(random(), terms);
        iw.deleteDocuments(new Term("s", term));
      }
    }
    reader = iw.getReader();
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    iw.close();
    dir.close();
    super.tearDown();
  }

  public void testEarlyTermination() throws IOException {
    createRandomIndexes(5);
    final int numHits = _TestUtil.nextInt(random(), 1, numDocs / 10);
    final Sort sort = new Sort(new SortField("ndv1", SortField.Type.LONG, false));
    final boolean fillFields = random().nextBoolean();
    final boolean trackDocScores = random().nextBoolean();
    final boolean trackMaxScore = random().nextBoolean();
    final boolean inOrder = random().nextBoolean();
    final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, fillFields, trackDocScores, trackMaxScore, inOrder);
    final TopFieldCollector collector2 = TopFieldCollector.create(sort, numHits, fillFields, trackDocScores, trackMaxScore, inOrder);

    final IndexSearcher searcher = newSearcher(reader);
    final int iters = atLeast(5);
    for (int i = 0; i < iters; ++i) {
      final TermQuery query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
      searcher.search(query, collector1);
      searcher.search(query, new EarlyTerminatingSortingCollector(collector2, sorter, numHits));
    }
    assertTrue(collector1.getTotalHits() >= collector2.getTotalHits());
    assertTopDocsEquals(collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
  }
  
  public void testEarlyTerminationDifferentSorter() throws IOException {
    // test that the collector works correctly when the index was sorted by a
    // different sorter than the one specified in the ctor.
    createRandomIndexes(5);
    final int numHits = _TestUtil.nextInt(random(), 1, numDocs / 10);
    final Sort sort = new Sort(new SortField("ndv2", SortField.Type.LONG, false));
    final boolean fillFields = random().nextBoolean();
    final boolean trackDocScores = random().nextBoolean();
    final boolean trackMaxScore = random().nextBoolean();
    final boolean inOrder = random().nextBoolean();
    final TopFieldCollector collector1 = TopFieldCollector.create(sort, numHits, fillFields, trackDocScores, trackMaxScore, inOrder);
    final TopFieldCollector collector2 = TopFieldCollector.create(sort, numHits, fillFields, trackDocScores, trackMaxScore, inOrder);
    
    final IndexSearcher searcher = newSearcher(reader);
    final int iters = atLeast(5);
    for (int i = 0; i < iters; ++i) {
      final TermQuery query = new TermQuery(new Term("s", RandomPicks.randomFrom(random(), terms)));
      searcher.search(query, collector1);
      searcher.search(query, new EarlyTerminatingSortingCollector(collector2, new NumericDocValuesSorter("ndv2"), numHits) {
        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
          super.setNextReader(context);
          assertFalse("segment should not be recognized as sorted as different sorter was used", segmentSorted);
        }
      });
    }
    assertTrue(collector1.getTotalHits() >= collector2.getTotalHits());
    assertTopDocsEquals(collector1.topDocs().scoreDocs, collector2.topDocs().scoreDocs);
  }

  private static void assertTopDocsEquals(ScoreDoc[] scoreDocs1, ScoreDoc[] scoreDocs2) {
    assertEquals(scoreDocs1.length, scoreDocs2.length);
    for (int i = 0; i < scoreDocs1.length; ++i) {
      final ScoreDoc scoreDoc1 = scoreDocs1[i];
      final ScoreDoc scoreDoc2 = scoreDocs2[i];
      assertEquals(scoreDoc1.doc, scoreDoc2.doc);
      assertEquals(scoreDoc1.score, scoreDoc2.score, 0.001f);
    }
  }

}
