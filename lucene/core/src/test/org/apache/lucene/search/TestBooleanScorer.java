package org.apache.lucene.search;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery.BooleanWeight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LuceneTestCase;

public class TestBooleanScorer extends LuceneTestCase {
  private static final String FIELD = "category";
  
  public void testMethod() throws Exception {
    Directory directory = newDirectory();

    String[] values = new String[] { "1", "2", "3", "4" };

    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    for (int i = 0; i < values.length; i++) {
      Document doc = new Document();
      doc.add(newStringField(FIELD, values[i], Field.Store.YES));
      writer.addDocument(doc);
    }
    IndexReader ir = writer.getReader();
    writer.close();

    BooleanQuery booleanQuery1 = new BooleanQuery();
    booleanQuery1.add(new TermQuery(new Term(FIELD, "1")), BooleanClause.Occur.SHOULD);
    booleanQuery1.add(new TermQuery(new Term(FIELD, "2")), BooleanClause.Occur.SHOULD);

    BooleanQuery query = new BooleanQuery();
    query.add(booleanQuery1, BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(FIELD, "9")), BooleanClause.Occur.MUST_NOT);

    IndexSearcher indexSearcher = newSearcher(ir);
    ScoreDoc[] hits = indexSearcher.search(query, null, 1000).scoreDocs;
    assertEquals("Number of matched documents", 2, hits.length);
    ir.close();
    directory.close();
  }
  
  public void testEmptyBucketWithMoreDocs() throws Exception {
    // This test checks the logic of nextDoc() when all sub scorers have docs
    // beyond the first bucket (for example). Currently, the code relies on the
    // 'more' variable to work properly, and this test ensures that if the logic
    // changes, we have a test to back it up.
    
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    writer.commit();
    IndexReader ir = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(ir);
    BooleanWeight weight = (BooleanWeight) new BooleanQuery().createWeight(searcher);
    BulkScorer[] scorers = new BulkScorer[] {new BulkScorer() {
      private int doc = -1;

      @Override
      public boolean score(LeafCollector c, int maxDoc) throws IOException {
        assert doc == -1;
        doc = 3000;
        FakeScorer fs = new FakeScorer();
        fs.doc = doc;
        fs.score = 1.0f;
        c.setScorer(fs);
        c.collect(3000);
        return false;
      }
    }};
    
    BooleanScorer bs = new BooleanScorer(weight, false, 1, Arrays.asList(scorers), Collections.<BulkScorer>emptyList(), scorers.length);

    final List<Integer> hits = new ArrayList<>();
    bs.score(new SimpleCollector() {
      int docBase;
      @Override
      public void setScorer(Scorer scorer) {
      }
      
      @Override
      public void collect(int doc) {
        hits.add(docBase+doc);
      }
      
      @Override
      protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        docBase = context.docBase;
      }
      
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }
      });

    assertEquals("should have only 1 hit", 1, hits.size());
    assertEquals("hit should have been docID=3000", 3000, hits.get(0).intValue());
    ir.close();
    directory.close();
  }

  public void testMoreThan32ProhibitedClauses() throws Exception {
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    Document doc = new Document();
    doc.add(new TextField("field", "0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33", Field.Store.NO));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new TextField("field", "33", Field.Store.NO));
    w.addDocument(doc);
    final IndexReader r = w.getReader();
    w.close();
    // we don't wrap with AssertingIndexSearcher in order to have the original scorer in setScorer.
    final IndexSearcher s = newSearcher(r, true, false);

    final BooleanQuery q = new BooleanQuery();
    for(int term=0;term<33;term++) {
      q.add(new BooleanClause(new TermQuery(new Term("field", ""+term)),
                              BooleanClause.Occur.MUST_NOT));
    }
    q.add(new BooleanClause(new TermQuery(new Term("field", "33")),
                            BooleanClause.Occur.SHOULD));
                            
    final int[] count = new int[1];
    s.search(q, new SimpleCollector() {
    
      @Override
      public void setScorer(Scorer scorer) {
        // Make sure we got BooleanScorer:
        final Class<?> clazz = scorer.getClass();
        assertEquals("Scorer is implemented by wrong class", FakeScorer.class.getName(), clazz.getName());
      }
      
      @Override
      public void collect(int doc) {
        count[0]++;
      }
      
      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }
    });

    assertEquals(1, count[0]);
    
    r.close();
    d.close();
  }

  /** Throws UOE if Weight.scorer is called */
  private static class CrazyMustUseBulkScorerQuery extends Query {

    @Override
    public String toString(String field) {
      return "MustUseBulkScorerQuery";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
      return new Weight() {
        @Override
        public Explanation explain(AtomicReaderContext context, int doc) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Query getQuery() {
          return CrazyMustUseBulkScorerQuery.this;
        }

        @Override
        public float getValueForNormalization() {
          return 1.0f;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) {
          throw new UnsupportedOperationException();
        }

        @Override
        public BulkScorer bulkScorer(AtomicReaderContext context, boolean scoreDocsInOrder, Bits acceptDocs) {
          return new BulkScorer() {

            @Override
            public boolean score(LeafCollector collector, int max) throws IOException {
              collector.setScorer(new FakeScorer());
              collector.collect(0);
              return false;
            }
          };
        }
      };
    }
  }

  /** Make sure BooleanScorer can embed another
   *  BooleanScorer. */
  public void testEmbeddedBooleanScorer() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "doctors are people who prescribe medicines of which they know little, to cure diseases of which they know less, in human beings of whom they know nothing", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);
    BooleanQuery q1 = new BooleanQuery();
    q1.add(new TermQuery(new Term("field", "little")), BooleanClause.Occur.SHOULD);
    q1.add(new TermQuery(new Term("field", "diseases")), BooleanClause.Occur.SHOULD);

    BooleanQuery q2 = new BooleanQuery();
    q2.add(q1, BooleanClause.Occur.SHOULD);
    q2.add(new CrazyMustUseBulkScorerQuery(), BooleanClause.Occur.SHOULD);

    assertEquals(1, s.search(q2, 10).totalHits);
    r.close();
    dir.close();
  }
}
