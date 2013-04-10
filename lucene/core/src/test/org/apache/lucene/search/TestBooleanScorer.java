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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AssertingIndexSearcher.AssertingScorer;
import org.apache.lucene.search.BooleanQuery.BooleanWeight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestBooleanScorer extends LuceneTestCase
{
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
    Scorer[] scorers = new Scorer[] {new Scorer(weight) {
      private int doc = -1;
      @Override public float score() { return 0; }
      @Override public int freq()  { return 0; }
      @Override public int docID() { return doc; }
      
      @Override public int nextDoc() {
        return doc = doc == -1 ? 3000 : NO_MORE_DOCS;
      }

      @Override public int advance(int target) {
        return doc = target <= 3000 ? 3000 : NO_MORE_DOCS;
      }
      
      @Override
      public long cost() {
        return 1;
      }
    }};
    
    BooleanScorer bs = new BooleanScorer(weight, false, 1, Arrays.asList(scorers), null, scorers.length);

    final List<Integer> hits = new ArrayList<Integer>();
    bs.score(new Collector() {
      int docBase;
      @Override
      public void setScorer(Scorer scorer) {
      }
      
      @Override
      public void collect(int doc) {
        hits.add(docBase+doc);
      }
      
      @Override
      public void setNextReader(AtomicReaderContext context) {
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
    final IndexSearcher s = newSearcher(r);

    final BooleanQuery q = new BooleanQuery();
    for(int term=0;term<33;term++) {
      q.add(new BooleanClause(new TermQuery(new Term("field", ""+term)),
                              BooleanClause.Occur.MUST_NOT));
    }
    q.add(new BooleanClause(new TermQuery(new Term("field", "33")),
                            BooleanClause.Occur.SHOULD));
                            
    final int[] count = new int[1];
    s.search(q, new Collector() {
    
      @Override
      public void setScorer(Scorer scorer) {
        // Make sure we got BooleanScorer:
        final Class<?> clazz = scorer instanceof AssertingScorer ? ((AssertingScorer) scorer).getIn().getClass() : scorer.getClass();
        assertEquals("Scorer is implemented by wrong class", BooleanScorer.class.getName() + "$BucketScorer", clazz.getName());
      }
      
      @Override
      public void collect(int doc) {
        count[0]++;
      }
      
      @Override
      public void setNextReader(AtomicReaderContext context) {
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
}
