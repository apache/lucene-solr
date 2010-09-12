package org.apache.lucene.search;

/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.index.Term;

public class TestBooleanQuery extends LuceneTestCase {
  
  public void testEquality() throws Exception {
    BooleanQuery bq1 = new BooleanQuery();
    bq1.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
    bq1.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
    BooleanQuery nested1 = new BooleanQuery();
    nested1.add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
    nested1.add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
    bq1.add(nested1, BooleanClause.Occur.SHOULD);

    BooleanQuery bq2 = new BooleanQuery();
    bq2.add(new TermQuery(new Term("field", "value1")), BooleanClause.Occur.SHOULD);
    bq2.add(new TermQuery(new Term("field", "value2")), BooleanClause.Occur.SHOULD);
    BooleanQuery nested2 = new BooleanQuery();
    nested2.add(new TermQuery(new Term("field", "nestedvalue1")), BooleanClause.Occur.SHOULD);
    nested2.add(new TermQuery(new Term("field", "nestedvalue2")), BooleanClause.Occur.SHOULD);
    bq2.add(nested2, BooleanClause.Occur.SHOULD);

    assertEquals(bq1, bq2);
  }

  public void testException() {
    try {
      BooleanQuery.setMaxClauseCount(0);
      fail();
    } catch (IllegalArgumentException e) {
      // okay
    }
  }

  // LUCENE-1630
  public void testNullOrSubScorer() throws Throwable {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random, dir);
    Document doc = new Document();
    doc.add(newField("field", "a b c d", Field.Store.NO, Field.Index.ANALYZED));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = new IndexSearcher(r);
    BooleanQuery q = new BooleanQuery();
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);

    // LUCENE-2617: make sure that a term not in the index still contributes to the score via coord factor
    float score = s.search(q, 10).getMaxScore();
    Query subQuery = new TermQuery(new Term("field", "not_in_index"));
    subQuery.setBoost(0);
    q.add(subQuery, BooleanClause.Occur.SHOULD);
    float score2 = s.search(q, 10).getMaxScore();
    assertEquals(score*.5, score2, 1e-6);

    // now test BooleanScorer2
    subQuery = new TermQuery(new Term("field", "b"));
    subQuery.setBoost(0);
    q.add(subQuery, BooleanClause.Occur.MUST);
    score2 = s.search(q, 10).getMaxScore();
    assertEquals(score*(2.0/3), score2, 1e-6);
 
    // PhraseQuery w/ no terms added returns a null scorer
    PhraseQuery pq = new PhraseQuery();
    q.add(pq, BooleanClause.Occur.SHOULD);
    assertEquals(1, s.search(q, 10).totalHits);

    // A required clause which returns null scorer should return null scorer to
    // IndexSearcher.
    q = new BooleanQuery();
    pq = new PhraseQuery();
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);
    q.add(pq, BooleanClause.Occur.MUST);
    assertEquals(0, s.search(q, 10).totalHits);

    DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(1.0f);
    dmq.add(new TermQuery(new Term("field", "a")));
    dmq.add(pq);
    assertEquals(1, s.search(dmq, 10).totalHits);
    
    r.close();
    w.close();
    dir.close();
  }
  
}
