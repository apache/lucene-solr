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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util._TestUtil;

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
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "a b c d", Field.Store.NO));
    w.addDocument(doc);

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    // this test relies upon coord being the default implementation,
    // otherwise scores are different!
    s.setSimilarity(new DefaultSimilarity());

    BooleanQuery q = new BooleanQuery();
    q.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.SHOULD);

    // LUCENE-2617: make sure that a term not in the index still contributes to the score via coord factor
    float score = s.search(q, 10).getMaxScore();
    Query subQuery = new TermQuery(new Term("field", "not_in_index"));
    subQuery.setBoost(0);
    q.add(subQuery, BooleanClause.Occur.SHOULD);
    float score2 = s.search(q, 10).getMaxScore();
    assertEquals(score*.5F, score2, 1e-6);

    // LUCENE-2617: make sure that a clause not in the index still contributes to the score via coord factor
    BooleanQuery qq = q.clone();
    PhraseQuery phrase = new PhraseQuery();
    phrase.add(new Term("field", "not_in_index"));
    phrase.add(new Term("field", "another_not_in_index"));
    phrase.setBoost(0);
    qq.add(phrase, BooleanClause.Occur.SHOULD);
    score2 = s.search(qq, 10).getMaxScore();
    assertEquals(score*(1/3F), score2, 1e-6);

    // now test BooleanScorer2
    subQuery = new TermQuery(new Term("field", "b"));
    subQuery.setBoost(0);
    q.add(subQuery, BooleanClause.Occur.MUST);
    score2 = s.search(q, 10).getMaxScore();
    assertEquals(score*(2/3F), score2, 1e-6);
 
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

  public void testDeMorgan() throws Exception {
    Directory dir1 = newDirectory();
    RandomIndexWriter iw1 = new RandomIndexWriter(random(), dir1);
    Document doc1 = new Document();
    doc1.add(newTextField("field", "foo bar", Field.Store.NO));
    iw1.addDocument(doc1);
    IndexReader reader1 = iw1.getReader();
    iw1.close();
    
    Directory dir2 = newDirectory();
    RandomIndexWriter iw2 = new RandomIndexWriter(random(), dir2);
    Document doc2 = new Document();
    doc2.add(newTextField("field", "foo baz", Field.Store.NO));
    iw2.addDocument(doc2);
    IndexReader reader2 = iw2.getReader();
    iw2.close();

    BooleanQuery query = new BooleanQuery(); // Query: +foo -ba*
    query.add(new TermQuery(new Term("field", "foo")), BooleanClause.Occur.MUST);
    WildcardQuery wildcardQuery = new WildcardQuery(new Term("field", "ba*"));
    wildcardQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    query.add(wildcardQuery, BooleanClause.Occur.MUST_NOT);
    
    MultiReader multireader = new MultiReader(reader1, reader2);
    IndexSearcher searcher = newSearcher(multireader);
    assertEquals(0, searcher.search(query, 10).totalHits);
    
    final ExecutorService es = Executors.newCachedThreadPool(new NamedThreadFactory("NRT search threads"));
    searcher = new IndexSearcher(multireader, es);
    if (VERBOSE)
      System.out.println("rewritten form: " + searcher.rewrite(query));
    assertEquals(0, searcher.search(query, 10).totalHits);
    es.shutdown();
    es.awaitTermination(1, TimeUnit.SECONDS);

    multireader.close();
    reader1.close();
    reader2.close();
    dir1.close();
    dir2.close();
  }

  public void testBS2DisjunctionNextVsAdvance() throws Exception {
    final Directory d = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), d);
    final int numDocs = atLeast(300);
    for(int docUpto=0;docUpto<numDocs;docUpto++) {
      String contents = "a";
      if (random().nextInt(20) <= 16) {
        contents += " b";
      }
      if (random().nextInt(20) <= 8) {
        contents += " c";
      }
      if (random().nextInt(20) <= 4) {
        contents += " d";
      }
      if (random().nextInt(20) <= 2) {
        contents += " e";
      }
      if (random().nextInt(20) <= 1) {
        contents += " f";
      }
      Document doc = new Document();
      doc.add(new TextField("field", contents, Field.Store.NO));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    final IndexReader r = w.getReader();
    final IndexSearcher s = newSearcher(r);
    w.close();

    for(int iter=0;iter<10*RANDOM_MULTIPLIER;iter++) {
      if (VERBOSE) {
        System.out.println("iter=" + iter);
      }
      final List<String> terms = new ArrayList<String>(Arrays.asList("a", "b", "c", "d", "e", "f"));
      final int numTerms = _TestUtil.nextInt(random(), 1, terms.size());
      while(terms.size() > numTerms) {
        terms.remove(random().nextInt(terms.size()));
      }

      if (VERBOSE) {
        System.out.println("  terms=" + terms);
      }

      final BooleanQuery q = new BooleanQuery();
      for(String term : terms) {
        q.add(new BooleanClause(new TermQuery(new Term("field", term)), BooleanClause.Occur.SHOULD));
      }

      Weight weight = s.createNormalizedWeight(q);

      Scorer scorer = weight.scorer(s.leafContexts.get(0),
                                          true, false, null);

      // First pass: just use .nextDoc() to gather all hits
      final List<ScoreDoc> hits = new ArrayList<ScoreDoc>();
      while(scorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        hits.add(new ScoreDoc(scorer.docID(), scorer.score()));
      }

      if (VERBOSE) {
        System.out.println("  " + hits.size() + " hits");
      }

      // Now, randomly next/advance through the list and
      // verify exact match:
      for(int iter2=0;iter2<10;iter2++) {

        weight = s.createNormalizedWeight(q);
        scorer = weight.scorer(s.leafContexts.get(0),
                               true, false, null);

        if (VERBOSE) {
          System.out.println("  iter2=" + iter2);
        }

        int upto = -1;
        while(upto < hits.size()) {
          final int nextUpto;
          final int nextDoc;
          final int left = hits.size() - upto;
          if (left == 1 || random().nextBoolean()) {
            // next
            nextUpto = 1+upto;
            nextDoc = scorer.nextDoc();
          } else {
            // advance
            int inc = _TestUtil.nextInt(random(), 1, left-1);
            nextUpto = inc + upto;
            nextDoc = scorer.advance(hits.get(nextUpto).doc);
          }

          if (nextUpto == hits.size()) {
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, nextDoc);
          } else {
            final ScoreDoc hit = hits.get(nextUpto);
            assertEquals(hit.doc, nextDoc);
            // Test for precise float equality:
            assertTrue("doc " + hit.doc + " has wrong score: expected=" + hit.score + " actual=" + scorer.score(), hit.score == scorer.score());
          }
          upto = nextUpto;
        }
      }
    }
    
    r.close();
    d.close();
  }

  // LUCENE-4477 / LUCENE-4401:
  public void testBooleanSpanQuery() throws Exception {
    boolean failed = false;
    int hits = 0;
    Directory directory = newDirectory();
    Analyzer indexerAnalyzer = new MockAnalyzer(random());

    IndexWriterConfig config = new IndexWriterConfig(TEST_VERSION_CURRENT, indexerAnalyzer);
    IndexWriter writer = new IndexWriter(directory, config);
    String FIELD = "content";
    Document d = new Document();
    d.add(new TextField(FIELD, "clockwork orange", Field.Store.YES));
    writer.addDocument(d);
    writer.close();

    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(indexReader);

    BooleanQuery query = new BooleanQuery();
    SpanQuery sq1 = new SpanTermQuery(new Term(FIELD, "clockwork"));
    SpanQuery sq2 = new SpanTermQuery(new Term(FIELD, "clckwork"));
    query.add(sq1, BooleanClause.Occur.SHOULD);
    query.add(sq2, BooleanClause.Occur.SHOULD);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, true);
    searcher.search(query, collector);
    hits = collector.topDocs().scoreDocs.length;
    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs){
      System.out.println(scoreDoc.doc);
    }
    indexReader.close();
    assertEquals("Bug in boolean query composed of span queries", failed, false);
    assertEquals("Bug in boolean query composed of span queries", hits, 1);
    directory.close();
  }

}
