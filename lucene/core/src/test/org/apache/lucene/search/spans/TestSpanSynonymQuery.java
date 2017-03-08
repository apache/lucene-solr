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
package org.apache.lucene.search.spans;


import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;

import org.apache.lucene.search.SynonymQuery;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import junit.framework.Assert;


public class TestSpanSynonymQuery extends LuceneTestCase {
  static IndexSearcher searcher;
  static IndexReader reader;
  static Directory directory;

  static final int MAX_TEST_DOC = 32;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
            .setMaxBufferedDocs(TestUtil.nextInt(random(), MAX_TEST_DOC, MAX_TEST_DOC + 100))
            .setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < MAX_TEST_DOC; i++) {
      Document doc = new Document();
      String text;
      if (i < (MAX_TEST_DOC-1)) {
        text = English.intToEnglish(i);
        if ((i % 5) == 0) { // add some multiple occurrences of the same term(s)
          text += " " + text;
        }
      } else { // last doc, for testing distances > 1, and repeating occurrrences of wb
        text = "az a b c d e wa wb wb wc az";
      }
      doc.add(newTextField("field", text, Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = new IndexSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcher = null;
    directory = null;
  }

  final String FIELD_NAME = "field";


  Term lcnTerm(String term) {
    return new Term(FIELD_NAME, term);
  }

  Term[] lcnTerms(String... terms) {
    Term[] lcnTrms = new Term[terms.length];
    for (int i = 0; i < terms.length; i++) {
      lcnTrms[i] = lcnTerm(terms[i]);
    }
    return lcnTrms;
  }

  TermQuery termQuery(String term) {
    return new TermQuery(lcnTerm(term));
  }

  SpanTermQuery spanTermQuery(String term) {
    return new SpanTermQuery(lcnTerm(term));
  }

  SpanTermQuery[] spanTermQueries(String... terms) {
    SpanTermQuery[] stqs = new SpanTermQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      stqs[i] = spanTermQuery(terms[i]);
    }
    return stqs;
  }

  SpanSynonymQuery spanSynonymQuery(String... terms) {
    return new SpanSynonymQuery(lcnTerms(terms));
  }

  SynonymQuery synonymQuery(String... terms) {
    return new SynonymQuery(lcnTerms(terms));
  }

  void sortByDoc(ScoreDoc[] scoreDocs) {
    Arrays.sort(scoreDocs, new Comparator<ScoreDoc>() {
        @Override
        public int compare(ScoreDoc sd1, ScoreDoc sd2) {
          return sd1.doc - sd2.doc;
        }
    });
  }

  ScoreDoc[] search(IndexSearcher searcher, Query query) throws IOException {
    TopScoreDocCollector collector = TopScoreDocCollector.create(MAX_TEST_DOC);
    searcher.search(query, collector);
    return collector.topDocs().scoreDocs;
  }

  int[] docsFromHits(ScoreDoc[] hits) throws Exception {
    int[] docs = new int[hits.length];
    for (int i = 0; i < hits.length; i++) {
      docs[i] = hits[i].doc;
    }
    return docs;
  }

  void showQueryResults(String message, Query q, ScoreDoc[] hits) {
    System.out.println(message + " results from query " + q);
    for (ScoreDoc hit : hits) {
      System.out.println("doc=" + hit.doc + ", score=" + hit.score);
    }
  }

  void checkEqualScores(Query qexp, Query qact) throws Exception {
    ScoreDoc[] expHits = search(searcher, qexp);

    int[] expDocs = docsFromHits(expHits);
    //showQueryResults("checkEqualScores expected", qexp, expHits);

    ScoreDoc[] actHits = search(searcher, qact);
    //showQueryResults("checkEqualScores actual", qact, actHits);

    CheckHits.checkHitsQuery(qact, actHits, expHits, expDocs);
  }

  void checkScoresInRange(Query qexp, Query qact, float maxFac, float minFac) throws Exception {
    ScoreDoc[] expHits = search(searcher, qexp);
    //showQueryResults("checkScoresInRange expected", qexp, expHits);

    ScoreDoc[] actHits = search(searcher, qact);
    //showQueryResults("checkScoresInRange actual", qact, actHits);

    if (expHits.length != actHits.length) {
      Assert.fail("Unequal lengths: expHits="+expHits.length+",actHits="+actHits.length);
    }

    sortByDoc(expHits);
    sortByDoc(actHits);
    for (int i = 0; i < expHits.length; i++) {
      if (expHits[i].doc != actHits[i].doc)
      {
        Assert.fail("At index " + i
                      + ": expHits[i].doc=" + expHits[i].doc
                      + " != actHits[i].doc=" + actHits[i].doc);
      }

      if ( (expHits[i].score * maxFac < actHits[i].score)
        || (expHits[i].score * minFac > actHits[i].score))
      {
        Assert.fail("At index " + i
                      + ", expHits[i].doc=" + expHits[i].doc
                      + ", score not in expected range: " + (expHits[i].score * minFac)
                      + " <= " + actHits[i].score
                      + " <= " + (expHits[i].score * maxFac));
      }
    }
  }

  void checkSingleTerm(String term) throws Exception {
    TermQuery tq = termQuery(term);
    SpanTermQuery stq = spanTermQuery(term);
    SpanSynonymQuery ssq = spanSynonymQuery(term);

    checkEqualScores(tq, stq);
    checkEqualScores(tq, ssq);
  }

  public void testSingleZero() throws Exception {
    checkSingleTerm("zero");
  }

  SpanOrQuery spanOrQuery(String... terms) {
    return new SpanOrQuery(spanTermQueries(terms));
  }

  void checkOrTerms(String... terms)  throws Exception {
    assertTrue(terms.length >= 1);
    SpanOrQuery soq = spanOrQuery(terms);
    SpanSynonymQuery ssq = spanSynonymQuery(terms);
    checkScoresInRange(soq, ssq, 0.7f, 0.3f);

    SynonymQuery sq = synonymQuery(terms);
    checkEqualScores(sq, ssq);
  }

  public void testOrTwoTermsNoDocOverlap() throws Exception {
    checkOrTerms("zero", "one");
  }

  public void testOrTwoTermsDocOverlap() throws Exception {
    checkOrTerms("twenty", "one");
  }
}
