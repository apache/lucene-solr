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


import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestConjunctions extends LuceneTestCase {
  Analyzer analyzer;
  Directory dir;
  IndexReader reader;
  IndexSearcher searcher;
  
  static final String F1 = "title";
  static final String F2 = "body";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    analyzer = new MockAnalyzer(random());
    dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(analyzer);
    config.setMergePolicy(newLogMergePolicy()); // we will use docids to validate
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    writer.addDocument(doc("lucene", "lucene is a very popular search engine library"));
    writer.addDocument(doc("solr", "solr is a very popular search server and is using lucene"));
    writer.addDocument(doc("nutch", "nutch is an internet search engine with web crawler and is using lucene and hadoop"));
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
    searcher.setSimilarity(new TFSimilarity());
  }
  
  static Document doc(String v1, String v2) {
    Document doc = new Document();
    doc.add(new StringField(F1, v1, Store.YES));
    doc.add(new TextField(F2, v2, Store.YES));
    return doc;
  }
  
  public void testTermConjunctionsWithOmitTF() throws Exception {
    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(new Term(F1, "nutch")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term(F2, "is")), BooleanClause.Occur.MUST);
    TopDocs td = searcher.search(bq.build(), 3);
    assertEquals(1, td.totalHits.value);
    assertEquals(3F, td.scoreDocs[0].score, 0.001F); // f1:nutch + f2:is + f2:is
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  // Similarity that returns the TF as score
  private static class TFSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return 1; // we dont care
    }

    @Override
    public SimScorer scorer(float boost,
        CollectionStatistics collectionStats, TermStatistics... termStats) {
      return new SimScorer() {
        @Override
        public float score(float freq, long norm) {
          return freq;
        }
      };
    }
  }

  public void testScorerGetChildren() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(newTextField("field", "a b", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    BooleanQuery.Builder b = new BooleanQuery.Builder();
    b.add(new TermQuery(new Term("field", "a")), BooleanClause.Occur.MUST);
    b.add(new TermQuery(new Term("field", "b")), BooleanClause.Occur.FILTER);
    Query q = b.build();
    IndexSearcher s = new IndexSearcher(r);
    final boolean[] setScorerCalled = new boolean[1];
    s.search(q, new SimpleCollector() {
        @Override
        public void setScorer(Scorable s) throws IOException {
          Collection<Scorer.ChildScorable> childScorers = s.getChildren();
          setScorerCalled[0] = true;
          assertEquals(2, childScorers.size());
          Set<String> terms = new HashSet<>();
          for (Scorer.ChildScorable childScorer : childScorers) {
            Query query = ((Scorer)childScorer.child).getWeight().getQuery();
            assertTrue(query instanceof TermQuery);
            Term term = ((TermQuery) query).getTerm();
            assertEquals("field", term.field());
            terms.add(term.text());
          }
          assertEquals(2, terms.size());
          assertTrue(terms.contains("a"));
          assertTrue(terms.contains("b"));
        }

        @Override
        public void collect(int doc) {
        }

        @Override
        public ScoreMode scoreMode() {
          return ScoreMode.COMPLETE;
        }
      });
    assertTrue(setScorerCalled[0]);
    IOUtils.close(r, w, dir);
  }
}
