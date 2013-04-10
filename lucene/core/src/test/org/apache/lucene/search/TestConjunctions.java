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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
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
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, analyzer);
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
    BooleanQuery bq = new BooleanQuery();
    bq.add(new TermQuery(new Term(F1, "nutch")), BooleanClause.Occur.MUST);
    bq.add(new TermQuery(new Term(F2, "is")), BooleanClause.Occur.MUST);
    TopDocs td = searcher.search(bq, 3);
    assertEquals(1, td.totalHits);
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
    public SimWeight computeWeight(float queryBoost,
        CollectionStatistics collectionStats, TermStatistics... termStats) {
      return new SimWeight() {
        @Override
        public float getValueForNormalization() {
          return 1; // we don't care
        }
        @Override
        public void normalize(float queryNorm, float topLevelBoost) {
          // we don't care
        }
      };
    }

    @Override
    public ExactSimScorer exactSimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      return new ExactSimScorer() {
        @Override
        public float score(int doc, int freq) {
          return freq;
        }
      };
    }

    @Override
    public SloppySimScorer sloppySimScorer(SimWeight weight, AtomicReaderContext context) throws IOException {
      return new SloppySimScorer() {
        @Override
        public float score(int doc, float freq) {
          return freq;
        }
        
        @Override
        public float computeSlopFactor(int distance) {
          return 1F;
        }

        @Override
        public float computePayloadFactor(int doc, int start, int end, BytesRef payload) {
          return 1F;
        }
      };
    }
  }
}
