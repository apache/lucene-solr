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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.Norm;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

public class TestSimilarityProvider extends LuceneTestCase {
  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    PerFieldSimilarityWrapper sim = new ExampleSimilarityProvider();
    IndexWriterConfig iwc = newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random())).setSimilarity(sim);
    RandomIndexWriter iw = new RandomIndexWriter(random(), directory, iwc);
    Document doc = new Document();
    Field field = newField("foo", "", TextField.TYPE_UNSTORED);
    doc.add(field);
    Field field2 = newField("bar", "", TextField.TYPE_UNSTORED);
    doc.add(field2);
    
    field.setStringValue("quick brown fox");
    field2.setStringValue("quick brown fox");
    iw.addDocument(doc);
    field.setStringValue("jumps over lazy brown dog");
    field2.setStringValue("jumps over lazy brown dog");
    iw.addDocument(doc);
    reader = iw.getReader();
    iw.close();
    searcher = newSearcher(reader);
    searcher.setSimilarity(sim);
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  public void testBasics() throws Exception {
    // sanity check of norms writer
    // TODO: generalize
    byte fooNorms[] = (byte[]) MultiDocValues.getNormDocValues(reader, "foo").getSource().getArray();
    byte barNorms[] = (byte[]) MultiDocValues.getNormDocValues(reader, "bar").getSource().getArray();
    for (int i = 0; i < fooNorms.length; i++) {
      assertFalse(fooNorms[i] == barNorms[i]);
    }
    
    // sanity check of searching
    TopDocs foodocs = searcher.search(new TermQuery(new Term("foo", "brown")), 10);
    assertTrue(foodocs.totalHits > 0);
    TopDocs bardocs = searcher.search(new TermQuery(new Term("bar", "brown")), 10);
    assertTrue(bardocs.totalHits > 0);
    assertTrue(foodocs.scoreDocs[0].score < bardocs.scoreDocs[0].score);
  }
  
  private class ExampleSimilarityProvider extends PerFieldSimilarityWrapper {
    private Similarity sim1 = new Sim1();
    private Similarity sim2 = new Sim2();
    
    public Similarity get(String field) {
      if (field.equals("foo")) {
        return sim1;
      } else {
        return sim2;
      }
    }
  }
  
  private class Sim1 extends TFIDFSimilarity {
    
    public float coord(int overlap, int maxOverlap) {
      return 1f;
    }

    public float queryNorm(float sumOfSquaredWeights) {
      return 1f;
    }

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      norm.setByte(encodeNormValue(1f));
    }

    @Override
    public float sloppyFreq(int distance) {
      return 1f;
    }

    @Override
    public float tf(float freq) {
      return 1f;
    }

    @Override
    public float idf(long docFreq, long numDocs) {
      return 1f;
    }

    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
      return 1f;
    }
  }
  
  private class Sim2 extends TFIDFSimilarity {
    
    public float coord(int overlap, int maxOverlap) {
      return 1f;
    }

    public float queryNorm(float sumOfSquaredWeights) {
      return 1f;
    }
    
    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      norm.setByte(encodeNormValue(10f));
    }

    @Override
    public float sloppyFreq(int distance) {
      return 10f;
    }

    @Override
    public float tf(float freq) {
      return 10f;
    }

    @Override
    public float idf(long docFreq, long numDocs) {
      return 10f;
    }

    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
      return 1f;
    }
  }
}
