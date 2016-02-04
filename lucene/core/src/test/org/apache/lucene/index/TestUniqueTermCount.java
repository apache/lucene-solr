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
package org.apache.lucene.index;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests the uniqueTermCount statistic in FieldInvertState
 */
public class TestUniqueTermCount extends LuceneTestCase { 
  Directory dir;
  IndexReader reader;
  /* expected uniqueTermCount values for our documents */
  ArrayList<Integer> expected = new ArrayList<>();
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);
    IndexWriterConfig config = newIndexWriterConfig(analyzer);
    config.setMergePolicy(newLogMergePolicy());
    config.setSimilarity(new TestSimilarity());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    Field foo = newTextField("foo", "", Field.Store.NO);
    doc.add(foo);
    for (int i = 0; i < 100; i++) {
      foo.setStringValue(addValue());
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
  }
  
  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }
  
  public void test() throws Exception {
    NumericDocValues fooNorms = MultiDocValues.getNormValues(reader, "foo");
    assertNotNull(fooNorms);
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(expected.get(i).longValue(), fooNorms.get(i));
    }
  }

  /**
   * Makes a bunch of single-char tokens (the max # unique terms will at most be 26).
   * puts the # unique terms into expected, to be checked against the norm.
   */
  private String addValue() {
    StringBuilder sb = new StringBuilder();
    HashSet<String> terms = new HashSet<>();
    int num = TestUtil.nextInt(random(), 0, 255);
    for (int i = 0; i < num; i++) {
      sb.append(' ');
      char term = (char) TestUtil.nextInt(random(), 'a', 'z');
      sb.append(term);
      terms.add("" + term);
    }
    expected.add(terms.size());
    return sb.toString();
  }
  
  /**
   * Simple similarity that encodes maxTermFrequency directly
   */
  class TestSimilarity extends Similarity {

    @Override
    public long computeNorm(FieldInvertState state) {
      return state.getUniqueTermCount();
    }

    @Override
    public SimWeight computeWeight(CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimScorer simScorer(SimWeight weight, LeafReaderContext context) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
