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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Tests the maxTermFrequency statistic in FieldInvertState
 */
public class TestMaxTermFrequency extends LuceneTestCase { 
  Directory dir;
  IndexReader reader;
  /* expected maxTermFrequency values for our documents */
  ArrayList<Integer> expected = new ArrayList<>();
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
                                 .setMergePolicy(newLogMergePolicy());
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
    for (int i = 0; i < reader.maxDoc(); i++) {
      assertEquals(expected.get(i).intValue(), fooNorms.get(i) & 0xff);
    }
  }

  /**
   * Makes a bunch of single-char tokens (the max freq will at most be 255).
   * shuffles them around, and returns the whole list with Arrays.toString().
   * This works fine because we use lettertokenizer.
   * puts the max-frequency term into expected, to be checked against the norm.
   */
  private String addValue() {
    List<String> terms = new ArrayList<>();
    int maxCeiling = TestUtil.nextInt(random(), 0, 255);
    int max = 0;
    for (char ch = 'a'; ch <= 'z'; ch++) {
      int num = TestUtil.nextInt(random(), 0, maxCeiling);
      for (int i = 0; i < num; i++)
        terms.add(Character.toString(ch));
      max = Math.max(max, num);
    }
    expected.add(max);
    Collections.shuffle(terms, random());
    return Arrays.toString(terms.toArray(new String[terms.size()]));
  }
  
  /**
   * Simple similarity that encodes maxTermFrequency directly as a byte
   */
  class TestSimilarity extends TFIDFSimilarity {

    @Override
    public float lengthNorm(FieldInvertState state) {
      return state.getMaxTermFrequency();
    }

    @Override
    public long encodeNormValue(float f) {
      return (byte) f;
    }

    @Override
    public float decodeNormValue(long norm) {
      return norm;
    }

    @Override public float coord(int overlap, int maxOverlap) { return 0; }
    @Override public float queryNorm(float sumOfSquaredWeights) { return 0; }
    @Override public float tf(float freq) { return 0; }
    @Override public float idf(long docFreq, long docCount) { return 0; }
    @Override public float sloppyFreq(int distance) { return 0; }
    @Override public float scorePayload(int doc, int start, int end, BytesRef payload) { return 0; }
  }
}
