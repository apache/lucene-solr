package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Tests the uniqueTermCount statistic in FieldInvertState
 */
public class TestUniqueTermCount extends LuceneTestCase { 
  Directory dir;
  IndexReader reader;
  /* expected uniqueTermCount values for our documents */
  ArrayList<Integer> expected = new ArrayList<Integer>();
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    IndexWriterConfig config = newIndexWriterConfig(TEST_VERSION_CURRENT, 
                                                    new MockAnalyzer(random(), MockTokenizer.SIMPLE, true)).setMergePolicy(newLogMergePolicy());
    config.setSimilarity(new TestSimilarity());
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config);
    Document doc = new Document();
    Field foo = newField("foo", "", TextField.TYPE_UNSTORED);
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
    byte fooNorms[] = (byte[])MultiDocValues.getNormDocValues(reader, "foo").getSource().getArray();
    for (int i = 0; i < reader.maxDoc(); i++)
      assertEquals(expected.get(i).intValue(), fooNorms[i] & 0xff);
  }

  /**
   * Makes a bunch of single-char tokens (the max # unique terms will at most be 26).
   * puts the # unique terms into expected, to be checked against the norm.
   */
  private String addValue() {
    StringBuilder sb = new StringBuilder();
    HashSet<String> terms = new HashSet<String>();
    int num = _TestUtil.nextInt(random(), 0, 255);
    for (int i = 0; i < num; i++) {
      sb.append(' ');
      char term = (char) _TestUtil.nextInt(random(), 'a', 'z');
      sb.append(term);
      terms.add("" + term);
    }
    expected.add(terms.size());
    return sb.toString();
  }
  
  /**
   * Simple similarity that encodes maxTermFrequency directly as a byte
   */
  class TestSimilarity extends DefaultSimilarity {

    @Override
    public void computeNorm(FieldInvertState state, Norm norm) {
      norm.setByte((byte) state.getUniqueTermCount());
    }
  }
}
