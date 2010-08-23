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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;

public class BaseTestRangeFilter extends LuceneTestCase {
  
  public static final boolean F = false;
  public static final boolean T = true;
  
  protected Random rand = newRandom();
  
  /**
   * Collation interacts badly with hyphens -- collation produces different
   * ordering than Unicode code-point ordering -- so two indexes are created:
   * one which can't have negative random integers, for testing collated ranges,
   * and the other which can have negative random integers, for all other tests.
   */
  class TestIndex {
    int maxR;
    int minR;
    boolean allowNegativeRandomInts;
    Directory index;
    
    TestIndex(int minR, int maxR, boolean allowNegativeRandomInts) {
      this.minR = minR;
      this.maxR = maxR;
      this.allowNegativeRandomInts = allowNegativeRandomInts;
      try {
        index = newDirectory(rand);
      } catch (IOException e) { throw new RuntimeException(e); }
    }
  }
  
  IndexReader signedIndexReader;
  IndexReader unsignedIndexReader;
  
  TestIndex signedIndexDir;
  TestIndex unsignedIndexDir;
  
  int minId = 0;
  int maxId = 10000;
  
  static final int intLength = Integer.toString(Integer.MAX_VALUE).length();
  
  /**
   * a simple padding function that should work with any int
   */
  public static String pad(int n) {
    StringBuilder b = new StringBuilder(40);
    String p = "0";
    if (n < 0) {
      p = "-";
      n = Integer.MAX_VALUE + n + 1;
    }
    b.append(p);
    String s = Integer.toString(n);
    for (int i = s.length(); i <= intLength; i++) {
      b.append("0");
    }
    b.append(s);
    
    return b.toString();
  }
   
  protected void setUp() throws Exception {
    super.setUp();
    signedIndexDir = new TestIndex(Integer.MAX_VALUE, Integer.MIN_VALUE, true);
    unsignedIndexDir = new TestIndex(Integer.MAX_VALUE, 0, false);
    signedIndexReader = build(rand, signedIndexDir);
    unsignedIndexReader = build(rand, unsignedIndexDir);
  }
  
  protected void tearDown() throws Exception {
    signedIndexReader.close();
    unsignedIndexReader.close();
    signedIndexDir.index.close();
    unsignedIndexDir.index.close();
    super.tearDown();
  }
  
  private IndexReader build(Random random, TestIndex index) throws IOException {
    /* build an index */
    RandomIndexWriter writer = new RandomIndexWriter(random, index.index, 
        newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer())
    .setOpenMode(OpenMode.CREATE));
    
    for (int d = minId; d <= maxId; d++) {
      Document doc = new Document();
      doc.add(new Field("id", pad(d), Field.Store.YES,
          Field.Index.NOT_ANALYZED));
      int r = index.allowNegativeRandomInts ? rand.nextInt() : rand
          .nextInt(Integer.MAX_VALUE);
      if (index.maxR < r) {
        index.maxR = r;
      }
      if (r < index.minR) {
        index.minR = r;
      }
      doc.add(new Field("rand", pad(r), Field.Store.YES,
          Field.Index.NOT_ANALYZED));
      doc.add(new Field("body", "body", Field.Store.YES,
          Field.Index.NOT_ANALYZED));
      writer.addDocument(doc);
    }
    
    IndexReader ir = writer.getReader();
    writer.close();
    return ir;
  }
  
  public void testPad() {
    
    int[] tests = new int[] {-9999999, -99560, -100, -3, -1, 0, 3, 9, 10, 1000,
        999999999};
    for (int i = 0; i < tests.length - 1; i++) {
      int a = tests[i];
      int b = tests[i + 1];
      String aa = pad(a);
      String bb = pad(b);
      String label = a + ":" + aa + " vs " + b + ":" + bb;
      assertEquals("length of " + label, aa.length(), bb.length());
      assertTrue("compare less than " + label, aa.compareTo(bb) < 0);
    }
    
  }
  
}
