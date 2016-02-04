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
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BaseTestRangeFilter extends LuceneTestCase {
  
  public static final boolean F = false;
  public static final boolean T = true;
  
  /**
   * Collation interacts badly with hyphens -- collation produces different
   * ordering than Unicode code-point ordering -- so two indexes are created:
   * one which can't have negative random integers, for testing collated ranges,
   * and the other which can have negative random integers, for all other tests.
   */
  static class TestIndex {
    int maxR;
    int minR;
    boolean allowNegativeRandomInts;
    Directory index;
    
    TestIndex(Random random, int minR, int maxR, boolean allowNegativeRandomInts) {
      this.minR = minR;
      this.maxR = maxR;
      this.allowNegativeRandomInts = allowNegativeRandomInts;
      index = newDirectory(random);
    }
  }
  
  static IndexReader signedIndexReader;
  static IndexReader unsignedIndexReader;
  
  static TestIndex signedIndexDir;
  static TestIndex unsignedIndexDir;
  
  static int minId = 0;
  static int maxId;
  
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
  
  @BeforeClass
  public static void beforeClassBaseTestRangeFilter() throws Exception {
    maxId = atLeast(500);
    signedIndexDir = new TestIndex(random(), Integer.MAX_VALUE, Integer.MIN_VALUE, true);
    unsignedIndexDir = new TestIndex(random(), Integer.MAX_VALUE, 0, false);
    signedIndexReader = build(random(), signedIndexDir);
    unsignedIndexReader = build(random(), unsignedIndexDir);
  }
  
  @AfterClass
  public static void afterClassBaseTestRangeFilter() throws Exception {
    signedIndexReader.close();
    unsignedIndexReader.close();
    signedIndexDir.index.close();
    unsignedIndexDir.index.close();
    signedIndexReader = null;
    unsignedIndexReader = null;
    signedIndexDir = null;
    unsignedIndexDir = null;
  }
  
  private static IndexReader build(Random random, TestIndex index) throws IOException {
    /* build an index */
    
    Document doc = new Document();
    Field idField = newStringField(random, "id", "", Field.Store.YES);
    Field idDVField = new SortedDocValuesField("id", new BytesRef());
    Field intIdField = new IntPoint("id_int", 0);
    Field intDVField = new NumericDocValuesField("id_int", 0);
    Field floatIdField = new FloatPoint("id_float", 0);
    Field floatDVField = new NumericDocValuesField("id_float", 0);
    Field longIdField = new LongPoint("id_long", 0);
    Field longDVField = new NumericDocValuesField("id_long", 0);
    Field doubleIdField = new DoublePoint("id_double", 0);
    Field doubleDVField = new NumericDocValuesField("id_double", 0);
    Field randField = newStringField(random, "rand", "", Field.Store.YES);
    Field randDVField = new SortedDocValuesField("rand", new BytesRef());
    Field bodyField = newStringField(random, "body", "", Field.Store.NO);
    Field bodyDVField = new SortedDocValuesField("body", new BytesRef());
    doc.add(idField);
    doc.add(idDVField);
    doc.add(intIdField);
    doc.add(intDVField);
    doc.add(floatIdField);
    doc.add(floatDVField);
    doc.add(longIdField);
    doc.add(longDVField);
    doc.add(doubleIdField);
    doc.add(doubleDVField);
    doc.add(randField);
    doc.add(randDVField);
    doc.add(bodyField);
    doc.add(bodyDVField);

    RandomIndexWriter writer = new RandomIndexWriter(random, index.index, 
                                                     newIndexWriterConfig(random, new MockAnalyzer(random))
                                                     .setOpenMode(OpenMode.CREATE).setMaxBufferedDocs(TestUtil.nextInt(random, 50, 1000)).setMergePolicy(newLogMergePolicy()));
    TestUtil.reduceOpenFiles(writer.w);

    while(true) {

      int minCount = 0;
      int maxCount = 0;

      for (int d = minId; d <= maxId; d++) {
        idField.setStringValue(pad(d));
        idDVField.setBytesValue(new BytesRef(pad(d)));
        intIdField.setIntValue(d);
        intDVField.setLongValue(d);
        floatIdField.setFloatValue(d);
        floatDVField.setLongValue(Float.floatToRawIntBits(d));
        longIdField.setLongValue(d);
        longDVField.setLongValue(d);
        doubleIdField.setDoubleValue(d);
        doubleDVField.setLongValue(Double.doubleToRawLongBits(d));
        int r = index.allowNegativeRandomInts ? random.nextInt() : random
          .nextInt(Integer.MAX_VALUE);
        if (index.maxR < r) {
          index.maxR = r;
          maxCount = 1;
        } else if (index.maxR == r) {
          maxCount++;
        }

        if (r < index.minR) {
          index.minR = r;
          minCount = 1;
        } else if (r == index.minR) {
          minCount++;
        }
        randField.setStringValue(pad(r));
        randDVField.setBytesValue(new BytesRef(pad(r)));
        bodyField.setStringValue("body");
        bodyDVField.setBytesValue(new BytesRef("body"));
        writer.addDocument(doc);
      }

      if (minCount == 1 && maxCount == 1) {
        // our subclasses rely on only 1 doc having the min or
        // max, so, we loop until we satisfy that.  it should be
        // exceedingly rare (Yonik calculates 1 in ~429,000)
        // times) that this loop requires more than one try:
        IndexReader ir = writer.getReader();
        writer.close();
        return ir;
      }

      // try again
      writer.deleteAll();
    }
  }
  
  @Test
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
