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
package org.apache.solr.legacy;


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNumericRangeQuery32 extends SolrTestCase {
  // distance of entries
  private static int distance;
  // shift the starting of the values to the left, to also have negative values:
  private static final int startOffset = - 1 << 15;
  // number of docs to generate for testing
  private static int noDocs;
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    noDocs = atLeast(4096);
    distance = (1 << 30) / noDocs;
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));
    
    final LegacyFieldType storedInt = new LegacyFieldType(LegacyIntField.TYPE_NOT_STORED);
    storedInt.setStored(true);
    storedInt.freeze();

    final LegacyFieldType storedInt8 = new LegacyFieldType(storedInt);
    storedInt8.setNumericPrecisionStep(8);

    final LegacyFieldType storedInt4 = new LegacyFieldType(storedInt);
    storedInt4.setNumericPrecisionStep(4);

    final LegacyFieldType storedInt2 = new LegacyFieldType(storedInt);
    storedInt2.setNumericPrecisionStep(2);

    final LegacyFieldType storedIntNone = new LegacyFieldType(storedInt);
    storedIntNone.setNumericPrecisionStep(Integer.MAX_VALUE);

    final LegacyFieldType unstoredInt = LegacyIntField.TYPE_NOT_STORED;

    final LegacyFieldType unstoredInt8 = new LegacyFieldType(unstoredInt);
    unstoredInt8.setNumericPrecisionStep(8);

    final LegacyFieldType unstoredInt4 = new LegacyFieldType(unstoredInt);
    unstoredInt4.setNumericPrecisionStep(4);

    final LegacyFieldType unstoredInt2 = new LegacyFieldType(unstoredInt);
    unstoredInt2.setNumericPrecisionStep(2);

    LegacyIntField
      field8 = new LegacyIntField("field8", 0, storedInt8),
      field4 = new LegacyIntField("field4", 0, storedInt4),
      field2 = new LegacyIntField("field2", 0, storedInt2),
      fieldNoTrie = new LegacyIntField("field"+Integer.MAX_VALUE, 0, storedIntNone),
      ascfield8 = new LegacyIntField("ascfield8", 0, unstoredInt8),
      ascfield4 = new LegacyIntField("ascfield4", 0, unstoredInt4),
      ascfield2 = new LegacyIntField("ascfield2", 0, unstoredInt2);
    
    Document doc = new Document();
    // add fields, that have a distance to test general functionality
    doc.add(field8); doc.add(field4); doc.add(field2); doc.add(fieldNoTrie);
    // add ascending fields with a distance of 1, beginning at -noDocs/2 to test the correct splitting of range and inclusive/exclusive
    doc.add(ascfield8); doc.add(ascfield4); doc.add(ascfield2);
    
    // Add a series of noDocs docs with increasing int values
    for (int l=0; l<noDocs; l++) {
      int val=distance*l+startOffset;
      field8.setIntValue(val);
      field4.setIntValue(val);
      field2.setIntValue(val);
      fieldNoTrie.setIntValue(val);

      val=l-(noDocs/2);
      ascfield8.setIntValue(val);
      ascfield4.setIntValue(val);
      ascfield2.setIntValue(val);
      writer.addDocument(doc);
    }
  
    reader = writer.getReader();
    searcher=newSearcher(reader);
    writer.close();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    searcher = null;
    if (null != reader) {
      reader.close();
      reader = null;
    }
    if (null != directory) {
      directory.close();
      directory = null;
    }
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // set the theoretical maximum term count for 8bit (see docs for the number)
    // super.tearDown will restore the default
    IndexSearcher.setMaxClauseCount(3*255*2 + 255);
  }
  
  /** test for both constant score and boolean query, the other tests only use the constant score mode */
  private void testRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    int lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    LegacyNumericRangeQuery<Integer> q = LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
    for (byte i=0; i<2; i++) {
      TopFieldCollector collector = TopFieldCollector.create(Sort.INDEXORDER, noDocs, Integer.MAX_VALUE);
      String type;
      switch (i) {
        case 0:
          type = " (constant score filter rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
          break;
        case 1:
          type = " (constant score boolean rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
          break;
        default:
          return;
      }
      searcher.search(q, collector);
      TopDocs topDocs = collector.topDocs();
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      assertEquals("Score doc count"+type, count, sd.length );
      Document doc=searcher.doc(sd[0].doc);
      assertEquals("First doc"+type, 2*distance+startOffset, doc.getField(field).numericValue().intValue());
      doc=searcher.doc(sd[sd.length-1].doc);
      assertEquals("Last doc"+type, (1+count)*distance+startOffset, doc.getField(field).numericValue().intValue());
    }
  }

  @Test
  public void testRange_8bit() throws Exception {
    testRange(8);
  }
  
  @Test
  public void testRange_4bit() throws Exception {
    testRange(4);
  }
  
  @Test
  public void testRange_2bit() throws Exception {
    testRange(2);
  }
  
  @Test
  public void testOneMatchQuery() throws Exception {
    LegacyNumericRangeQuery<Integer> q = LegacyNumericRangeQuery.newIntRange("ascfield8", 8, 1000, 1000, true, true);
    TopDocs topDocs = searcher.search(q, noDocs);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", 1, sd.length );
  }
  
  private void testLeftOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    int upper=(count-1)*distance + (distance/3) + startOffset;
    LegacyNumericRangeQuery<Integer> q= LegacyNumericRangeQuery.newIntRange(field, precisionStep, null, upper, true, true);
    TopDocs topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, doc.getField(field).numericValue().intValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, doc.getField(field).numericValue().intValue());
    
    q= LegacyNumericRangeQuery.newIntRange(field, precisionStep, null, upper, false, true);
    topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, doc.getField(field).numericValue().intValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, doc.getField(field).numericValue().intValue());
  }
  
  @Test
  public void testLeftOpenRange_8bit() throws Exception {
    testLeftOpenRange(8);
  }
  
  @Test
  public void testLeftOpenRange_4bit() throws Exception {
    testLeftOpenRange(4);
  }
  
  @Test
  public void testLeftOpenRange_2bit() throws Exception {
    testLeftOpenRange(2);
  }
  
  private void testRightOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    int lower=(count-1)*distance + (distance/3) +startOffset;
    LegacyNumericRangeQuery<Integer> q= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, null, true, true);
    TopFieldCollector collector = TopFieldCollector.create(Sort.INDEXORDER, noDocs, Integer.MAX_VALUE);
    searcher.search(q, collector);
    TopDocs topDocs = collector.topDocs();
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, doc.getField(field).numericValue().intValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, doc.getField(field).numericValue().intValue());

    q= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, null, true, false);
    collector = TopFieldCollector.create(Sort.INDEXORDER, noDocs, Integer.MAX_VALUE);
    searcher.search(q, collector);
    topDocs = collector.topDocs();
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, doc.getField(field).numericValue().intValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, doc.getField(field).numericValue().intValue() );
  }
  
  @Test
  public void testRightOpenRange_8bit() throws Exception {
    testRightOpenRange(8);
  }
  
  @Test
  public void testRightOpenRange_4bit() throws Exception {
    testRightOpenRange(4);
  }
  
  @Test
  public void testRightOpenRange_2bit() throws Exception {
    testRightOpenRange(2);
  }
  
  @Test
  public void testInfiniteValues() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir,
      newIndexWriterConfig(new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new LegacyFloatField("float", Float.NEGATIVE_INFINITY, Field.Store.NO));
    doc.add(new LegacyIntField("int", Integer.MIN_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new LegacyFloatField("float", Float.POSITIVE_INFINITY, Field.Store.NO));
    doc.add(new LegacyIntField("int", Integer.MAX_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new LegacyFloatField("float", 0.0f, Field.Store.NO));
    doc.add(new LegacyIntField("int", 0, Field.Store.NO));
    writer.addDocument(doc);
    
    for (float f : TestLegacyNumericUtils.FLOAT_NANs) {
      doc = new Document();
      doc.add(new LegacyFloatField("float", f, Field.Store.NO));
      writer.addDocument(doc);
    }
    
    writer.close();
    
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    
    Query q= LegacyNumericRangeQuery.newIntRange("int", null, null, true, true);
    TopDocs topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q= LegacyNumericRangeQuery.newIntRange("int", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newIntRange("int", Integer.MIN_VALUE, Integer.MAX_VALUE, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q= LegacyNumericRangeQuery.newIntRange("int", Integer.MIN_VALUE, Integer.MAX_VALUE, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newFloatRange("float", null, null, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newFloatRange("float", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newFloatRange("float", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newFloatRange("float", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newFloatRange("float", Float.NaN, Float.NaN, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", TestLegacyNumericUtils.FLOAT_NANs.length,  topDocs.scoreDocs.length );

    r.close();
    dir.close();
  }
  
  private void testRangeSplit(int precisionStep) throws Exception {
    String field="ascfield"+precisionStep;
    // 10 random tests
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int  i =0;  i< num; i++) {
      int lower=(int)(random().nextDouble()*noDocs - noDocs/2);
      int upper=(int)(random().nextDouble()*noDocs - noDocs/2);
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      Query tq= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
      TopScoreDocCollector collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      TopDocs tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits.value );
      // test exclusive range
      tq= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, upper, false, false);
      collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits.value );
      // test left exclusive range
      tq= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, upper, false, true);
      collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits.value );
      // test right exclusive range
      tq= LegacyNumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, false);
      collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits.value );
    }
  }

  @Test
  public void testRangeSplit_8bit() throws Exception {
    testRangeSplit(8);
  }
  
  @Test
  public void testRangeSplit_4bit() throws Exception {
    testRangeSplit(4);
  }
  
  @Test
  public void testRangeSplit_2bit() throws Exception {
    testRangeSplit(2);
  }
  
  /** we fake a float test using int2float conversion of LegacyNumericUtils */
  private void testFloatRange(int precisionStep) throws Exception {
    final String field="ascfield"+precisionStep;
    final int lower=-1000, upper=+2000;
    
    Query tq= LegacyNumericRangeQuery.newFloatRange(field, precisionStep,
        NumericUtils.sortableIntToFloat(lower), NumericUtils.sortableIntToFloat(upper), true, true);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
    searcher.search(tq, collector);
    TopDocs tTopDocs = collector.topDocs();
    assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits.value );
  }

  @Test
  public void testFloatRange_8bit() throws Exception {
    testFloatRange(8);
  }
  
  @Test
  public void testFloatRange_4bit() throws Exception {
    testFloatRange(4);
  }
  
  @Test
  public void testFloatRange_2bit() throws Exception {
    testFloatRange(2);
  }
  
  @Test
  public void testEqualsAndHash() throws Exception {
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test1", 4, 10, 20, true, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test2", 4, 10, 20, false, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test3", 4, 10, 20, true, false));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test4", 4, 10, 20, false, false));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test5", 4, 10, null, true, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test6", 4, null, 20, true, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newIntRange("test7", 4, null, null, true, true));
    QueryUtils.checkEqual(
      LegacyNumericRangeQuery.newIntRange("test8", 4, 10, 20, true, true),
      LegacyNumericRangeQuery.newIntRange("test8", 4, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newIntRange("test9", 4, 10, 20, true, true),
      LegacyNumericRangeQuery.newIntRange("test9", 8, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newIntRange("test10a", 4, 10, 20, true, true),
      LegacyNumericRangeQuery.newIntRange("test10b", 4, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newIntRange("test11", 4, 10, 20, true, true),
      LegacyNumericRangeQuery.newIntRange("test11", 4, 20, 10, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newIntRange("test12", 4, 10, 20, true, true),
      LegacyNumericRangeQuery.newIntRange("test12", 4, 10, 20, false, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newIntRange("test13", 4, 10, 20, true, true),
      LegacyNumericRangeQuery.newFloatRange("test13", 4, 10f, 20f, true, true)
    );
    // the following produces a hash collision, because Long and Integer have the same hashcode, so only test equality:
    Query q1 = LegacyNumericRangeQuery.newIntRange("test14", 4, 10, 20, true, true);
    Query q2 = LegacyNumericRangeQuery.newLongRange("test14", 4, 10L, 20L, true, true);
    assertFalse(q1.equals(q2));
    assertFalse(q2.equals(q1));
  }
  
}
