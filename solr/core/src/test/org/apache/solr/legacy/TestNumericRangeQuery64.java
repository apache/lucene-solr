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
import org.apache.lucene.search.BooleanQuery;
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

public class TestNumericRangeQuery64 extends SolrTestCase {
  // distance of entries
  private static long distance;
  // shift the starting of the values to the left, to also have negative values:
  private static final long startOffset = - 1L << 31;
  // number of docs to generate for testing
  private static int noDocs;
  
  private static Directory directory = null;
  private static IndexReader reader = null;
  private static IndexSearcher searcher = null;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    noDocs = atLeast(4096);
    distance = (1L << 60) / noDocs;
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new MockAnalyzer(random()))
        .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));

    final LegacyFieldType storedLong = new LegacyFieldType(LegacyLongField.TYPE_NOT_STORED);
    storedLong.setStored(true);
    storedLong.freeze();

    final LegacyFieldType storedLong8 = new LegacyFieldType(storedLong);
    storedLong8.setNumericPrecisionStep(8);

    final LegacyFieldType storedLong4 = new LegacyFieldType(storedLong);
    storedLong4.setNumericPrecisionStep(4);

    final LegacyFieldType storedLong6 = new LegacyFieldType(storedLong);
    storedLong6.setNumericPrecisionStep(6);

    final LegacyFieldType storedLong2 = new LegacyFieldType(storedLong);
    storedLong2.setNumericPrecisionStep(2);

    final LegacyFieldType storedLongNone = new LegacyFieldType(storedLong);
    storedLongNone.setNumericPrecisionStep(Integer.MAX_VALUE);

    final LegacyFieldType unstoredLong = LegacyLongField.TYPE_NOT_STORED;

    final LegacyFieldType unstoredLong8 = new LegacyFieldType(unstoredLong);
    unstoredLong8.setNumericPrecisionStep(8);

    final LegacyFieldType unstoredLong6 = new LegacyFieldType(unstoredLong);
    unstoredLong6.setNumericPrecisionStep(6);

    final LegacyFieldType unstoredLong4 = new LegacyFieldType(unstoredLong);
    unstoredLong4.setNumericPrecisionStep(4);

    final LegacyFieldType unstoredLong2 = new LegacyFieldType(unstoredLong);
    unstoredLong2.setNumericPrecisionStep(2);

    LegacyLongField
      field8 = new LegacyLongField("field8", 0L, storedLong8),
      field6 = new LegacyLongField("field6", 0L, storedLong6),
      field4 = new LegacyLongField("field4", 0L, storedLong4),
      field2 = new LegacyLongField("field2", 0L, storedLong2),
      fieldNoTrie = new LegacyLongField("field"+Integer.MAX_VALUE, 0L, storedLongNone),
      ascfield8 = new LegacyLongField("ascfield8", 0L, unstoredLong8),
      ascfield6 = new LegacyLongField("ascfield6", 0L, unstoredLong6),
      ascfield4 = new LegacyLongField("ascfield4", 0L, unstoredLong4),
      ascfield2 = new LegacyLongField("ascfield2", 0L, unstoredLong2);

    Document doc = new Document();
    // add fields, that have a distance to test general functionality
    doc.add(field8); doc.add(field6); doc.add(field4); doc.add(field2); doc.add(fieldNoTrie);
    // add ascending fields with a distance of 1, beginning at -noDocs/2 to test the correct splitting of range and inclusive/exclusive
    doc.add(ascfield8); doc.add(ascfield6); doc.add(ascfield4); doc.add(ascfield2);
    
    // Add a series of noDocs docs with increasing long values, by updating the fields
    for (int l=0; l<noDocs; l++) {
      long val=distance*l+startOffset;
      field8.setLongValue(val);
      field6.setLongValue(val);
      field4.setLongValue(val);
      field2.setLongValue(val);
      fieldNoTrie.setLongValue(val);

      val=l-(noDocs/2);
      ascfield8.setLongValue(val);
      ascfield6.setLongValue(val);
      ascfield4.setLongValue(val);
      ascfield2.setLongValue(val);
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
    BooleanQuery.setMaxClauseCount(7*255*2 + 255);
  }
  
  /** test for constant score + boolean query + filter, the other tests only use the constant score mode */
  private void testRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    LegacyNumericRangeQuery<Long> q = LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, true);
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
      assertEquals("First doc"+type, 2*distance+startOffset, doc.getField(field).numericValue().longValue() );
      doc=searcher.doc(sd[sd.length-1].doc);
      assertEquals("Last doc"+type, (1+count)*distance+startOffset, doc.getField(field).numericValue().longValue() );
    }
  }

  @Test
  public void testRange_8bit() throws Exception {
    testRange(8);
  }
  
  @Test
  public void testRange_6bit() throws Exception {
    testRange(6);
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
    LegacyNumericRangeQuery<Long> q = LegacyNumericRangeQuery.newLongRange("ascfield8", 8, 1000L, 1000L, true, true);
    TopDocs topDocs = searcher.search(q, noDocs);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", 1, sd.length );
  }
  
  private void testLeftOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long upper=(count-1)*distance + (distance/3) + startOffset;
    LegacyNumericRangeQuery<Long> q= LegacyNumericRangeQuery.newLongRange(field, precisionStep, null, upper, true, true);

    TopDocs topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, doc.getField(field).numericValue().longValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, doc.getField(field).numericValue().longValue() );

    q= LegacyNumericRangeQuery.newLongRange(field, precisionStep, null, upper, false, true);
    topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, doc.getField(field).numericValue().longValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, doc.getField(field).numericValue().longValue() );
  }
  
  @Test
  public void testLeftOpenRange_8bit() throws Exception {
    testLeftOpenRange(8);
  }
  
  @Test
  public void testLeftOpenRange_6bit() throws Exception {
    testLeftOpenRange(6);
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
    long lower=(count-1)*distance + (distance/3) +startOffset;
    LegacyNumericRangeQuery<Long> q= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, null, true, true);
    TopDocs topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, doc.getField(field).numericValue().longValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, doc.getField(field).numericValue().longValue() );

    q= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, null, true, false);
    topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, doc.getField(field).numericValue().longValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, doc.getField(field).numericValue().longValue() );
  }
  
  @Test
  public void testRightOpenRange_8bit() throws Exception {
    testRightOpenRange(8);
  }
  
  @Test
  public void testRightOpenRange_6bit() throws Exception {
    testRightOpenRange(6);
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
    doc.add(new LegacyDoubleField("double", Double.NEGATIVE_INFINITY, Field.Store.NO));
    doc.add(new LegacyLongField("long", Long.MIN_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new LegacyDoubleField("double", Double.POSITIVE_INFINITY, Field.Store.NO));
    doc.add(new LegacyLongField("long", Long.MAX_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new LegacyDoubleField("double", 0.0, Field.Store.NO));
    doc.add(new LegacyLongField("long", 0L, Field.Store.NO));
    writer.addDocument(doc);
    
    for (double d : TestLegacyNumericUtils.DOUBLE_NANs) {
      doc = new Document();
      doc.add(new LegacyDoubleField("double", d, Field.Store.NO));
      writer.addDocument(doc);
    }
    
    writer.close();
    
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    
    Query q= LegacyNumericRangeQuery.newLongRange("long", null, null, true, true);
    TopDocs topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q= LegacyNumericRangeQuery.newLongRange("long", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newLongRange("long", Long.MIN_VALUE, Long.MAX_VALUE, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q= LegacyNumericRangeQuery.newLongRange("long", Long.MIN_VALUE, Long.MAX_VALUE, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newDoubleRange("double", null, null, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newDoubleRange("double", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newDoubleRange("double", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newDoubleRange("double", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q= LegacyNumericRangeQuery.newDoubleRange("double", Double.NaN, Double.NaN, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", TestLegacyNumericUtils.DOUBLE_NANs.length,  topDocs.scoreDocs.length );

    r.close();
    dir.close();
  }
  
  private void testRangeSplit(int precisionStep) throws Exception {
    String field="ascfield"+precisionStep;
    // 10 random tests
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      long lower=(long)(random().nextDouble()*noDocs - noDocs/2);
      long upper=(long)(random().nextDouble()*noDocs - noDocs/2);
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      Query tq= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, true);
      TopScoreDocCollector collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      TopDocs tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits.value );
      // test exclusive range
      tq= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, upper, false, false);
      collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits.value );
      // test left exclusive range
      tq= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, upper, false, true);
      collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
      searcher.search(tq, collector);
      tTopDocs = collector.topDocs();
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits.value );
      // test right exclusive range
      tq= LegacyNumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, false);
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
  public void testRangeSplit_6bit() throws Exception {
    testRangeSplit(6);
  }
  
  @Test
  public void testRangeSplit_4bit() throws Exception {
    testRangeSplit(4);
  }
  
  @Test
  public void testRangeSplit_2bit() throws Exception {
    testRangeSplit(2);
  }
  
  /** we fake a double test using long2double conversion of LegacyNumericUtils */
  private void testDoubleRange(int precisionStep) throws Exception {
    final String field="ascfield"+precisionStep;
    final long lower=-1000L, upper=+2000L;
    
    Query tq= LegacyNumericRangeQuery.newDoubleRange(field, precisionStep,
        NumericUtils.sortableLongToDouble(lower), NumericUtils.sortableLongToDouble(upper), true, true);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1, Integer.MAX_VALUE);
    searcher.search(tq, collector);
    TopDocs tTopDocs = collector.topDocs();
    assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits.value );
  }

  @Test
  public void testDoubleRange_8bit() throws Exception {
    testDoubleRange(8);
  }
  
  @Test
  public void testDoubleRange_6bit() throws Exception {
    testDoubleRange(6);
  }
  
  @Test
  public void testDoubleRange_4bit() throws Exception {
    testDoubleRange(4);
  }
  
  @Test
  public void testDoubleRange_2bit() throws Exception {
    testDoubleRange(2);
  }
  
  @Test
  public void testEqualsAndHash() throws Exception {
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test1", 4, 10L, 20L, true, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test2", 4, 10L, 20L, false, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test3", 4, 10L, 20L, true, false));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test4", 4, 10L, 20L, false, false));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test5", 4, 10L, null, true, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test6", 4, null, 20L, true, true));
    QueryUtils.checkHashEquals(LegacyNumericRangeQuery.newLongRange("test7", 4, null, null, true, true));
    QueryUtils.checkEqual(
      LegacyNumericRangeQuery.newLongRange("test8", 4, 10L, 20L, true, true),
      LegacyNumericRangeQuery.newLongRange("test8", 4, 10L, 20L, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newLongRange("test9", 4, 10L, 20L, true, true),
      LegacyNumericRangeQuery.newLongRange("test9", 8, 10L, 20L, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newLongRange("test10a", 4, 10L, 20L, true, true),
      LegacyNumericRangeQuery.newLongRange("test10b", 4, 10L, 20L, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newLongRange("test11", 4, 10L, 20L, true, true),
      LegacyNumericRangeQuery.newLongRange("test11", 4, 20L, 10L, true, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newLongRange("test12", 4, 10L, 20L, true, true),
      LegacyNumericRangeQuery.newLongRange("test12", 4, 10L, 20L, false, true)
    );
    QueryUtils.checkUnequal(
      LegacyNumericRangeQuery.newLongRange("test13", 4, 10L, 20L, true, true),
      LegacyNumericRangeQuery.newFloatRange("test13", 4, 10f, 20f, true, true)
    );
     // difference to int range is tested in TestNumericRangeQuery32
  }
}
