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


import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestNumericUtils; // NaN arrays
import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNumericRangeQuery64 extends LuceneTestCase {
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

    final FieldType storedLong = new FieldType(LongField.TYPE_NOT_STORED);
    storedLong.setStored(true);
    storedLong.freeze();

    final FieldType storedLong8 = new FieldType(storedLong);
    storedLong8.setNumericPrecisionStep(8);

    final FieldType storedLong4 = new FieldType(storedLong);
    storedLong4.setNumericPrecisionStep(4);

    final FieldType storedLong6 = new FieldType(storedLong);
    storedLong6.setNumericPrecisionStep(6);

    final FieldType storedLong2 = new FieldType(storedLong);
    storedLong2.setNumericPrecisionStep(2);

    final FieldType storedLongNone = new FieldType(storedLong);
    storedLongNone.setNumericPrecisionStep(Integer.MAX_VALUE);

    final FieldType unstoredLong = LongField.TYPE_NOT_STORED;

    final FieldType unstoredLong8 = new FieldType(unstoredLong);
    unstoredLong8.setNumericPrecisionStep(8);

    final FieldType unstoredLong6 = new FieldType(unstoredLong);
    unstoredLong6.setNumericPrecisionStep(6);

    final FieldType unstoredLong4 = new FieldType(unstoredLong);
    unstoredLong4.setNumericPrecisionStep(4);

    final FieldType unstoredLong2 = new FieldType(unstoredLong);
    unstoredLong2.setNumericPrecisionStep(2);

    LongField
      field8 = new LongField("field8", 0L, storedLong8),
      field6 = new LongField("field6", 0L, storedLong6),
      field4 = new LongField("field4", 0L, storedLong4),
      field2 = new LongField("field2", 0L, storedLong2),
      fieldNoTrie = new LongField("field"+Integer.MAX_VALUE, 0L, storedLongNone),
      ascfield8 = new LongField("ascfield8", 0L, unstoredLong8),
      ascfield6 = new LongField("ascfield6", 0L, unstoredLong6),
      ascfield4 = new LongField("ascfield4", 0L, unstoredLong4),
      ascfield2 = new LongField("ascfield2", 0L, unstoredLong2);

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
    reader.close();
    reader = null;
    directory.close();
    directory = null;
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
    NumericRangeQuery<Long> q = NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, true);
    for (byte i=0; i<2; i++) {
      TopDocs topDocs;
      String type;
      switch (i) {
        case 0:
          type = " (constant score filter rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
          topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
          break;
        case 1:
          type = " (constant score boolean rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
          topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
          break;
        default:
          return;
      }
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
    NumericRangeQuery<Long> q = NumericRangeQuery.newLongRange("ascfield8", 8, 1000L, 1000L, true, true);
    TopDocs topDocs = searcher.search(q, noDocs);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", 1, sd.length );
  }
  
  private void testLeftOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long upper=(count-1)*distance + (distance/3) + startOffset;
    NumericRangeQuery<Long> q=NumericRangeQuery.newLongRange(field, precisionStep, null, upper, true, true);
    TopDocs topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, doc.getField(field).numericValue().longValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, doc.getField(field).numericValue().longValue() );

    q=NumericRangeQuery.newLongRange(field, precisionStep, null, upper, false, true);
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
    NumericRangeQuery<Long> q=NumericRangeQuery.newLongRange(field, precisionStep, lower, null, true, true);
    TopDocs topDocs = searcher.search(q, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, doc.getField(field).numericValue().longValue() );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, doc.getField(field).numericValue().longValue() );

    q=NumericRangeQuery.newLongRange(field, precisionStep, lower, null, true, false);
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
    doc.add(new DoubleField("double", Double.NEGATIVE_INFINITY, Field.Store.NO));
    doc.add(new LongField("long", Long.MIN_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoubleField("double", Double.POSITIVE_INFINITY, Field.Store.NO));
    doc.add(new LongField("long", Long.MAX_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoubleField("double", 0.0, Field.Store.NO));
    doc.add(new LongField("long", 0L, Field.Store.NO));
    writer.addDocument(doc);
    
    for (double d : TestNumericUtils.DOUBLE_NANs) {
      doc = new Document();
      doc.add(new DoubleField("double", d, Field.Store.NO));
      writer.addDocument(doc);
    }
    
    writer.close();
    
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    
    Query q=NumericRangeQuery.newLongRange("long", null, null, true, true);
    TopDocs topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q=NumericRangeQuery.newLongRange("long", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newLongRange("long", Long.MIN_VALUE, Long.MAX_VALUE, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q=NumericRangeQuery.newLongRange("long", Long.MIN_VALUE, Long.MAX_VALUE, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newDoubleRange("double", null, null, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newDoubleRange("double", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newDoubleRange("double", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newDoubleRange("double", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newDoubleRange("double", Double.NaN, Double.NaN, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", TestNumericUtils.DOUBLE_NANs.length,  topDocs.scoreDocs.length );

    r.close();
    dir.close();
  }
  
  private void testRandomTrieAndClassicRangeQuery(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int totalTermCountT=0,totalTermCountC=0,termCountT,termCountC;
    int num = TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      long lower=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      final BytesRef lowerBytes, upperBytes;
      BytesRefBuilder b = new BytesRefBuilder();
      NumericUtils.longToPrefixCodedBytes(lower, 0, b);
      lowerBytes = b.toBytesRef();
      NumericUtils.longToPrefixCodedBytes(upper, 0, b);
      upperBytes = b.toBytesRef();
      
      // test inclusive range
      NumericRangeQuery<Long> tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, true);
      TermRangeQuery cq=new TermRangeQuery(field, lowerBytes, upperBytes, true, true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      TopDocs cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test exclusive range
      tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, false, false);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, false, false);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test left exclusive range
      tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, false, true);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, false, true);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test right exclusive range
      tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, false);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, true, false);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
    }
    
    checkTermCounts(precisionStep, totalTermCountT, totalTermCountC);
    if (VERBOSE && precisionStep != Integer.MAX_VALUE) {
      System.out.println("Average number of terms during random search on '" + field + "':");
      System.out.println(" Numeric query: " + (((double)totalTermCountT)/(num * 4)));
      System.out.println(" Classical query: " + (((double)totalTermCountC)/(num * 4)));
    }
  }
  
  @Test
  public void testEmptyEnums() throws Exception {
    int count=3000;
    long lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    // test empty enum
    assert lower < upper;
    assertTrue(0 < countTerms(NumericRangeQuery.newLongRange("field4", 4, lower, upper, true, true)));
    assertEquals(0, countTerms(NumericRangeQuery.newLongRange("field4", 4, upper, lower, true, true)));
    // test empty enum outside of bounds
    lower = distance*noDocs+startOffset;
    upper = 2L * lower;
    assert lower < upper;
    assertEquals(0, countTerms(NumericRangeQuery.newLongRange("field4", 4, lower, upper, true, true)));
  }
  
  private int countTerms(MultiTermQuery q) throws Exception {
    final Terms terms = MultiFields.getTerms(reader, q.getField());
    if (terms == null)
      return 0;
    final TermsEnum termEnum = q.getTermsEnum(terms);
    assertNotNull(termEnum);
    int count = 0;
    BytesRef cur, last = null;
    while ((cur = termEnum.next()) != null) {
      count++;
      if (last != null) {
        assertTrue(last.compareTo(cur) < 0);
      }
      last = BytesRef.deepCopyOf(cur);
    } 
    // LUCENE-3314: the results after next() already returned null are undefined,
    // assertNull(termEnum.next());
    return count;
  }
  
  private void checkTermCounts(int precisionStep, int termCountT, int termCountC) {
    if (precisionStep == Integer.MAX_VALUE) {
      assertEquals("Number of terms should be equal for unlimited precStep", termCountC, termCountT);
    } else {
      assertTrue("Number of terms for NRQ should be <= compared to classical TRQ", termCountT <= termCountC);
    }
  }

  @Test
  public void testRandomTrieAndClassicRangeQuery_8bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(8);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_6bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(6);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_4bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(4);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_2bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(2);
  }
  
  @Test
  public void testRandomTrieAndClassicRangeQuery_NoTrie() throws Exception {
    testRandomTrieAndClassicRangeQuery(Integer.MAX_VALUE);
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
      Query tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
      // test exclusive range
      tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, false, false);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits );
      // test left exclusive range
      tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, false, true);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
      // test right exclusive range
      tq=NumericRangeQuery.newLongRange(field, precisionStep, lower, upper, true, false);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
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
  
  /** we fake a double test using long2double conversion of NumericUtils */
  private void testDoubleRange(int precisionStep) throws Exception {
    final String field="ascfield"+precisionStep;
    final long lower=-1000L, upper=+2000L;
    
    Query tq=NumericRangeQuery.newDoubleRange(field, precisionStep,
      NumericUtils.sortableLongToDouble(lower), NumericUtils.sortableLongToDouble(upper), true, true);
    TopDocs tTopDocs = searcher.search(tq, 1);
    assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
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
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test1", 4, 10L, 20L, true, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test2", 4, 10L, 20L, false, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test3", 4, 10L, 20L, true, false));
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test4", 4, 10L, 20L, false, false));
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test5", 4, 10L, null, true, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test6", 4, null, 20L, true, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newLongRange("test7", 4, null, null, true, true));
    QueryUtils.checkEqual(
      NumericRangeQuery.newLongRange("test8", 4, 10L, 20L, true, true), 
      NumericRangeQuery.newLongRange("test8", 4, 10L, 20L, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newLongRange("test9", 4, 10L, 20L, true, true), 
      NumericRangeQuery.newLongRange("test9", 8, 10L, 20L, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newLongRange("test10a", 4, 10L, 20L, true, true), 
      NumericRangeQuery.newLongRange("test10b", 4, 10L, 20L, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newLongRange("test11", 4, 10L, 20L, true, true), 
      NumericRangeQuery.newLongRange("test11", 4, 20L, 10L, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newLongRange("test12", 4, 10L, 20L, true, true), 
      NumericRangeQuery.newLongRange("test12", 4, 10L, 20L, false, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newLongRange("test13", 4, 10L, 20L, true, true), 
      NumericRangeQuery.newFloatRange("test13", 4, 10f, 20f, true, true)
    );
     // difference to int range is tested in TestNumericRangeQuery32
  }
}
