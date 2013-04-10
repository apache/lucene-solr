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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestNumericUtils; // NaN arrays
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNumericRangeQuery32 extends LuceneTestCase {
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
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(_TestUtil.nextInt(random(), 100, 1000))
        .setMergePolicy(newLogMergePolicy()));
    
    final FieldType storedInt = new FieldType(IntField.TYPE_NOT_STORED);
    storedInt.setStored(true);
    storedInt.freeze();

    final FieldType storedInt8 = new FieldType(storedInt);
    storedInt8.setNumericPrecisionStep(8);

    final FieldType storedInt4 = new FieldType(storedInt);
    storedInt4.setNumericPrecisionStep(4);

    final FieldType storedInt2 = new FieldType(storedInt);
    storedInt2.setNumericPrecisionStep(2);

    final FieldType storedIntNone = new FieldType(storedInt);
    storedIntNone.setNumericPrecisionStep(Integer.MAX_VALUE);

    final FieldType unstoredInt = IntField.TYPE_NOT_STORED;

    final FieldType unstoredInt8 = new FieldType(unstoredInt);
    unstoredInt8.setNumericPrecisionStep(8);

    final FieldType unstoredInt4 = new FieldType(unstoredInt);
    unstoredInt4.setNumericPrecisionStep(4);

    final FieldType unstoredInt2 = new FieldType(unstoredInt);
    unstoredInt2.setNumericPrecisionStep(2);

    IntField
      field8 = new IntField("field8", 0, storedInt8),
      field4 = new IntField("field4", 0, storedInt4),
      field2 = new IntField("field2", 0, storedInt2),
      fieldNoTrie = new IntField("field"+Integer.MAX_VALUE, 0, storedIntNone),
      ascfield8 = new IntField("ascfield8", 0, unstoredInt8),
      ascfield4 = new IntField("ascfield4", 0, unstoredInt4),
      ascfield2 = new IntField("ascfield2", 0, unstoredInt2);
    
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
    BooleanQuery.setMaxClauseCount(3*255*2 + 255);
  }
  
  /** test for both constant score and boolean query, the other tests only use the constant score mode */
  private void testRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    int lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    NumericRangeQuery<Integer> q = NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
    NumericRangeFilter<Integer> f = NumericRangeFilter.newIntRange(field, precisionStep, lower, upper, true, true);
    for (byte i=0; i<3; i++) {
      TopDocs topDocs;
      String type;
      switch (i) {
        case 0:
          type = " (constant score filter rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_FILTER_REWRITE);
          topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
          break;
        case 1:
          type = " (constant score boolean rewrite)";
          q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
          topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
          break;
        case 2:
          type = " (filter)";
          topDocs = searcher.search(new MatchAllDocsQuery(), f, noDocs, Sort.INDEXORDER);
          break;
        default:
          return;
      }
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
  public void testInverseRange() throws Exception {
    AtomicReaderContext context = SlowCompositeReaderWrapper.wrap(reader).getContext();
    NumericRangeFilter<Integer> f = NumericRangeFilter.newIntRange("field8", 8, 1000, -1000, true, true);
    assertSame("A inverse range should return the EMPTY_DOCIDSET instance", DocIdSet.EMPTY_DOCIDSET, f.getDocIdSet(context, context.reader().getLiveDocs()));
    f = NumericRangeFilter.newIntRange("field8", 8, Integer.MAX_VALUE, null, false, false);
    assertSame("A exclusive range starting with Integer.MAX_VALUE should return the EMPTY_DOCIDSET instance",
               DocIdSet.EMPTY_DOCIDSET, f.getDocIdSet(context, context.reader().getLiveDocs()));
    f = NumericRangeFilter.newIntRange("field8", 8, null, Integer.MIN_VALUE, false, false);
    assertSame("A exclusive range ending with Integer.MIN_VALUE should return the EMPTY_DOCIDSET instance",
               DocIdSet.EMPTY_DOCIDSET, f.getDocIdSet(context, context.reader().getLiveDocs()));
  }
  
  @Test
  public void testOneMatchQuery() throws Exception {
    NumericRangeQuery<Integer> q = NumericRangeQuery.newIntRange("ascfield8", 8, 1000, 1000, true, true);
    TopDocs topDocs = searcher.search(q, noDocs);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", 1, sd.length );
  }
  
  private void testLeftOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    int upper=(count-1)*distance + (distance/3) + startOffset;
    NumericRangeQuery<Integer> q=NumericRangeQuery.newIntRange(field, precisionStep, null, upper, true, true);
    TopDocs topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, doc.getField(field).numericValue().intValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, doc.getField(field).numericValue().intValue());
    
    q=NumericRangeQuery.newIntRange(field, precisionStep, null, upper, false, true);
    topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
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
    NumericRangeQuery<Integer> q=NumericRangeQuery.newIntRange(field, precisionStep, lower, null, true, true);
    TopDocs topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, doc.getField(field).numericValue().intValue());
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, doc.getField(field).numericValue().intValue());

    q=NumericRangeQuery.newIntRange(field, precisionStep, lower, null, true, false);
    topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
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
      newIndexWriterConfig( TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    Document doc = new Document();
    doc.add(new FloatField("float", Float.NEGATIVE_INFINITY, Field.Store.NO));
    doc.add(new IntField("int", Integer.MIN_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new FloatField("float", Float.POSITIVE_INFINITY, Field.Store.NO));
    doc.add(new IntField("int", Integer.MAX_VALUE, Field.Store.NO));
    writer.addDocument(doc);
    
    doc = new Document();
    doc.add(new FloatField("float", 0.0f, Field.Store.NO));
    doc.add(new IntField("int", 0, Field.Store.NO));
    writer.addDocument(doc);
    
    for (float f : TestNumericUtils.FLOAT_NANs) {
      doc = new Document();
      doc.add(new FloatField("float", f, Field.Store.NO));
      writer.addDocument(doc);
    }
    
    writer.close();
    
    IndexReader r = DirectoryReader.open(dir);
    IndexSearcher s = newSearcher(r);
    
    Query q=NumericRangeQuery.newIntRange("int", null, null, true, true);
    TopDocs topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q=NumericRangeQuery.newIntRange("int", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newIntRange("int", Integer.MIN_VALUE, Integer.MAX_VALUE, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );
    
    q=NumericRangeQuery.newIntRange("int", Integer.MIN_VALUE, Integer.MAX_VALUE, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newFloatRange("float", null, null, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newFloatRange("float", null, null, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newFloatRange("float", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 3,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newFloatRange("float", Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, false, false);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", 1,  topDocs.scoreDocs.length );

    q=NumericRangeQuery.newFloatRange("float", Float.NaN, Float.NaN, true, true);
    topDocs = s.search(q, 10);
    assertEquals("Score doc count", TestNumericUtils.FLOAT_NANs.length,  topDocs.scoreDocs.length );

    r.close();
    dir.close();
  }
  
  private void testRandomTrieAndClassicRangeQuery(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int totalTermCountT=0,totalTermCountC=0,termCountT,termCountC;
    int num = _TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      int lower=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      int upper=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      final BytesRef lowerBytes = new BytesRef(NumericUtils.BUF_SIZE_INT), upperBytes = new BytesRef(NumericUtils.BUF_SIZE_INT);
      NumericUtils.intToPrefixCodedBytes(lower, 0, lowerBytes);
      NumericUtils.intToPrefixCodedBytes(upper, 0, upperBytes);

      // test inclusive range
      NumericRangeQuery<Integer> tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
      TermRangeQuery cq=new TermRangeQuery(field, lowerBytes, upperBytes, true, true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      TopDocs cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test exclusive range
      tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, false, false);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, false, false);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test left exclusive range
      tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, false, true);
      cq=new TermRangeQuery(field, lowerBytes, upperBytes, false, true);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for NumericRangeQuery and TermRangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      totalTermCountT += termCountT = countTerms(tq);
      totalTermCountC += termCountC = countTerms(cq);
      checkTermCounts(precisionStep, termCountT, termCountC);
      // test right exclusive range
      tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, false);
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
    int lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    // test empty enum
    assert lower < upper;
    assertTrue(0 < countTerms(NumericRangeQuery.newIntRange("field4", 4, lower, upper, true, true)));
    assertEquals(0, countTerms(NumericRangeQuery.newIntRange("field4", 4, upper, lower, true, true)));
    // test empty enum outside of bounds
    lower = distance*noDocs+startOffset;
    upper = 2 * lower;
    assert lower < upper;
    assertEquals(0, countTerms(NumericRangeQuery.newIntRange("field4", 4, lower, upper, true, true)));
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
    int num = _TestUtil.nextInt(random(), 10, 20);
    for (int  i =0;  i< num; i++) {
      int lower=(int)(random().nextDouble()*noDocs - noDocs/2);
      int upper=(int)(random().nextDouble()*noDocs - noDocs/2);
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      Query tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
      // test exclusive range
      tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, false, false);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits );
      // test left exclusive range
      tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, false, true);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
      // test right exclusive range
      tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, false);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
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
  
  /** we fake a float test using int2float conversion of NumericUtils */
  private void testFloatRange(int precisionStep) throws Exception {
    final String field="ascfield"+precisionStep;
    final int lower=-1000, upper=+2000;
    
    Query tq=NumericRangeQuery.newFloatRange(field, precisionStep,
      NumericUtils.sortableIntToFloat(lower), NumericUtils.sortableIntToFloat(upper), true, true);
    TopDocs tTopDocs = searcher.search(tq, 1);
    assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
    
    Filter tf=NumericRangeFilter.newFloatRange(field, precisionStep,
      NumericUtils.sortableIntToFloat(lower), NumericUtils.sortableIntToFloat(upper), true, true);
    tTopDocs = searcher.search(new MatchAllDocsQuery(), tf, 1);
    assertEquals("Returned count of range filter must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
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
  
  private void testSorting(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    // 10 random tests, the index order is ascending,
    // so using a reverse sort field should retun descending documents
    int num = _TestUtil.nextInt(random(), 10, 20);
    for (int i = 0; i < num; i++) {
      int lower=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      int upper=(int)(random().nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        int a=lower; lower=upper; upper=a;
      }
      Query tq=NumericRangeQuery.newIntRange(field, precisionStep, lower, upper, true, true);
      TopDocs topDocs = searcher.search(tq, null, noDocs, new Sort(new SortField(field, SortField.Type.INT, true)));
      if (topDocs.totalHits==0) continue;
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      int last = searcher.doc(sd[0].doc).getField(field).numericValue().intValue();
      for (int j=1; j<sd.length; j++) {
        int act = searcher.doc(sd[j].doc).getField(field).numericValue().intValue();
        assertTrue("Docs should be sorted backwards", last>act );
        last=act;
      }
    }
  }

  @Test
  public void testSorting_8bit() throws Exception {
    testSorting(8);
  }
  
  @Test
  public void testSorting_4bit() throws Exception {
    testSorting(4);
  }
  
  @Test
  public void testSorting_2bit() throws Exception {
    testSorting(2);
  }
  
  @Test
  public void testEqualsAndHash() throws Exception {
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test1", 4, 10, 20, true, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test2", 4, 10, 20, false, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test3", 4, 10, 20, true, false));
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test4", 4, 10, 20, false, false));
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test5", 4, 10, null, true, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test6", 4, null, 20, true, true));
    QueryUtils.checkHashEquals(NumericRangeQuery.newIntRange("test7", 4, null, null, true, true));
    QueryUtils.checkEqual(
      NumericRangeQuery.newIntRange("test8", 4, 10, 20, true, true), 
      NumericRangeQuery.newIntRange("test8", 4, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newIntRange("test9", 4, 10, 20, true, true), 
      NumericRangeQuery.newIntRange("test9", 8, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newIntRange("test10a", 4, 10, 20, true, true), 
      NumericRangeQuery.newIntRange("test10b", 4, 10, 20, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newIntRange("test11", 4, 10, 20, true, true), 
      NumericRangeQuery.newIntRange("test11", 4, 20, 10, true, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newIntRange("test12", 4, 10, 20, true, true), 
      NumericRangeQuery.newIntRange("test12", 4, 10, 20, false, true)
    );
    QueryUtils.checkUnequal(
      NumericRangeQuery.newIntRange("test13", 4, 10, 20, true, true), 
      NumericRangeQuery.newFloatRange("test13", 4, 10f, 20f, true, true)
    );
    // the following produces a hash collision, because Long and Integer have the same hashcode, so only test equality:
    Query q1 = NumericRangeQuery.newIntRange("test14", 4, 10, 20, true, true);
    Query q2 = NumericRangeQuery.newLongRange("test14", 4, 10L, 20L, true, true);
    assertFalse(q1.equals(q2));
    assertFalse(q2.equals(q1));
  }
  
}
