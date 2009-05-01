package org.apache.lucene.search.trie;

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

import java.util.Random;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestLongTrieRangeQuery extends LuceneTestCase {
  // distance of entries
  private static final long distance = 66666L;
  // shift the starting of the values to the left, to also have negative values:
  private static final long startOffset = - 1L << 31;
  // number of docs to generate for testing
  private static final int noDocs = 10000;
  
  private static Field newField(String name, int precisionStep) {
    LongTrieTokenStream stream = new LongTrieTokenStream(precisionStep);
    stream.setUseNewAPI(true);
    Field f=new Field(name, stream);
    f.setOmitTermFreqAndPositions(true);
    f.setOmitNorms(true);
    return f;
  }
  
  private static final RAMDirectory directory;
  private static final IndexSearcher searcher;
  static {
    try {
      // set the theoretical maximum term count for 8bit (see docs for the number)
      BooleanQuery.setMaxClauseCount(7*255*2 + 255);
      
      directory = new RAMDirectory();
      IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(),
      true, MaxFieldLength.UNLIMITED);
      
      Field
        field8 = newField("field8", 8),
        field4 = newField("field4", 4),
        field2 = newField("field2", 2),
        ascfield8 = newField("ascfield8", 8),
        ascfield4 = newField("ascfield4", 4),
        ascfield2 = newField("ascfield2", 2);
      
      // Add a series of noDocs docs with increasing long values
      for (int l=0; l<noDocs; l++) {
        Document doc=new Document();
        // add fields, that have a distance to test general functionality
        long val=distance*l+startOffset;
        doc.add(new Field("value", TrieUtils.longToPrefixCoded(val), Field.Store.YES, Field.Index.NO));
        ((LongTrieTokenStream)field8.tokenStreamValue()).setValue(val);
        doc.add(field8);
        ((LongTrieTokenStream)field4.tokenStreamValue()).setValue(val);
        doc.add(field4);
        ((LongTrieTokenStream)field2.tokenStreamValue()).setValue(val);
        doc.add(field2);
        // add ascending fields with a distance of 1, beginning at -noDocs/2 to test the correct splitting of range and inclusive/exclusive
        val=l-(noDocs/2);
        ((LongTrieTokenStream)ascfield8.tokenStreamValue()).setValue(val);
        doc.add(ascfield8);
        ((LongTrieTokenStream)ascfield4.tokenStreamValue()).setValue(val);
        doc.add(ascfield4);
        ((LongTrieTokenStream)ascfield2.tokenStreamValue()).setValue(val);
        doc.add(ascfield2);
        writer.addDocument(doc);
      }
    
      writer.optimize();
      writer.close();
      searcher=new IndexSearcher(directory);
    } catch (Exception e) {
      throw new Error(e);
    }
  }
  
  /** test for constant score + boolean query + filter, the other tests only use the constant score mode */
  private void testRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long lower=(distance*3/2)+startOffset, upper=lower + count*distance + (distance/3);
    LongTrieRangeQuery q = new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), true, true);
    LongTrieRangeFilter f = new LongTrieRangeFilter(field, precisionStep, new Long(lower), new Long(upper), true, true);
    int lastTerms = 0;
    for (byte i=0; i<3; i++) {
      TopDocs topDocs;
      int terms;
      String type;
      q.clearTotalNumberOfTerms();
      f.clearTotalNumberOfTerms();
      switch (i) {
        case 0:
          type = " (constant score)";
          q.setConstantScoreRewrite(true);
          topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
          terms = q.getTotalNumberOfTerms();
          break;
        case 1:
          type = " (boolean query)";
          q.setConstantScoreRewrite(false);
          topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
          terms = q.getTotalNumberOfTerms();
          break;
        case 2:
          type = " (filter)";
          topDocs = searcher.search(new MatchAllDocsQuery(), f, noDocs, Sort.INDEXORDER);
          terms = f.getTotalNumberOfTerms();
          break;
        default:
          return;
      }
      System.out.println("Found "+terms+" distinct terms in range for field '"+field+"'"+type+".");
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      assertEquals("Score doc count"+type, count, sd.length );
      Document doc=searcher.doc(sd[0].doc);
      assertEquals("First doc"+type, 2*distance+startOffset, TrieUtils.prefixCodedToLong(doc.get("value")) );
      doc=searcher.doc(sd[sd.length-1].doc);
      assertEquals("Last doc"+type, (1+count)*distance+startOffset, TrieUtils.prefixCodedToLong(doc.get("value")) );
      if (i>0) {
        assertEquals("Distinct term number is equal for all query types", lastTerms, terms);
      }
      lastTerms = terms;
    }
  }

  public void testRange_8bit() throws Exception {
    testRange(8);
  }
  
  public void testRange_4bit() throws Exception {
    testRange(4);
  }
  
  public void testRange_2bit() throws Exception {
    testRange(2);
  }
  
  public void testInverseRange() throws Exception {
    LongTrieRangeFilter f = new LongTrieRangeFilter("field8", 8, new Long(1000L), new Long(-1000L), true, true);
    assertSame("A inverse range should return the EMPTY_DOCIDSET instance", DocIdSet.EMPTY_DOCIDSET, f.getDocIdSet(searcher.getIndexReader()));
  }
  
  private void testLeftOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long upper=(count-1)*distance + (distance/3) + startOffset;
    LongTrieRangeQuery q=new LongTrieRangeQuery(field, precisionStep, null, new Long(upper), true, true);
    TopDocs topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    System.out.println("Found "+q.getTotalNumberOfTerms()+" distinct terms in left open range for field '"+field+"'.");
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", startOffset, TrieUtils.prefixCodedToLong(doc.get("value")) );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (count-1)*distance+startOffset, TrieUtils.prefixCodedToLong(doc.get("value")) );
  }
  
  public void testLeftOpenRange_8bit() throws Exception {
    testLeftOpenRange(8);
  }
  
  public void testLeftOpenRange_4bit() throws Exception {
    testLeftOpenRange(4);
  }
  
  public void testLeftOpenRange_2bit() throws Exception {
    testLeftOpenRange(2);
  }
  
  private void testRightOpenRange(int precisionStep) throws Exception {
    String field="field"+precisionStep;
    int count=3000;
    long lower=(count-1)*distance + (distance/3) +startOffset;
    LongTrieRangeQuery q=new LongTrieRangeQuery(field, precisionStep, new Long(lower), null, true, true);
    TopDocs topDocs = searcher.search(q, null, noDocs, Sort.INDEXORDER);
    System.out.println("Found "+q.getTotalNumberOfTerms()+" distinct terms in right open range for field '"+field+"'.");
    ScoreDoc[] sd = topDocs.scoreDocs;
    assertNotNull(sd);
    assertEquals("Score doc count", noDocs-count, sd.length );
    Document doc=searcher.doc(sd[0].doc);
    assertEquals("First doc", count*distance+startOffset, TrieUtils.prefixCodedToLong(doc.get("value")) );
    doc=searcher.doc(sd[sd.length-1].doc);
    assertEquals("Last doc", (noDocs-1)*distance+startOffset, TrieUtils.prefixCodedToLong(doc.get("value")) );
  }
  
  public void testRightOpenRange_8bit() throws Exception {
    testRightOpenRange(8);
  }
  
  public void testRightOpenRange_4bit() throws Exception {
    testRightOpenRange(4);
  }
  
  public void testRightOpenRange_2bit() throws Exception {
    testRightOpenRange(2);
  }
  
  private void testRandomTrieAndClassicRangeQuery(int precisionStep) throws Exception {
    final Random rnd=newRandom();
    String field="field"+precisionStep;
    int termCountT=0,termCountC=0;
    for (int i=0; i<50; i++) {
      long lower=(long)(rnd.nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(rnd.nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      LongTrieRangeQuery tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), true, true);
      RangeQuery cq=new RangeQuery(field, TrieUtils.longToPrefixCoded(lower), TrieUtils.longToPrefixCoded(upper), true, true);
      cq.setConstantScoreRewrite(true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      TopDocs cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for LongTrieRangeQuery and RangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      termCountT += tq.getTotalNumberOfTerms();
      termCountC += cq.getTotalNumberOfTerms();
      // test exclusive range
      tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), false, false);
      cq=new RangeQuery(field, TrieUtils.longToPrefixCoded(lower), TrieUtils.longToPrefixCoded(upper), false, false);
      cq.setConstantScoreRewrite(true);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for LongTrieRangeQuery and RangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      termCountT += tq.getTotalNumberOfTerms();
      termCountC += cq.getTotalNumberOfTerms();
      // test left exclusive range
      tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), false, true);
      cq=new RangeQuery(field, TrieUtils.longToPrefixCoded(lower), TrieUtils.longToPrefixCoded(upper), false, true);
      cq.setConstantScoreRewrite(true);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for LongTrieRangeQuery and RangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      termCountT += tq.getTotalNumberOfTerms();
      termCountC += cq.getTotalNumberOfTerms();
      // test right exclusive range
      tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), true, false);
      cq=new RangeQuery(field, TrieUtils.longToPrefixCoded(lower), TrieUtils.longToPrefixCoded(upper), true, false);
      cq.setConstantScoreRewrite(true);
      tTopDocs = searcher.search(tq, 1);
      cTopDocs = searcher.search(cq, 1);
      assertEquals("Returned count for LongTrieRangeQuery and RangeQuery must be equal", cTopDocs.totalHits, tTopDocs.totalHits );
      termCountT += tq.getTotalNumberOfTerms();
      termCountC += cq.getTotalNumberOfTerms();
    }
    System.out.println("Average number of terms during random search on '" + field + "':");
    System.out.println(" Trie query: " + (((double)termCountT)/(50*4)));
    System.out.println(" Classical query: " + (((double)termCountC)/(50*4)));
  }
  
  public void testRandomTrieAndClassicRangeQuery_8bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(8);
  }
  
  public void testRandomTrieAndClassicRangeQuery_4bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(4);
  }
  
  public void testRandomTrieAndClassicRangeQuery_2bit() throws Exception {
    testRandomTrieAndClassicRangeQuery(2);
  }
  
  private void testRangeSplit(int precisionStep) throws Exception {
    final Random rnd=newRandom();
    String field="ascfield"+precisionStep;
    // 50 random tests
    for (int i=0; i<50; i++) {
      long lower=(long)(rnd.nextDouble()*noDocs - noDocs/2);
      long upper=(long)(rnd.nextDouble()*noDocs - noDocs/2);
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      // test inclusive range
      Query tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), true, true);
      TopDocs tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to inclusive range length", upper-lower+1, tTopDocs.totalHits );
      // test exclusive range
      tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), false, false);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to exclusive range length", Math.max(upper-lower-1, 0), tTopDocs.totalHits );
      // test left exclusive range
      tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), false, true);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
      // test right exclusive range
      tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), true, false);
      tTopDocs = searcher.search(tq, 1);
      assertEquals("Returned count of range query must be equal to half exclusive range length", upper-lower, tTopDocs.totalHits );
    }
  }

  public void testRangeSplit_8bit() throws Exception {
    testRangeSplit(8);
  }
  
  public void testRangeSplit_4bit() throws Exception {
    testRangeSplit(4);
  }
  
  public void testRangeSplit_2bit() throws Exception {
    testRangeSplit(2);
  }
  
  private void testSorting(int precisionStep) throws Exception {
    final Random rnd=newRandom();
    String field="field"+precisionStep;
    // 10 random tests, the index order is ascending,
    // so using a reverse sort field should retun descending documents
    for (int i=0; i<10; i++) {
      long lower=(long)(rnd.nextDouble()*noDocs*distance)+startOffset;
      long upper=(long)(rnd.nextDouble()*noDocs*distance)+startOffset;
      if (lower>upper) {
        long a=lower; lower=upper; upper=a;
      }
      Query tq=new LongTrieRangeQuery(field, precisionStep, new Long(lower), new Long(upper), true, true);
      TopDocs topDocs = searcher.search(tq, null, noDocs, new Sort(TrieUtils.getLongSortField(field, true)));
      if (topDocs.totalHits==0) continue;
      ScoreDoc[] sd = topDocs.scoreDocs;
      assertNotNull(sd);
      long last=TrieUtils.prefixCodedToLong(searcher.doc(sd[0].doc).get("value"));
      for (int j=1; j<sd.length; j++) {
        long act=TrieUtils.prefixCodedToLong(searcher.doc(sd[j].doc).get("value"));
        assertTrue("Docs should be sorted backwards", last>act );
        last=act;
      }
    }
  }

  public void testSorting_8bit() throws Exception {
    testSorting(8);
  }
  
  public void testSorting_4bit() throws Exception {
    testSorting(4);
  }
  
  public void testSorting_2bit() throws Exception {
    testSorting(2);
  }
  
  public void testEqualsAndHash() throws Exception {
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test1", 4, new Long(10L), new Long(20L), true, true));
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test2", 4, new Long(10L), new Long(20L), false, true));
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test3", 4, new Long(10L), new Long(20L), true, false));
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test4", 4, new Long(10L), new Long(20L), false, false));
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test5", 4, new Long(10L), null, true, true));
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test6", 4, null, new Long(20L), true, true));
    QueryUtils.checkHashEquals(new LongTrieRangeQuery("test7", 4, null, null, true, true));
    QueryUtils.checkEqual(
      new LongTrieRangeQuery("test8", 4, new Long(10L), new Long(20L), true, true), 
      new LongTrieRangeQuery("test8", 4, new Long(10L), new Long(20L), true, true)
     );
    QueryUtils.checkUnequal(
      new LongTrieRangeQuery("test9", 4, new Long(10L), new Long(20L), true, true), 
      new LongTrieRangeQuery("test9", 8, new Long(10L), new Long(20L), true, true)
     );
    QueryUtils.checkUnequal(
      new LongTrieRangeQuery("test10a", 4, new Long(10L), new Long(20L), true, true), 
      new LongTrieRangeQuery("test10b", 4, new Long(10L), new Long(20L), true, true)
     );
    QueryUtils.checkUnequal(
      new LongTrieRangeQuery("test11", 4, new Long(10L), new Long(20L), true, true), 
      new LongTrieRangeQuery("test11", 4, new Long(20L), new Long(10L), true, true)
     );
    QueryUtils.checkUnequal(
      new LongTrieRangeQuery("test12", 4, new Long(10L), new Long(20L), true, true), 
      new LongTrieRangeQuery("test12", 4, new Long(10L), new Long(20L), false, true)
     );
  }
  
}
