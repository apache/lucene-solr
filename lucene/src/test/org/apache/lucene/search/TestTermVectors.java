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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

public class TestTermVectors extends LuceneTestCase {
  private IndexSearcher searcher;
  private IndexReader reader;
  private Directory directory;

  @Override
  public void setUp() throws Exception {                  
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.SIMPLE, true)).setMergePolicy(newLogMergePolicy()));
    //writer.setUseCompoundFile(true);
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      Field.TermVector termVector;
      int mod3 = i % 3;
      int mod2 = i % 2;
      if (mod2 == 0 && mod3 == 0){
        termVector = Field.TermVector.WITH_POSITIONS_OFFSETS;
      }
      else if (mod2 == 0){
        termVector = Field.TermVector.WITH_POSITIONS;
      }
      else if (mod3 == 0){
        termVector = Field.TermVector.WITH_OFFSETS;
      }
      else {
        termVector = Field.TermVector.YES;
      }
      doc.add(new Field("field", English.intToEnglish(i),
          Field.Store.YES, Field.Index.ANALYZED, termVector));
      //test no term vectors too
      doc.add(new Field("noTV", English.intToEnglish(i),
          Field.Store.YES, Field.Index.ANALYZED));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @Override
  public void tearDown() throws Exception {
    searcher.close();
    reader.close();
    directory.close();
    super.tearDown();
  }

  public void test() {
    assertTrue(searcher != null);
  }

  public void testTermVectors() throws IOException {
    Query query = new TermQuery(new Term("field", "seventy"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(100, hits.length);
      
    for (int i = 0; i < hits.length; i++)
    {
      TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits[i].doc);
      assertTrue(vector != null);
      assertTrue(vector.length == 1);
    }
    TermFreqVector vector;
    vector = searcher.reader.getTermFreqVector(hits[0].doc, "noTV");
    assertNull(vector);

    TestTermVectorMapper mapper = new TestTermVectorMapper();
    searcher.reader.getTermFreqVector(hits[0].doc, "noTV", mapper);
    assertNull(mapper.field);
  }
  
  public void testTermVectorsFieldOrder() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, new MockAnalyzer(random, MockTokenizer.SIMPLE, true));
    Document doc = new Document();
    doc.add(new Field("c", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("a", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("b", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("x", "some content here", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();
    TermFreqVector[] v = reader.getTermFreqVectors(0);
    assertEquals(4, v.length);
    String[] expectedFields = new String[]{"a", "b", "c", "x"};
    int[] expectedPositions = new int[]{1, 2, 0};
    for(int i=0;i<v.length;i++) {
      TermPositionVector posVec = (TermPositionVector) v[i];
      assertEquals(expectedFields[i], posVec.getField());
      BytesRef[] terms = posVec.getTerms();
      assertEquals(3, terms.length);
      assertEquals("content", terms[0].utf8ToString());
      assertEquals("here", terms[1].utf8ToString());
      assertEquals("some", terms[2].utf8ToString());
      for(int j=0;j<3;j++) {
        int[] positions = posVec.getTermPositions(j);
        assertEquals(1, positions.length);
        assertEquals(expectedPositions[j], positions[0]);
      }
    }
    reader.close();
    dir.close();
  }

  public void testTermPositionVectors() throws IOException {
    Query query = new TermQuery(new Term("field", "zero"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    
    for (int i = 0; i < hits.length; i++) {
      TermFreqVector[] vector = searcher.reader.getTermFreqVectors(hits[i].doc);
      assertTrue(vector != null);
      assertTrue(vector.length == 1);
      
      boolean shouldBePosVector = (hits[i].doc % 2 == 0) ? true : false;
      assertTrue((shouldBePosVector == false)
          || (shouldBePosVector == true && (vector[0] instanceof TermPositionVector == true)));
      
      boolean shouldBeOffVector = (hits[i].doc % 3 == 0) ? true : false;
      assertTrue((shouldBeOffVector == false)
          || (shouldBeOffVector == true && (vector[0] instanceof TermPositionVector == true)));
      
      if (shouldBePosVector || shouldBeOffVector) {
        TermPositionVector posVec = (TermPositionVector) vector[0];
        BytesRef[] terms = posVec.getTerms();
        assertTrue(terms != null && terms.length > 0);
        
        for (int j = 0; j < terms.length; j++) {
          int[] positions = posVec.getTermPositions(j);
          TermVectorOffsetInfo[] offsets = posVec.getOffsets(j);
          
          if (shouldBePosVector) {
            assertTrue(positions != null);
            assertTrue(positions.length > 0);
          } else assertTrue(positions == null);
          
          if (shouldBeOffVector) {
            assertTrue(offsets != null);
            assertTrue(offsets.length > 0);
          } else assertTrue(offsets == null);
        }
      } else {
        try {
          assertTrue(false);
        } catch (ClassCastException ignore) {
          TermFreqVector freqVec = vector[0];
          BytesRef[] terms = freqVec.getTerms();
          assertTrue(terms != null && terms.length > 0);
        }
        
      }
    }
  }
  
  public void testTermOffsetVectors() {
    Query query = new TermQuery(new Term("field", "fifty"));
    try {
      ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
      assertEquals(100, hits.length);
      
      for (int i = 0; i < hits.length; i++)
      {
        TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits[i].doc);
        assertTrue(vector != null);
        assertTrue(vector.length == 1);
        
        //assertTrue();
      }
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  public void testKnownSetOfDocuments() throws IOException {
    String test1 = "eating chocolate in a computer lab"; //6 terms
    String test2 = "computer in a computer lab"; //5 terms
    String test3 = "a chocolate lab grows old"; //5 terms
    String test4 = "eating chocolate with a chocolate lab in an old chocolate colored computer lab"; //13 terms
    Map<String,Integer> test4Map = new HashMap<String,Integer>();
    test4Map.put("chocolate", Integer.valueOf(3));
    test4Map.put("lab", Integer.valueOf(2));
    test4Map.put("eating", Integer.valueOf(1));
    test4Map.put("computer", Integer.valueOf(1));
    test4Map.put("with", Integer.valueOf(1));
    test4Map.put("a", Integer.valueOf(1));
    test4Map.put("colored", Integer.valueOf(1));
    test4Map.put("in", Integer.valueOf(1));
    test4Map.put("an", Integer.valueOf(1));
    test4Map.put("computer", Integer.valueOf(1));
    test4Map.put("old", Integer.valueOf(1));
    
    Document testDoc1 = new Document();
    setupDoc(testDoc1, test1);
    Document testDoc2 = new Document();
    setupDoc(testDoc2, test2);
    Document testDoc3 = new Document();
    setupDoc(testDoc3, test3);
    Document testDoc4 = new Document();
    setupDoc(testDoc4, test4);
    
    Directory dir = newDirectory();
    
    RandomIndexWriter writer = new RandomIndexWriter(random, dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.SIMPLE, true))
                                                     .setOpenMode(OpenMode.CREATE).setMergePolicy(newLogMergePolicy()));
    writer.addDocument(testDoc1);
    writer.addDocument(testDoc2);
    writer.addDocument(testDoc3);
    writer.addDocument(testDoc4);
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher knownSearcher = newSearcher(reader);
    FieldsEnum fields = MultiFields.getFields(knownSearcher.reader).iterator();
    
    DocsEnum docs = null;
    while(fields.next() != null) {
      TermsEnum terms = fields.terms();
      while(terms.next() != null) {
        String text = terms.term().utf8ToString();
        docs = terms.docs(MultiFields.getDeletedDocs(knownSearcher.reader), docs);
        
        while (docs.nextDoc() != DocsEnum.NO_MORE_DOCS) {
          int docId = docs.docID();
          int freq = docs.freq();
          //System.out.println("Doc Id: " + docId + " freq " + freq);
          TermFreqVector vector = knownSearcher.reader.getTermFreqVector(docId, "field");
          //float tf = sim.tf(freq);
          //float idf = sim.idf(knownSearcher.docFreq(term), knownSearcher.maxDoc());
          //float qNorm = sim.queryNorm()
          //This is fine since we don't have stop words
          //float lNorm = sim.lengthNorm("field", vector.getTerms().length);
          //float coord = sim.coord()
          //System.out.println("TF: " + tf + " IDF: " + idf + " LenNorm: " + lNorm);
          assertTrue(vector != null);
          BytesRef[] vTerms = vector.getTerms();
          int [] freqs = vector.getTermFrequencies();
          for (int i = 0; i < vTerms.length; i++)
          {
            if (text.equals(vTerms[i].utf8ToString()))
            {
              assertTrue(freqs[i] == freq);
            }
          }
        }
      }
      //System.out.println("--------");
    }
    Query query = new TermQuery(new Term("field", "chocolate"));
    ScoreDoc[] hits = knownSearcher.search(query, null, 1000).scoreDocs;
    //doc 3 should be the first hit b/c it is the shortest match
    assertTrue(hits.length == 3);
    /*System.out.println("Hit 0: " + hits.id(0) + " Score: " + hits.score(0) + " String: " + hits.doc(0).toString());
      System.out.println("Explain: " + knownSearcher.explain(query, hits.id(0)));
      System.out.println("Hit 1: " + hits.id(1) + " Score: " + hits.score(1) + " String: " + hits.doc(1).toString());
      System.out.println("Explain: " + knownSearcher.explain(query, hits.id(1)));
      System.out.println("Hit 2: " + hits.id(2) + " Score: " + hits.score(2) + " String: " +  hits.doc(2).toString());
      System.out.println("Explain: " + knownSearcher.explain(query, hits.id(2)));*/
    assertTrue(hits[0].doc == 2);
    assertTrue(hits[1].doc == 3);
    assertTrue(hits[2].doc == 0);
    TermFreqVector vector = knownSearcher.reader.getTermFreqVector(hits[1].doc, "field");
    assertTrue(vector != null);
    //System.out.println("Vector: " + vector);
    BytesRef[] terms = vector.getTerms();
    int [] freqs = vector.getTermFrequencies();
    assertTrue(terms != null && terms.length == 10);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i].utf8ToString();
      //System.out.println("Term: " + term);
      int freq = freqs[i];
      assertTrue(test4.indexOf(term) != -1);
      Integer freqInt = test4Map.get(term);
      assertTrue(freqInt != null);
      assertTrue(freqInt.intValue() == freq);        
    }
    SortedTermVectorMapper mapper = new SortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
    knownSearcher.reader.getTermFreqVector(hits[1].doc, mapper);
    SortedSet<TermVectorEntry> vectorEntrySet = mapper.getTermVectorEntrySet();
    assertTrue("mapper.getTermVectorEntrySet() Size: " + vectorEntrySet.size() + " is not: " + 10, vectorEntrySet.size() == 10);
    TermVectorEntry last = null;
    for (final TermVectorEntry tve : vectorEntrySet) {
      if (tve != null && last != null)
      {
        assertTrue("terms are not properly sorted", last.getFrequency() >= tve.getFrequency());
        Integer expectedFreq =  test4Map.get(tve.getTerm().utf8ToString());
        //we expect double the expectedFreq, since there are two fields with the exact same text and we are collapsing all fields
        assertTrue("Frequency is not correct:", tve.getFrequency() == 2*expectedFreq.intValue());
      }
      last = tve;
      
    }
    
    FieldSortedTermVectorMapper fieldMapper = new FieldSortedTermVectorMapper(new TermVectorEntryFreqSortedComparator());
    knownSearcher.reader.getTermFreqVector(hits[1].doc, fieldMapper);
    Map<String,SortedSet<TermVectorEntry>> map = fieldMapper.getFieldToTerms();
    assertTrue("map Size: " + map.size() + " is not: " + 2, map.size() == 2);
    vectorEntrySet = map.get("field");
    assertTrue("vectorEntrySet is null and it shouldn't be", vectorEntrySet != null);
    assertTrue("vectorEntrySet Size: " + vectorEntrySet.size() + " is not: " + 10, vectorEntrySet.size() == 10);
    knownSearcher.close();
    reader.close();
    dir.close();
  } 
  
  private void setupDoc(Document doc, String text)
  {
    doc.add(new Field("field2", text, Field.Store.YES,
        Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("field", text, Field.Store.YES,
        Field.Index.ANALYZED, Field.TermVector.YES));
    //System.out.println("Document: " + doc);
  }

  // Test only a few docs having vectors
  public void testRareVectors() throws IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random, directory, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random, MockTokenizer.SIMPLE, true))
        .setOpenMode(OpenMode.CREATE));
    writer.w.setInfoStream(VERBOSE ? System.out : null);
    if (VERBOSE) {
      System.out.println("TEST: now add non-vectors");
    }
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new Field("field", English.intToEnglish(i),
                        Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
      writer.addDocument(doc);
    }
    if (VERBOSE) {
      System.out.println("TEST: now add vectors");
    }
    for(int i=0;i<10;i++) {
      Document doc = new Document();
      doc.add(new Field("field", English.intToEnglish(100+i),
                        Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
      writer.addDocument(doc);
    }

    if (VERBOSE) {
      System.out.println("TEST: now getReader");
    }
    IndexReader reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);

    Query query = new TermQuery(new Term("field", "hundred"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(10, hits.length);
    for (int i = 0; i < hits.length; i++) {

      TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits[i].doc);
      assertTrue(vector != null);
      assertTrue(vector.length == 1);
    }
    reader.close();
  }


  // In a single doc, for the same field, mix the term
  // vectors up
  public void testMixedVectrosVectors() throws IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random, directory, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random, MockTokenizer.SIMPLE, true)).setOpenMode(OpenMode.CREATE));
    Document doc = new Document();
    doc.add(new Field("field", "one",
                      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.NO));
    doc.add(new Field("field", "one",
                      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.YES));
    doc.add(new Field("field", "one",
                      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS));
    doc.add(new Field("field", "one",
                      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_OFFSETS));
    doc.add(new Field("field", "one",
                      Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    searcher = newSearcher(reader);

    Query query = new TermQuery(new Term("field", "one"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits[0].doc);
    assertTrue(vector != null);
    assertTrue(vector.length == 1);
    TermPositionVector tfv = (TermPositionVector) vector[0];
    assertTrue(tfv.getField().equals("field"));
    BytesRef[] terms = tfv.getTerms();
    assertEquals(1, terms.length);
    assertEquals(terms[0].utf8ToString(), "one");
    assertEquals(5, tfv.getTermFrequencies()[0]);

    int[] positions = tfv.getTermPositions(0);
    assertEquals(5, positions.length);
    for(int i=0;i<5;i++)
      assertEquals(i, positions[i]);
    TermVectorOffsetInfo[] offsets = tfv.getOffsets(0);
    assertEquals(5, offsets.length);
    for(int i=0;i<5;i++) {
      assertEquals(4*i, offsets[i].getStartOffset());
      assertEquals(4*i+3, offsets[i].getEndOffset());
    }
    reader.close();
  }

  private static class TestTermVectorMapper extends TermVectorMapper {
    public String field = null;

    @Override
    public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
      this.field = field;
    }

    @Override
    public void map(BytesRef term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {

    }
  }
}
