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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.English;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestTermVectors extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;

  @BeforeClass
  public static void beforeClass() throws Exception {                  
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true)).setMergePolicy(newLogMergePolicy()));
    //writer.setUseCompoundFile(true);
    //writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      FieldType ft = new FieldType(TextField.TYPE_STORED);
      int mod3 = i % 3;
      int mod2 = i % 2;
      if (mod2 == 0 && mod3 == 0) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorOffsets(true);
        ft.setStoreTermVectorPositions(true);
      } else if (mod2 == 0) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorPositions(true);
      } else if (mod3 == 0) {
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorOffsets(true);
      } else {
        ft.setStoreTermVectors(true);
      }
      doc.add(new Field("field", English.intToEnglish(i), ft));
      //test no term vectors too
      doc.add(new Field("noTV", English.intToEnglish(i), TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    reader = null;
    directory = null;
    searcher = null;
  }

  public void test() {
    assertTrue(searcher != null);
  }

  public void testTermVectors() throws IOException {
    Query query = new TermQuery(new Term("field", "seventy"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(100, hits.length);
      
    for (int i = 0; i < hits.length; i++) {
      Fields vectors = searcher.reader.getTermVectors(hits[i].doc);
      assertNotNull(vectors);
      assertEquals("doc=" + hits[i].doc + " tv=" + vectors, 1, vectors.size());
    }
    Terms vector;
    vector = searcher.reader.getTermVectors(hits[0].doc).terms("noTV");
    assertNull(vector);
  }
  
  public void testTermVectorsFieldOrder() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true));
    Document doc = new Document();
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    doc.add(newField("c", "some content here", ft));
    doc.add(newField("a", "some content here", ft));
    doc.add(newField("b", "some content here", ft));
    doc.add(newField("x", "some content here", ft));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();
    Fields v = reader.getTermVectors(0);
    assertEquals(4, v.size());
    String[] expectedFields = new String[]{"a", "b", "c", "x"};
    int[] expectedPositions = new int[]{1, 2, 0};
    FieldsEnum fieldsEnum = v.iterator();
    for(int i=0;i<expectedFields.length;i++) {
      assertEquals(expectedFields[i], fieldsEnum.next());
      assertEquals(3, v.terms(expectedFields[i]).size());

      DocsAndPositionsEnum dpEnum = null;
      Terms terms = fieldsEnum.terms();
      assertNotNull(terms);
      TermsEnum termsEnum = terms.iterator(null);
      assertEquals("content", termsEnum.next().utf8ToString());
      dpEnum = termsEnum.docsAndPositions(null, dpEnum, false);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(1, dpEnum.freq());
      assertEquals(expectedPositions[0], dpEnum.nextPosition());

      assertEquals("here", termsEnum.next().utf8ToString());
      dpEnum = termsEnum.docsAndPositions(null, dpEnum, false);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(1, dpEnum.freq());
      assertEquals(expectedPositions[1], dpEnum.nextPosition());

      assertEquals("some", termsEnum.next().utf8ToString());
      dpEnum = termsEnum.docsAndPositions(null, dpEnum, false);
      assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
      assertEquals(1, dpEnum.freq());
      assertEquals(expectedPositions[2], dpEnum.nextPosition());

      assertNull(termsEnum.next());
    }
    reader.close();
    dir.close();
  }

  public void testTermPositionVectors() throws IOException {
    Query query = new TermQuery(new Term("field", "zero"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    DocsAndPositionsEnum dpEnum = null;
    for (int i = 0; i < hits.length; i++) {
      Fields vectors = searcher.reader.getTermVectors(hits[i].doc);
      assertNotNull(vectors);
      assertEquals(1, vectors.size());
      
      TermsEnum termsEnum = vectors.terms("field").iterator(null);
      assertNotNull(termsEnum.next());

      boolean shouldBePosVector = hits[i].doc % 2 == 0;
      boolean shouldBeOffVector = hits[i].doc % 3 == 0;
      
      if (shouldBePosVector || shouldBeOffVector) {
        while(true) {
          dpEnum = termsEnum.docsAndPositions(null, dpEnum, shouldBeOffVector);
          assertNotNull(dpEnum);
          assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

          dpEnum.nextPosition();

          if (shouldBeOffVector) {
            assertTrue(dpEnum.startOffset() != -1);
            assertTrue(dpEnum.endOffset() != -1);
          }

          if (termsEnum.next() == null) {
            break;
          }
        }
      } else {
        fail();
      }
    }
  }
  
  public void testTermOffsetVectors() throws IOException {
    Query query = new TermQuery(new Term("field", "fifty"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(100, hits.length);
      
    for (int i = 0; i < hits.length; i++) {
      Fields vectors = searcher.reader.getTermVectors(hits[i].doc);
      assertNotNull(vectors);
      assertEquals(1, vectors.size());
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
    
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
          .setOpenMode(OpenMode.CREATE)
          .setMergePolicy(newLogMergePolicy())
          .setSimilarity(new DefaultSimilarity()));
    writer.addDocument(testDoc1);
    writer.addDocument(testDoc2);
    writer.addDocument(testDoc3);
    writer.addDocument(testDoc4);
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher knownSearcher = newSearcher(reader);
    knownSearcher.setSimilarity(new DefaultSimilarity());
    FieldsEnum fields = MultiFields.getFields(knownSearcher.reader).iterator();
    
    DocsEnum docs = null;
    while(fields.next() != null) {
      Terms terms = fields.terms();
      assertNotNull(terms); // NOTE: kinda sketchy assumptions, but ideally we would fix fieldsenum api... 
      TermsEnum termsEnum = terms.iterator(null);

      while (termsEnum.next() != null) {
        String text = termsEnum.term().utf8ToString();
        docs = _TestUtil.docs(random(), termsEnum, MultiFields.getLiveDocs(knownSearcher.reader), docs, true);
        
        while (docs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          int docId = docs.docID();
          int freq = docs.freq();
          //System.out.println("Doc Id: " + docId + " freq " + freq);
          Terms vector = knownSearcher.reader.getTermVectors(docId).terms("field");
          //float tf = sim.tf(freq);
          //float idf = sim.idf(knownSearcher.docFreq(term), knownSearcher.maxDoc());
          //float qNorm = sim.queryNorm()
          //This is fine since we don't have stop words
          //float lNorm = sim.lengthNorm("field", vector.getTerms().length);
          //float coord = sim.coord()
          //System.out.println("TF: " + tf + " IDF: " + idf + " LenNorm: " + lNorm);
          assertNotNull(vector);
          TermsEnum termsEnum2 = vector.iterator(null);

          while(termsEnum2.next() != null) {
            if (text.equals(termsEnum2.term().utf8ToString())) {
              assertEquals(freq, termsEnum2.totalTermFreq());
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
    Terms vector = knownSearcher.reader.getTermVectors(hits[1].doc).terms("field");
    assertNotNull(vector);
    //System.out.println("Vector: " + vector);
    assertEquals(10, vector.size());
    TermsEnum termsEnum = vector.iterator(null);
    while(termsEnum.next() != null) {
      String term = termsEnum.term().utf8ToString();
      //System.out.println("Term: " + term);
      int freq = (int) termsEnum.totalTermFreq();
      assertTrue(test4.indexOf(term) != -1);
      Integer freqInt = test4Map.get(term);
      assertTrue(freqInt != null);
      assertEquals(freqInt.intValue(), freq);
    }
    reader.close();
    dir.close();
  } 
  
  private void setupDoc(Document doc, String text)
  {
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setStoreTermVectors(true);
    doc.add(newField("field2", text, ft));
    doc.add(newField("field", text, ft2));
    //System.out.println("Document: " + doc);
  }

  // Test only a few docs having vectors
  public void testRareVectors() throws IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
        .setOpenMode(OpenMode.CREATE));
    if (VERBOSE) {
      System.out.println("TEST: now add non-vectors");
    }
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(newField("field", English.intToEnglish(i), TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    if (VERBOSE) {
      System.out.println("TEST: now add vectors");
    }
    FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    for(int i=0;i<10;i++) {
      Document doc = new Document();
      doc.add(newField("field", English.intToEnglish(100+i), ft));
      writer.addDocument(doc);
    }

    if (VERBOSE) {
      System.out.println("TEST: now getReader");
    }
    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = newSearcher(reader);

    Query query = new TermQuery(new Term("field", "hundred"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(10, hits.length);
    for (int i = 0; i < hits.length; i++) {

      Fields vectors = searcher.reader.getTermVectors(hits[i].doc);
      assertNotNull(vectors);
      assertEquals(1, vectors.size());
    }
    reader.close();
  }


  // In a single doc, for the same field, mix the term
  // vectors up
  public void testMixedVectrosVectors() throws IOException {
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, 
        newIndexWriterConfig(TEST_VERSION_CURRENT, 
        new MockAnalyzer(random(), MockTokenizer.SIMPLE, true)).setOpenMode(OpenMode.CREATE));
    Document doc = new Document();
    
    FieldType ft2 = new FieldType(TextField.TYPE_STORED);
    ft2.setStoreTermVectors(true);
    
    FieldType ft3 = new FieldType(TextField.TYPE_STORED);
    ft3.setStoreTermVectors(true);
    ft3.setStoreTermVectorPositions(true);
    
    FieldType ft4 = new FieldType(TextField.TYPE_STORED);
    ft4.setStoreTermVectors(true);
    ft4.setStoreTermVectorOffsets(true);
    
    FieldType ft5 = new FieldType(TextField.TYPE_STORED);
    ft5.setStoreTermVectors(true);
    ft5.setStoreTermVectorOffsets(true);
    ft5.setStoreTermVectorPositions(true);
    
    doc.add(newField("field", "one", TextField.TYPE_STORED));
    doc.add(newField("field", "one", ft2));
    doc.add(newField("field", "one", ft3));
    doc.add(newField("field", "one", ft4));
    doc.add(newField("field", "one", ft5));
    writer.addDocument(doc);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);

    Query query = new TermQuery(new Term("field", "one"));
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    Fields vectors = searcher.reader.getTermVectors(hits[0].doc);
    assertNotNull(vectors);
    assertEquals(1, vectors.size());
    Terms vector = vectors.terms("field");
    assertNotNull(vector);
    assertEquals(1, vector.size());
    TermsEnum termsEnum = vector.iterator(null);
    assertNotNull(termsEnum.next());
    assertEquals("one", termsEnum.term().utf8ToString());
    assertEquals(5, termsEnum.totalTermFreq());
    DocsAndPositionsEnum dpEnum = termsEnum.docsAndPositions(null, null, false);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(5, dpEnum.freq());
    for(int i=0;i<5;i++) {
      assertEquals(i, dpEnum.nextPosition());
    }

    dpEnum = termsEnum.docsAndPositions(null, dpEnum, true);
    assertNotNull(dpEnum);
    assertTrue(dpEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    assertEquals(5, dpEnum.freq());
    for(int i=0;i<5;i++) {
      dpEnum.nextPosition();
      assertEquals(4*i, dpEnum.startOffset());
      assertEquals(4*i+3, dpEnum.endOffset());
    }
    reader.close();
  }

  private IndexWriter createWriter(Directory dir) throws IOException {
    return new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT,
        new MockAnalyzer(random())).setMaxBufferedDocs(2));
  }

  private void createDir(Directory dir) throws IOException {
    IndexWriter writer = createWriter(dir);
    writer.addDocument(createDoc());
    writer.close();
  }

  private Document createDoc() {
    Document doc = new Document();
    final FieldType ft = new FieldType(TextField.TYPE_STORED);
    ft.setStoreTermVectors(true);
    ft.setStoreTermVectorOffsets(true);
    ft.setStoreTermVectorPositions(true);
    doc.add(newField("c", "aaa", ft));
    return doc;
  }

  private void verifyIndex(Directory dir) throws IOException {
    IndexReader r = IndexReader.open(dir);
    int numDocs = r.numDocs();
    for (int i = 0; i < numDocs; i++) {
      assertNotNull("term vectors should not have been null for document " + i, r.getTermVectors(i).terms("c"));
    }
    r.close();
  }
  
  public void testFullMergeAddDocs() throws Exception {
    Directory target = newDirectory();
    IndexWriter writer = createWriter(target);
    // with maxBufferedDocs=2, this results in two segments, so that forceMerge
    // actually does something.
    for (int i = 0; i < 4; i++) {
      writer.addDocument(createDoc());
    }
    writer.forceMerge(1);
    writer.close();
    
    verifyIndex(target);
    target.close();
  }

  public void testFullMergeAddIndexesDir() throws Exception {
    Directory[] input = new Directory[] { newDirectory(), newDirectory() };
    Directory target = newDirectory();
    
    for (Directory dir : input) {
      createDir(dir);
    }
    
    IndexWriter writer = createWriter(target);
    writer.addIndexes(input);
    writer.forceMerge(1);
    writer.close();

    verifyIndex(target);

    IOUtils.close(target, input[0], input[1]);
  }
  
  public void testFullMergeAddIndexesReader() throws Exception {
    Directory[] input = new Directory[] { newDirectory(), newDirectory() };
    Directory target = newDirectory();
    
    for (Directory dir : input) {
      createDir(dir);
    }
    
    IndexWriter writer = createWriter(target);
    for (Directory dir : input) {
      IndexReader r = IndexReader.open(dir);
      writer.addIndexes(r);
      r.close();
    }
    writer.forceMerge(1);
    writer.close();
    
    verifyIndex(target);
    IOUtils.close(target, input[0], input[1]);
  }

}
