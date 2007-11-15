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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.Similarity;

public class TestSegmentReader extends LuceneTestCase {
  private RAMDirectory dir = new RAMDirectory();
  private Document testDoc = new Document();
  private SegmentReader reader = null;

  public TestSegmentReader(String s) {
    super(s);
  }
  
  //TODO: Setup the reader w/ multiple documents
  protected void setUp() throws Exception {
    super.setUp();
    DocHelper.setupDoc(testDoc);
    SegmentInfo info = DocHelper.writeDoc(dir, testDoc);
    reader = SegmentReader.get(info);
  }

  public void test() {
    assertTrue(dir != null);
    assertTrue(reader != null);
    assertTrue(DocHelper.nameValues.size() > 0);
    assertTrue(DocHelper.numFields(testDoc) == DocHelper.all.size());
  }
  
  public void testDocument() throws IOException {
    assertTrue(reader.numDocs() == 1);
    assertTrue(reader.maxDoc() >= 1);
    Document result = reader.document(0);
    assertTrue(result != null);
    //There are 2 unstored fields on the document that are not preserved across writing
    assertTrue(DocHelper.numFields(result) == DocHelper.numFields(testDoc) - DocHelper.unstored.size());
    
    List fields = result.getFields();
    for (Iterator iter = fields.iterator(); iter.hasNext();) {
      Fieldable field = (Fieldable) iter.next();
      assertTrue(field != null);
      assertTrue(DocHelper.nameValues.containsKey(field.name()));
    }
  }
  
  public void testDelete() throws IOException {
    Document docToDelete = new Document();
    DocHelper.setupDoc(docToDelete);
    SegmentInfo info = DocHelper.writeDoc(dir, docToDelete);
    SegmentReader deleteReader = SegmentReader.get(info);
    assertTrue(deleteReader != null);
    assertTrue(deleteReader.numDocs() == 1);
    deleteReader.deleteDocument(0);
    assertTrue(deleteReader.isDeleted(0) == true);
    assertTrue(deleteReader.hasDeletions() == true);
    assertTrue(deleteReader.numDocs() == 0);
    try {
      deleteReader.document(0);
      fail();
    } catch (IllegalArgumentException e) {
      // expcected exception
    }
  }    
  
  public void testGetFieldNameVariations() {
    Collection result = reader.getFieldNames(IndexReader.FieldOption.ALL);
    assertTrue(result != null);
    assertTrue(result.size() == DocHelper.all.size());
    for (Iterator iter = result.iterator(); iter.hasNext();) {
      String s = (String) iter.next();
      //System.out.println("Name: " + s);
      assertTrue(DocHelper.nameValues.containsKey(s) == true || s.equals(""));
    }                                                                               
    result = reader.getFieldNames(IndexReader.FieldOption.INDEXED);
    assertTrue(result != null);
    assertTrue(result.size() == DocHelper.indexed.size());
    for (Iterator iter = result.iterator(); iter.hasNext();) {
      String s = (String) iter.next();
      assertTrue(DocHelper.indexed.containsKey(s) == true || s.equals(""));
    }
    
    result = reader.getFieldNames(IndexReader.FieldOption.UNINDEXED);
    assertTrue(result != null);
    assertTrue(result.size() == DocHelper.unindexed.size());
    //Get all indexed fields that are storing term vectors
    result = reader.getFieldNames(IndexReader.FieldOption.INDEXED_WITH_TERMVECTOR);
    assertTrue(result != null);
    assertTrue(result.size() == DocHelper.termvector.size());
    
    result = reader.getFieldNames(IndexReader.FieldOption.INDEXED_NO_TERMVECTOR);
    assertTrue(result != null);
    assertTrue(result.size() == DocHelper.notermvector.size());
  } 
  
  public void testTerms() throws IOException {
    TermEnum terms = reader.terms();
    assertTrue(terms != null);
    while (terms.next() == true)
    {
      Term term = terms.term();
      assertTrue(term != null);
      //System.out.println("Term: " + term);
      String fieldValue = (String)DocHelper.nameValues.get(term.field());
      assertTrue(fieldValue.indexOf(term.text()) != -1);
    }
    
    TermDocs termDocs = reader.termDocs();
    assertTrue(termDocs != null);
    termDocs.seek(new Term(DocHelper.TEXT_FIELD_1_KEY, "field"));
    assertTrue(termDocs.next() == true);

    termDocs.seek(new Term(DocHelper.NO_NORMS_KEY,  DocHelper.NO_NORMS_TEXT));
    assertTrue(termDocs.next() == true);

    
    TermPositions positions = reader.termPositions();
    positions.seek(new Term(DocHelper.TEXT_FIELD_1_KEY, "field"));
    assertTrue(positions != null);
    assertTrue(positions.doc() == 0);
    assertTrue(positions.nextPosition() >= 0);
  }    
  
  public void testNorms() throws IOException {
    //TODO: Not sure how these work/should be tested
/*
    try {
      byte [] norms = reader.norms(DocHelper.TEXT_FIELD_1_KEY);
      System.out.println("Norms: " + norms);
      assertTrue(norms != null);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
*/

    checkNorms(reader);
  }

  public static void checkNorms(IndexReader reader) throws IOException {
        // test omit norms
    for (int i=0; i<DocHelper.fields.length; i++) {
      Fieldable f = DocHelper.fields[i];
      if (f.isIndexed()) {
        assertEquals(reader.hasNorms(f.name()), !f.getOmitNorms());
        assertEquals(reader.hasNorms(f.name()), !DocHelper.noNorms.containsKey(f.name()));
        if (!reader.hasNorms(f.name())) {
          // test for fake norms of 1.0
          byte [] norms = reader.norms(f.name());
          assertEquals(norms.length,reader.maxDoc());
          for (int j=0; j<reader.maxDoc(); j++) {
            assertEquals(norms[j], DefaultSimilarity.encodeNorm(1.0f));
          }
          norms = new byte[reader.maxDoc()];
          reader.norms(f.name(),norms, 0);
          for (int j=0; j<reader.maxDoc(); j++) {
            assertEquals(norms[j], DefaultSimilarity.encodeNorm(1.0f));
          }
        }
      }
    }
  }
  
  public void testTermVectors() throws IOException {
    TermFreqVector result = reader.getTermFreqVector(0, DocHelper.TEXT_FIELD_2_KEY);
    assertTrue(result != null);
    String [] terms = result.getTerms();
    int [] freqs = result.getTermFrequencies();
    assertTrue(terms != null && terms.length == 3 && freqs != null && freqs.length == 3);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      int freq = freqs[i];
      assertTrue(DocHelper.FIELD_2_TEXT.indexOf(term) != -1);
      assertTrue(freq > 0);
    }

    TermFreqVector [] results = reader.getTermFreqVectors(0);
    assertTrue(results != null);
    assertTrue("We do not have 4 term freq vectors, we have: " + results.length, results.length == 4);      
  }    
  
  public void testIndexDivisor() throws IOException {
    dir = new MockRAMDirectory();
    testDoc = new Document();
    DocHelper.setupDoc(testDoc);
    SegmentInfo si = DocHelper.writeDoc(dir, testDoc);
    
    reader = SegmentReader.get(si);
    reader.setTermInfosIndexDivisor(3);
    testDocument();
    testDelete();
    testGetFieldNameVariations();
    testNorms();
    testTerms();
    testTermVectors();
  }
}
