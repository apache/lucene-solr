package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Enumeration;

public class TestSegmentReader extends TestCase {
  private RAMDirectory dir = new RAMDirectory();
  private Document testDoc = new Document();
  private SegmentReader reader = null;

  public TestSegmentReader(String s) {
    super(s);
  }
  
  //TODO: Setup the reader w/ multiple documents
  protected void setUp() {

    try {
      DocHelper.setupDoc(testDoc);
      DocHelper.writeDoc(dir, testDoc);
      reader = SegmentReader.get(new SegmentInfo("test", 1, dir));
    } catch (IOException e) {
      
    }
  }

  protected void tearDown() {

  }

  public void test() {
    assertTrue(dir != null);
    assertTrue(reader != null);
    assertTrue(DocHelper.nameValues.size() > 0);
    assertTrue(DocHelper.numFields(testDoc) == 6);
  }
  
  public void testDocument() {
    try {
      assertTrue(reader.numDocs() == 1);
      assertTrue(reader.maxDoc() >= 1);
      Document result = reader.document(0);
      assertTrue(result != null);
      //There are 2 unstored fields on the document that are not preserved across writing
      assertTrue(DocHelper.numFields(result) == DocHelper.numFields(testDoc) - 2);
      
      Enumeration fields = result.fields();
      while (fields.hasMoreElements()) {
        Field field = (Field) fields.nextElement();
        assertTrue(field != null);
        assertTrue(DocHelper.nameValues.containsKey(field.name()));
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  public void testDelete() {
    Document docToDelete = new Document();
    DocHelper.setupDoc(docToDelete);
    DocHelper.writeDoc(dir, "seg-to-delete", docToDelete);
    try {
      SegmentReader deleteReader = SegmentReader.get(new SegmentInfo("seg-to-delete", 1, dir));
      assertTrue(deleteReader != null);
      assertTrue(deleteReader.numDocs() == 1);
      deleteReader.delete(0);
      assertTrue(deleteReader.isDeleted(0) == true);
      assertTrue(deleteReader.hasDeletions() == true);
      assertTrue(deleteReader.numDocs() == 0);
      try {
        Document test = deleteReader.document(0);
        assertTrue(false);
      } catch (IllegalArgumentException e) {
        assertTrue(true);
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }    
  
  public void testGetFieldNameVariations() {
    Collection result = reader.getFieldNames();
    assertTrue(result != null);
    assertTrue(result.size() == 7);
    for (Iterator iter = result.iterator(); iter.hasNext();) {
      String s = (String) iter.next();
      //System.out.println("Name: " + s);
      assertTrue(DocHelper.nameValues.containsKey(s) == true || s.equals(""));
    }                                                                               
    result = reader.getFieldNames(true);
    assertTrue(result != null);
    assertTrue(result.size() == 5);
    for (Iterator iter = result.iterator(); iter.hasNext();) {
      String s = (String) iter.next();
      assertTrue(DocHelper.nameValues.containsKey(s) == true || s.equals(""));
    }
    
    result = reader.getFieldNames(false);
    assertTrue(result != null);
    assertTrue(result.size() == 2);
    //Get all indexed fields that are storing term vectors
    result = reader.getIndexedFieldNames(true);
    assertTrue(result != null);
    assertTrue(result.size() == 2);
    
    result = reader.getIndexedFieldNames(false);
    assertTrue(result != null);
    assertTrue(result.size() == 3);
  } 
  
  public void testTerms() {
    try {
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
      
      TermPositions positions = reader.termPositions();
      positions.seek(new Term(DocHelper.TEXT_FIELD_1_KEY, "field"));
      assertTrue(positions != null);
      assertTrue(positions.doc() == 0);
      assertTrue(positions.nextPosition() >= 0);
      
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }    
  
  public void testNorms() {
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
    assertTrue(results.length == 2);      
  }    
  
}
