package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
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
      reader = new SegmentReader(new SegmentInfo("test", 1, dir));
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
      SegmentReader deleteReader = new SegmentReader(new SegmentInfo("seg-to-delete", 1, dir));
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
    try {
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
//      System.out.println("Size: " + result.size());
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
      
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
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
  
  public void testTermVectors() {
    try {
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
      
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }    
  
}
