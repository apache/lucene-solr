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

import java.io.IOException;

public class TestTermVectorsWriter extends TestCase {

  private String[] testTerms = {"this", "is", "a", "test"};
  private String [] testFields = {"f1", "f2", "f3"};
  private int[][] positions = new int[testTerms.length][];
  private RAMDirectory dir = new RAMDirectory();
  private String seg = "testSegment";
  private FieldInfos fieldInfos = new FieldInfos();

  public TestTermVectorsWriter(String s) {
    super(s);
  }

  protected void setUp() {

    for (int i = 0; i < testFields.length; i++) {
      fieldInfos.add(testFields[i], true, true);
    }
    

    for (int i = 0; i < testTerms.length; i++) {
      positions[i] = new int[5];
      for (int j = 0; j < positions[i].length; j++) {
        positions[i][j] = i * 100;
      }
    }
  }

  protected void tearDown() {
  }

  public void test() {
    assertTrue(dir != null);
    assertTrue(positions != null);
  }
  
  /*public void testWriteNoPositions() {
    try {
      TermVectorsWriter writer = new TermVectorsWriter(dir, seg, 50);
      writer.openDocument();
      assertTrue(writer.isDocumentOpen() == true);
      writer.openField(0);
      assertTrue(writer.isFieldOpen() == true);
      for (int i = 0; i < testTerms.length; i++) {
        writer.addTerm(testTerms[i], i);
      }
      writer.closeField();
      
      writer.closeDocument();
      writer.close();
      assertTrue(writer.isDocumentOpen() == false);
      //Check to see the files were created
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVD_EXTENSION));
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVX_EXTENSION));
      //Now read it back in
      TermVectorsReader reader = new TermVectorsReader(dir, seg);
      assertTrue(reader != null);
      checkTermVector(reader, 0, 0);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }  */  

  public void testWriter() {
    try {
      TermVectorsWriter writer = new TermVectorsWriter(dir, seg, fieldInfos);
      writer.openDocument();
      assertTrue(writer.isDocumentOpen() == true);
      writeField(writer, testFields[0]);
      writer.closeDocument();
      writer.close();
      assertTrue(writer.isDocumentOpen() == false);
      //Check to see the files were created
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVD_EXTENSION));
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVX_EXTENSION));
      //Now read it back in
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      assertTrue(reader != null);
      checkTermVector(reader, 0, testFields[0]);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  private void checkTermVector(TermVectorsReader reader, int docNum, String field) throws IOException {
    TermFreqVector vector = reader.get(docNum, field);
    assertTrue(vector != null);
    String[] terms = vector.getTerms();
    assertTrue(terms != null);
    assertTrue(terms.length == testTerms.length);
    for (int i = 0; i < terms.length; i++) {
      String term = terms[i];
      assertTrue(term.equals(testTerms[i]));
    }
  }

  /**
   * Test one document, multiple fields
   */
  public void testMultipleFields() {
    try {
      TermVectorsWriter writer = new TermVectorsWriter(dir, seg, fieldInfos);
      writeDocument(writer, testFields.length);

      writer.close();

      assertTrue(writer.isDocumentOpen() == false);
      //Check to see the files were created
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVD_EXTENSION));
      assertTrue(dir.fileExists(seg + TermVectorsWriter.TVX_EXTENSION));
      //Now read it back in
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      assertTrue(reader != null);

      for (int j = 0; j < testFields.length; j++) {
        checkTermVector(reader, 0, testFields[j]);
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  private void writeDocument(TermVectorsWriter writer, int numFields) throws IOException {
    writer.openDocument();
    assertTrue(writer.isDocumentOpen() == true);

    for (int j = 0; j < numFields; j++) {
      writeField(writer, testFields[j]);
    }
    writer.closeDocument();
    assertTrue(writer.isDocumentOpen() == false);
  }

  /**
   * 
   * @param writer The writer to write to
   * @param j The field number
   * @throws IOException
   */
  private void writeField(TermVectorsWriter writer, String f) throws IOException {
    writer.openField(f);
    assertTrue(writer.isFieldOpen() == true);
    for (int i = 0; i < testTerms.length; i++) {
      writer.addTerm(testTerms[i], i);
    }
    writer.closeField();
  }


  public void testMultipleDocuments() {

    try {
      TermVectorsWriter writer = new TermVectorsWriter(dir, seg, fieldInfos);
      assertTrue(writer != null);
      for (int i = 0; i < 10; i++) {
        writeDocument(writer, testFields.length);
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }      
    //Do some arbitrary tests
    try {
      TermVectorsReader reader = new TermVectorsReader(dir, seg, fieldInfos);
      for (int i = 0; i < 10; i++) {        
        assertTrue(reader != null);
        checkTermVector(reader, 5, testFields[0]);
        checkTermVector(reader, 2, testFields[2]);
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

}
