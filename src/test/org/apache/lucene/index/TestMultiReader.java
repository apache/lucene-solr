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
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class TestMultiReader extends TestCase {
  private Directory dir = new RAMDirectory();
  private Document doc1 = new Document();
  private Document doc2 = new Document();
  private SegmentReader reader1;
  private SegmentReader reader2;
  private SegmentReader [] readers = new SegmentReader[2];
  private SegmentInfos sis = new SegmentInfos();
  
  public TestMultiReader(String s) {
    super(s);
  }

  protected void setUp() {
    DocHelper.setupDoc(doc1);
    DocHelper.setupDoc(doc2);
    DocHelper.writeDoc(dir, "seg-1", doc1);
    DocHelper.writeDoc(dir, "seg-2", doc2);
    
    try {
      sis.write(dir);
      reader1 = new SegmentReader(new SegmentInfo("seg-1", 1, dir));
      reader2 = new SegmentReader(new SegmentInfo("seg-2", 1, dir));
      readers[0] = reader1;
      readers[1] = reader2;      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
/*IndexWriter writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
      writer.addDocument(doc1);
      writer.addDocument(doc2);
      writer.close();*/
  protected void tearDown() {

  }
  
  public void test() {
    assertTrue(dir != null);
    assertTrue(reader1 != null);
    assertTrue(reader2 != null);
    assertTrue(sis != null);
  }    

  public void testDocument() {
    try {    
      sis.read(dir);
      MultiReader reader = new MultiReader(dir, readers);
      assertTrue(reader != null);
      Document newDoc1 = reader.document(0);
      assertTrue(newDoc1 != null);
      assertTrue(DocHelper.numFields(newDoc1) == DocHelper.numFields(doc1) - 2);
      Document newDoc2 = reader.document(1);
      assertTrue(newDoc2 != null);
      assertTrue(DocHelper.numFields(newDoc2) == DocHelper.numFields(doc2) - 2);
      TermFreqVector vector = reader.getTermFreqVector(0, DocHelper.TEXT_FIELD_2_KEY);
      assertTrue(vector != null);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
  
  public void testTermVectors() {
    try {
      MultiReader reader = new MultiReader(dir, readers);
      assertTrue(reader != null);
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }    
}
