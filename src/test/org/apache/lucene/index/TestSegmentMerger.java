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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;

import java.io.IOException;
import java.util.Collection;

public class TestSegmentMerger extends TestCase {
  //The variables for the new merged segment
  private Directory mergedDir = new RAMDirectory();
  private String mergedSegment = "test";
  //First segment to be merged
  private Directory merge1Dir = new RAMDirectory();
  private Document doc1 = new Document();
  private String merge1Segment = "test-1";
  private SegmentReader reader1 = null;
  //Second Segment to be merged
  private Directory merge2Dir = new RAMDirectory();
  private Document doc2 = new Document();
  private String merge2Segment = "test-2";
  private SegmentReader reader2 = null;
  

  public TestSegmentMerger(String s) {
    super(s);
  }

  protected void setUp() {
    DocHelper.setupDoc(doc1);
    DocHelper.writeDoc(merge1Dir, merge1Segment, doc1);
    DocHelper.setupDoc(doc2);
    DocHelper.writeDoc(merge2Dir, merge2Segment, doc2);
    try {
      reader1 = new SegmentReader(new SegmentInfo(merge1Segment, 1, merge1Dir));
      reader2 = new SegmentReader(new SegmentInfo(merge2Segment, 1, merge2Dir));
    } catch (IOException e) {
      e.printStackTrace();                                                      
    }

  }

  protected void tearDown() {

  }

  public void test() {
    assertTrue(mergedDir != null);
    assertTrue(merge1Dir != null);
    assertTrue(merge2Dir != null);
    assertTrue(reader1 != null);
    assertTrue(reader2 != null);
  }
  
  public void testMerge() {                             
    //System.out.println("----------------TestMerge------------------");
    SegmentMerger merger = new SegmentMerger(mergedDir, mergedSegment, false);
    merger.add(reader1);
    merger.add(reader2);
    try {
      int docsMerged = merger.merge();
      assertTrue(docsMerged == 2);
      //Should be able to open a new SegmentReader against the new directory
      SegmentReader mergedReader = new SegmentReader(new SegmentInfo(mergedSegment, docsMerged, mergedDir));
      assertTrue(mergedReader != null);
      assertTrue(mergedReader.numDocs() == 2);
      Document newDoc1 = mergedReader.document(0);
      assertTrue(newDoc1 != null);
      //There are 2 unstored fields on the document
      assertTrue(DocHelper.numFields(newDoc1) == DocHelper.numFields(doc1) - 2);
      Document newDoc2 = mergedReader.document(1);
      assertTrue(newDoc2 != null);
      assertTrue(DocHelper.numFields(newDoc2) == DocHelper.numFields(doc2) - 2);
      
      TermDocs termDocs = mergedReader.termDocs(new Term(DocHelper.TEXT_FIELD_2_KEY, "field"));
      assertTrue(termDocs != null);
      assertTrue(termDocs.next() == true);
      
      Collection stored = mergedReader.getIndexedFieldNames(true);
      assertTrue(stored != null);
      //System.out.println("stored size: " + stored.size());
      assertTrue(stored.size() == 2);
      
      TermFreqVector vector = mergedReader.getTermFreqVector(0, DocHelper.TEXT_FIELD_2_KEY);
      assertTrue(vector != null);
      String [] terms = vector.getTerms();
      assertTrue(terms != null);
      //System.out.println("Terms size: " + terms.length);
      assertTrue(terms.length == 3);
      int [] freqs = vector.getTermFrequencies();
      assertTrue(freqs != null);
      //System.out.println("Freqs size: " + freqs.length);
      
      for (int i = 0; i < terms.length; i++) {
        String term = terms[i];
        int freq = freqs[i];
        //System.out.println("Term: " + term + " Freq: " + freq);
        assertTrue(DocHelper.FIELD_2_TEXT.indexOf(term) != -1);
        assertTrue(DocHelper.FIELD_2_FREQS[i] == freq);
      }                                                
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }
    //System.out.println("---------------------end TestMerge-------------------");
  }    
}
