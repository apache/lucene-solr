package org.apache.lucene.search;

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
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.English;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestTermVectors extends TestCase {
  private IndexSearcher searcher;
  private RAMDirectory directory = new RAMDirectory();
  public TestTermVectors(String s) {
    super(s);
  }

  public void setUp() throws Exception {                  
    IndexWriter writer
            = new IndexWriter(directory, new SimpleAnalyzer(), true);
    //writer.setUseCompoundFile(true);
    //writer.infoStream = System.out;
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(Field.Text("field", English.intToEnglish(i), true));
      writer.addDocument(doc);
    }
    writer.close();
    searcher = new IndexSearcher(directory);
  }

  protected void tearDown() {

  }

  public void test() {
    assertTrue(searcher != null);
  }

  public void testTermVectors() {
    Query query = new TermQuery(new Term("field", "seventy"));
    try {
      Hits hits = searcher.search(query);
      assertEquals(100, hits.length());
      
      for (int i = 0; i < hits.length(); i++)
      {
        TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits.id(i));
        assertTrue(vector != null);
        assertTrue(vector.length == 1);
        //assertTrue();
      }
      TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits.id(50));
      //System.out.println("Explain: " + searcher.explain(query, hits.id(50)));
      //System.out.println("Vector: " + vector[0].toString());
    } catch (IOException e) {
      assertTrue(false);
    }
  }
  
  public void testTermPositionVectors() {
    Query query = new TermQuery(new Term("field", "fifty"));
    try {
      Hits hits = searcher.search(query);
      assertEquals(100, hits.length());
      
      for (int i = 0; i < hits.length(); i++)
      {
        TermFreqVector [] vector = searcher.reader.getTermFreqVectors(hits.id(i));
        assertTrue(vector != null);
        assertTrue(vector.length == 1);
        //assertTrue();
      }
    } catch (IOException e) {
      assertTrue(false);
    }
  }
  
  public void testKnownSetOfDocuments() {
    String [] termArray = {"eating", "chocolate", "in", "a", "computer", "lab", "grows", "old", "colored",
                      "with", "an"};
    String test1 = "eating chocolate in a computer lab"; //6 terms
    String test2 = "computer in a computer lab"; //5 terms
    String test3 = "a chocolate lab grows old"; //5 terms
    String test4 = "eating chocolate with a chocolate lab in an old chocolate colored computer lab"; //13 terms
    Map test4Map = new HashMap();
    test4Map.put("chocolate", new Integer(3));
    test4Map.put("lab", new Integer(2));
    test4Map.put("eating", new Integer(1));
    test4Map.put("computer", new Integer(1));
    test4Map.put("with", new Integer(1));
    test4Map.put("a", new Integer(1));
    test4Map.put("colored", new Integer(1));
    test4Map.put("in", new Integer(1));
    test4Map.put("an", new Integer(1));
    test4Map.put("computer", new Integer(1));
    test4Map.put("old", new Integer(1));
    
    Document testDoc1 = new Document();
    setupDoc(testDoc1, test1);
    Document testDoc2 = new Document();
    setupDoc(testDoc2, test2);
    Document testDoc3 = new Document();
    setupDoc(testDoc3, test3);
    Document testDoc4 = new Document();
    setupDoc(testDoc4, test4);
        
    Directory dir = new RAMDirectory();
    
    try {
      IndexWriter writer = new IndexWriter(dir, new SimpleAnalyzer(), true);
      assertTrue(writer != null);
      writer.addDocument(testDoc1);
      writer.addDocument(testDoc2);
      writer.addDocument(testDoc3);
      writer.addDocument(testDoc4);
      writer.close();
      IndexSearcher knownSearcher = new IndexSearcher(dir);
      TermEnum termEnum = knownSearcher.reader.terms();
      TermDocs termDocs = knownSearcher.reader.termDocs();
      //System.out.println("Terms: " + termEnum.size() + " Orig Len: " + termArray.length);
      
      Similarity sim = knownSearcher.getSimilarity();
      while (termEnum.next() == true)
      {
        Term term = termEnum.term();
        //System.out.println("Term: " + term);
        termDocs.seek(term);
        while (termDocs.next())
        {
          int docId = termDocs.doc();
          int freq = termDocs.freq();
          //System.out.println("Doc Id: " + docId + " freq " + freq);
          TermFreqVector vector = knownSearcher.reader.getTermFreqVector(docId, "field");
          float tf = sim.tf(freq);
          float idf = sim.idf(term, knownSearcher);
          //float qNorm = sim.queryNorm()
          //This is fine since we don't have stop words
          float lNorm = sim.lengthNorm("field", vector.getTerms().length);
          //float coord = sim.coord()
          //System.out.println("TF: " + tf + " IDF: " + idf + " LenNorm: " + lNorm);
          assertTrue(vector != null);
          String[] vTerms = vector.getTerms();
          int [] freqs = vector.getTermFrequencies();
          for (int i = 0; i < vTerms.length; i++)
          {
            if (term.text().equals(vTerms[i]) == true)
            {
              assertTrue(freqs[i] == freq);
            }
          }
          
        }
        //System.out.println("--------");
      }
      Query query = new TermQuery(new Term("field", "chocolate"));
      Hits hits = knownSearcher.search(query);
      //doc 3 should be the first hit b/c it is the shortest match
      assertTrue(hits.length() == 3);
      float score = hits.score(0);
      /*System.out.println("Hit 0: " + hits.id(0) + " Score: " + hits.score(0) + " String: " + hits.doc(0).toString());
      System.out.println("Explain: " + knownSearcher.explain(query, hits.id(0)));
      System.out.println("Hit 1: " + hits.id(1) + " Score: " + hits.score(1) + " String: " + hits.doc(1).toString());
      System.out.println("Explain: " + knownSearcher.explain(query, hits.id(1)));
      System.out.println("Hit 2: " + hits.id(2) + " Score: " + hits.score(2) + " String: " +  hits.doc(2).toString());
      System.out.println("Explain: " + knownSearcher.explain(query, hits.id(2)));*/
      assertTrue(testDoc3.toString().equals(hits.doc(0).toString()));
      assertTrue(testDoc4.toString().equals(hits.doc(1).toString()));
      assertTrue(testDoc1.toString().equals(hits.doc(2).toString()));
      TermFreqVector vector = knownSearcher.reader.getTermFreqVector(hits.id(1), "field");
      assertTrue(vector != null);
      //System.out.println("Vector: " + vector);
      String[] terms = vector.getTerms();
      int [] freqs = vector.getTermFrequencies();
      assertTrue(terms != null && terms.length == 10);
      for (int i = 0; i < terms.length; i++) {
        String term = terms[i];
        //System.out.println("Term: " + term);
        int freq = freqs[i];
        assertTrue(test4.indexOf(term) != -1);
        Integer freqInt = (Integer)test4Map.get(term);
        assertTrue(freqInt != null);
        assertTrue(freqInt.intValue() == freq);        
      } 
      knownSearcher.close();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }


  } 
  
  private void setupDoc(Document doc, String text)
  {
    doc.add(Field.Text("field", text, true));
    //System.out.println("Document: " + doc);
  }
  
  
}
