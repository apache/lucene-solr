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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import junit.framework.TestCase;

 /** Document boost unit test.
  *
  * @author Doug Cutting
  * @version $Revision$
  */
public class TestDocBoost extends TestCase {
  public TestDocBoost(String name) {
    super(name);
  }
  
  public static void test() throws Exception {
    RAMDirectory store = new RAMDirectory();
    IndexWriter writer = new IndexWriter(store, new SimpleAnalyzer(), true);
    
    Field f1 = Field.Text("field", "word");
    Field f2 = Field.Text("field", "word");
    f2.setBoost(2.0f);
    
    Document d1 = new Document();
    Document d2 = new Document();
    Document d3 = new Document();
    Document d4 = new Document();
    d3.setBoost(3.0f);
    d4.setBoost(2.0f);
    
    d1.add(f1);                                 // boost = 1
    d2.add(f2);                                 // boost = 2
    d3.add(f1);                                 // boost = 3
    d4.add(f2);                                 // boost = 4
    
    writer.addDocument(d1);
    writer.addDocument(d2);
    writer.addDocument(d3);
    writer.addDocument(d4);
    writer.optimize();
    writer.close();

    final float[] scores = new float[4];

    new IndexSearcher(store).search
      (new TermQuery(new Term("field", "word")),
       new HitCollector() {
         public final void collect(int doc, float score) {
           scores[doc] = score;
         }
       });
    
    float lastScore = 0.0f;

    for (int i = 0; i < 4; i++) {
      assertTrue(scores[i] > lastScore);
      lastScore = scores[i];
    }
  }
}
