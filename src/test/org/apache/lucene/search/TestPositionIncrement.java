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
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.io.Reader;
import java.io.IOException;

import junit.framework.TestCase;

 /** Document boost unit test.
  *
  * @author Doug Cutting
  * @version $Revision$
  */
public class TestPositionIncrement extends TestCase {
  public TestPositionIncrement(String name) {
    super(name);
  }
  

  public static void test() throws Exception {
    Analyzer analyzer = new Analyzer() {
        public TokenStream tokenStream(String fieldName, Reader reader) {
          return new TokenStream() {
              private final String[] TOKENS = {"1", "2", "3", "4", "5"};
              private final int[] INCREMENTS = {1, 2,  1,    0,   1};
              private int i = 0;
              public Token next() throws IOException {
                if (i == TOKENS.length)
                  return null;
                Token t = new Token(TOKENS[i], i, i);
                t.setPositionIncrement(INCREMENTS[i]);
                i++;
                return t;
              }
            };
        }
      };
    RAMDirectory store = new RAMDirectory();
    IndexWriter writer = new IndexWriter(store, analyzer, true);
    Document d = new Document();
    d.add(Field.Text("field", "bogus"));
    writer.addDocument(d);
    writer.optimize();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(store);
    PhraseQuery q;
    Hits hits;

    q = new PhraseQuery();
    q.add(new Term("field","1"));
    q.add(new Term("field","2"));
    hits = searcher.search(q);
    assertEquals(0, hits.length());

    q = new PhraseQuery();
    q.add(new Term("field","2"));
    q.add(new Term("field","3"));
    hits = searcher.search(q);
    assertEquals(1, hits.length());

    q = new PhraseQuery();
    q.add(new Term("field","3"));
    q.add(new Term("field","4"));
    hits = searcher.search(q);
    assertEquals(0, hits.length());

    q = new PhraseQuery();
    q.add(new Term("field","2"));
    q.add(new Term("field","4"));
    hits = searcher.search(q);
    assertEquals(1, hits.length());

    q = new PhraseQuery();
    q.add(new Term("field","3"));
    q.add(new Term("field","5"));
    hits = searcher.search(q);
    assertEquals(1, hits.length());

    q = new PhraseQuery();
    q.add(new Term("field","4"));
    q.add(new Term("field","5"));
    hits = searcher.search(q);
    assertEquals(1, hits.length());

    q = new PhraseQuery();
    q.add(new Term("field","2"));
    q.add(new Term("field","5"));
    hits = searcher.search(q);
    assertEquals(0, hits.length());

  }
}
