package org.apache.lucene.search;

/**
 * Copyright 2005 The Apache Software Foundation
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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * Tests {@link MultiSearcher} ranking, i.e. makes sure this bug is fixed:
 * http://issues.apache.org/bugzilla/show_bug.cgi?id=31841
 *
 * @version $Id: TestMultiSearcher.java 150492 2004-09-06 22:01:49Z dnaber $
 */
public class TestMultiSearcherRanking extends TestCase
{

  private final Query query = new TermQuery(new Term("body", "three"));
  
  public void testMultiSearcherRanking() throws IOException {
    Hits multiSearcherHits = multi();
    Hits singleSearcherHits = single();
    assertEquals(multiSearcherHits.length(), singleSearcherHits.length());
    for(int i = 0; i < multiSearcherHits.length(); i++) {
      assertEquals(multiSearcherHits.score(i), singleSearcherHits.score(i), 0.001f);
      Document docMulti = multiSearcherHits.doc(i);
      Document docSingle = singleSearcherHits.doc(i);
      assertEquals(docMulti.get("body"), docSingle.get("body"));
    }
  }

  // Collection 1+2 searched with MultiSearcher:
  private Hits multi() throws IOException {
		Directory d1 = new RAMDirectory();
  	IndexWriter iw = new IndexWriter(d1, new StandardAnalyzer(), true);
    addCollection1(iw);
    iw.close();

    Directory d2 = new RAMDirectory();
    iw = new IndexWriter(d2, new StandardAnalyzer(), true);
    addCollection2(iw);
    iw.close();
    
    Searchable[] s = new Searchable[2];
    s[0] = new IndexSearcher(d1);
    s[1] = new IndexSearcher(d2);
    MultiSearcher ms = new MultiSearcher(s);
    Hits hits = ms.search(query);
    return hits;
  }
  
  // Collection 1+2 indexed together:
  private Hits single() throws IOException {
    Directory d = new RAMDirectory();
    IndexWriter iw = new IndexWriter(d, new StandardAnalyzer(), true);
    addCollection1(iw);
    addCollection2(iw);
    iw.close();
    IndexSearcher is = new IndexSearcher(d);
    Hits hits = is.search(query);
    return hits;
  }

  private void addCollection1(IndexWriter iw) throws IOException {
    add("one blah three", iw);
    add("one foo three", iw);
    add("one foobar three", iw);
  }
  
  private void addCollection2(IndexWriter iw) throws IOException {
    add("two blah three", iw);
    add("two foo xxx", iw);
    add("two foobar xxx", iw);
  }

  private void add(String value, IndexWriter iw) throws IOException {
    Document d = new Document();
    d.add(new Field("body", value, Field.Store.YES, Field.Index.TOKENIZED));
    iw.addDocument(d);
  }
  
}
