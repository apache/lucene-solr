package org.apache.lucene.search;

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

import java.io.IOException;

import junit.framework.TestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

/**
 * Tests {@link FuzzyQuery}.
 *
 * @author Daniel Naber
 */
public class TestFuzzyQuery extends TestCase {

  public void testFuzziness() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    addDoc("aaaaa", writer);
    addDoc("aaaab", writer);
    addDoc("aaabb", writer);
    addDoc("aabbb", writer);
    addDoc("abbbb", writer);
    addDoc("bbbbb", writer);
    addDoc("ddddd", writer);
    writer.optimize();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory);

    FuzzyQuery query = new FuzzyQuery(new Term("field", "aaaaa"));   
    Hits hits = searcher.search(query);
    assertEquals(3, hits.length());

    // not similar enough:
    query = new FuzzyQuery(new Term("field", "xxxxx"));  	
    hits = searcher.search(query);
    assertEquals(0, hits.length());
    query = new FuzzyQuery(new Term("field", "aaccc"));   // edit distance to "aaaaa" = 3
    hits = searcher.search(query);
    assertEquals(0, hits.length());

    // query identical to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaaa"));   
    hits = searcher.search(query);
    assertEquals(3, hits.length());
    assertEquals(hits.doc(0).get("field"), ("aaaaa"));
    // default allows for up to two edits:
    assertEquals(hits.doc(1).get("field"), ("aaaab"));
    assertEquals(hits.doc(2).get("field"), ("aaabb"));

    // query similar to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaac"));   
    hits = searcher.search(query);
    assertEquals(3, hits.length());
    assertEquals(hits.doc(0).get("field"), ("aaaaa"));
    assertEquals(hits.doc(1).get("field"), ("aaaab"));
    assertEquals(hits.doc(2).get("field"), ("aaabb"));

    query = new FuzzyQuery(new Term("field", "ddddX"));   
    hits = searcher.search(query);
    assertEquals(1, hits.length());
    assertEquals(hits.doc(0).get("field"), ("ddddd"));

    // different field = no match:
    query = new FuzzyQuery(new Term("anotherfield", "ddddX"));   
    hits = searcher.search(query);
    assertEquals(0, hits.length());

    searcher.close();
    directory.close();
  }

  public void testFuzzinessLong() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    addDoc("aaaaaaa", writer);
    addDoc("segment", writer);
    writer.optimize();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory);

    FuzzyQuery query;
    // not similar enough:
    query = new FuzzyQuery(new Term("field", "xxxxx"));   
    Hits hits = searcher.search(query);
    assertEquals(0, hits.length());
    // edit distance to "aaaaaaa" = 3, this matches because the string is longer than
    // in testDefaultFuzziness so a bigger difference is allowed:
    query = new FuzzyQuery(new Term("field", "aaaaccc"));   
    hits = searcher.search(query);
    assertEquals(1, hits.length());
    assertEquals(hits.doc(0).get("field"), ("aaaaaaa"));

    // no match, more than half of the characters is wrong:
    query = new FuzzyQuery(new Term("field", "aaacccc"));   
    hits = searcher.search(query);
    assertEquals(0, hits.length());

    // "student" and "stellent" are indeed similar to "segment" by default:
    query = new FuzzyQuery(new Term("field", "student"));   
    hits = searcher.search(query);
    assertEquals(1, hits.length());
    query = new FuzzyQuery(new Term("field", "stellent"));   
    hits = searcher.search(query);
    assertEquals(1, hits.length());

    // "student" doesn't match anymore thanks to increased minimum similarity:
    query = new FuzzyQuery(new Term("field", "student"), 0.6f);   
    hits = searcher.search(query);
    assertEquals(0, hits.length());

    try {
      query = new FuzzyQuery(new Term("field", "student"), 1.1f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expecting exception
    }
    try {
      query = new FuzzyQuery(new Term("field", "student"), -0.1f);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expecting exception
    }

    searcher.close();
    directory.close();
  }
  
  private void addDoc(String text, IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(new Field("field", text, Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);
  }

}
