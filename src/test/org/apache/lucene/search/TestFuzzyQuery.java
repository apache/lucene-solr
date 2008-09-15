package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

/**
 * Tests {@link FuzzyQuery}.
 *
 */
public class TestFuzzyQuery extends LuceneTestCase {

  public void testFuzziness() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
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

    FuzzyQuery query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 0);   
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    
    // same with prefix
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 3);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 6);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // not similar enough:
    query = new FuzzyQuery(new Term("field", "xxxxx"), FuzzyQuery.defaultMinSimilarity, 0);  	
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query = new FuzzyQuery(new Term("field", "aaccc"), FuzzyQuery.defaultMinSimilarity, 0);   // edit distance to "aaaaa" = 3
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // query identical to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    // default allows for up to two edits:
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));

    // query similar to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 3);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(2, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    

    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 3);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    

    // different field = no match:
    query = new FuzzyQuery(new Term("anotherfield", "ddddX"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    searcher.close();
    directory.close();
  }

  public void testFuzzinessLong() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    addDoc("aaaaaaa", writer);
    addDoc("segment", writer);
    writer.optimize();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory);

    FuzzyQuery query;
    // not similar enough:
    query = new FuzzyQuery(new Term("field", "xxxxx"), FuzzyQuery.defaultMinSimilarity, 0);   
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    // edit distance to "aaaaaaa" = 3, this matches because the string is longer than
    // in testDefaultFuzziness so a bigger difference is allowed:
    query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaaaa"));
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaaaa"));
    query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaaaa"));
    query = new FuzzyQuery(new Term("field", "aaaaccc"), FuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // no match, more than half of the characters is wrong:
    query = new FuzzyQuery(new Term("field", "aaacccc"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "aaacccc"), FuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // "student" and "stellent" are indeed similar to "segment" by default:
    query = new FuzzyQuery(new Term("field", "student"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new FuzzyQuery(new Term("field", "stellent"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "student"), FuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new FuzzyQuery(new Term("field", "stellent"), FuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new FuzzyQuery(new Term("field", "student"), FuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query = new FuzzyQuery(new Term("field", "stellent"), FuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    // "student" doesn't match anymore thanks to increased minimum similarity:
    query = new FuzzyQuery(new Term("field", "student"), 0.6f, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

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
    doc.add(new Field("field", text, Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
  }

}
