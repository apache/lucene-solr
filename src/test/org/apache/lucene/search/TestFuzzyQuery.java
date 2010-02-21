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

import java.util.List;
import java.util.Arrays;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.queryParser.QueryParser;

/**
 * Tests {@link FuzzyQuery}.
 *
 */
public class TestFuzzyQuery extends LuceneTestCase {

  public void testFuzziness() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(TEST_VERSION_CURRENT), true, IndexWriter.MaxFieldLength.LIMITED);
    addDoc("aaaaa", writer);
    addDoc("aaaab", writer);
    addDoc("aaabb", writer);
    addDoc("aabbb", writer);
    addDoc("abbbb", writer);
    addDoc("bbbbb", writer);
    addDoc("ddddd", writer);
    writer.optimize();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory, true);

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
    
    // test scoring
    query = new FuzzyQuery(new Term("field", "bbbbb"), FuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("3 documents should match", 3, hits.length);
    List<String> order = Arrays.asList("bbbbb","abbbb","aabbb");
    for (int i = 0; i < hits.length; i++) {
      final String term = searcher.doc(hits[i].doc).get("field");
      //System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

    // test pq size by supplying maxExpansions=2
    // This query would normally return 3 documents, because 3 terms match (see above):
    query = new FuzzyQuery(new Term("field", "bbbbb"), FuzzyQuery.defaultMinSimilarity, 0, 2); 
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals("only 2 documents should match", 2, hits.length);
    order = Arrays.asList("bbbbb","abbbb");
    for (int i = 0; i < hits.length; i++) {
      final String term = searcher.doc(hits[i].doc).get("field");
      //System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

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
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(TEST_VERSION_CURRENT), true, IndexWriter.MaxFieldLength.LIMITED);
    addDoc("aaaaaaa", writer);
    addDoc("segment", writer);
    writer.optimize();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory, true);

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
  
  public void testTokenLengthOpt() throws IOException {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(TEST_VERSION_CURRENT),
        true, IndexWriter.MaxFieldLength.LIMITED);
    addDoc("12345678911", writer);
    addDoc("segment", writer);
    writer.optimize();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(directory, true);

    Query query;
    // term not over 10 chars, so optimization shortcuts
    query = new FuzzyQuery(new Term("field", "1234569"), 0.9f);
    ScoreDoc[] hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // 10 chars, so no optimization
    query = new FuzzyQuery(new Term("field", "1234567891"), 0.9f);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    // over 10 chars, so no optimization
    query = new FuzzyQuery(new Term("field", "12345678911"), 0.9f);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // over 10 chars, no match
    query = new FuzzyQuery(new Term("field", "sdfsdfsdfsdf"), 0.9f);
    hits = searcher.search(query, null, 1000).scoreDocs;
    assertEquals(0, hits.length);
  }
  
  public void testGiga() throws Exception {

    StandardAnalyzer analyzer = new StandardAnalyzer(TEST_VERSION_CURRENT);

    Directory index = new MockRAMDirectory();
    IndexWriter w = new IndexWriter(index, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);

    addDoc("Lucene in Action", w);
    addDoc("Lucene for Dummies", w);

    //addDoc("Giga", w);
    addDoc("Giga byte", w);

    addDoc("ManagingGigabytesManagingGigabyte", w);
    addDoc("ManagingGigabytesManagingGigabytes", w);

    addDoc("The Art of Computer Science", w);
    addDoc("J. K. Rowling", w);
    addDoc("JK Rowling", w);
    addDoc("Joanne K Roling", w);
    addDoc("Bruce Willis", w);
    addDoc("Willis bruce", w);
    addDoc("Brute willis", w);
    addDoc("B. willis", w);
    IndexReader r = w.getReader();
    w.close();

    Query q = new QueryParser(TEST_VERSION_CURRENT, "field", analyzer).parse( "giga~0.9" );

    // 3. search
    IndexSearcher searcher = new IndexSearcher(r);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("Giga byte", searcher.doc(hits[0].doc).get("field"));
    r.close();
  }

  private void addDoc(String text, IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(new Field("field", text, Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
  }

}
