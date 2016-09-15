/*
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
package org.apache.lucene.sandbox.queries;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/**
 * Tests {@link SlowFuzzyQuery}.
 *
 */
public class TestSlowFuzzyQuery extends LuceneTestCase {

  public void testFuzziness() throws Exception {
    //every test with SlowFuzzyQuery.defaultMinSimilarity
    //is exercising the Automaton, not the brute force linear method
    
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("aaaaa", writer);
    addDoc("aaaab", writer);
    addDoc("aaabb", writer);
    addDoc("aabbb", writer);
    addDoc("abbbb", writer);
    addDoc("bbbbb", writer);
    addDoc("ddddd", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    SlowFuzzyQuery query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 0);   
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    
    // same with prefix
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 3);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 6);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    
    // test scoring
    query = new SlowFuzzyQuery(new Term("field", "bbbbb"), SlowFuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("3 documents should match", 3, hits.length);
    List<String> order = Arrays.asList("bbbbb","abbbb","aabbb");
    for (int i = 0; i < hits.length; i++) {
      final String term = searcher.doc(hits[i].doc).get("field");
      //System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

    // test pq size by supplying maxExpansions=2
    // This query would normally return 3 documents, because 3 terms match (see above):
    query = new SlowFuzzyQuery(new Term("field", "bbbbb"), SlowFuzzyQuery.defaultMinSimilarity, 0, 2); 
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("only 2 documents should match", 2, hits.length);
    order = Arrays.asList("bbbbb","abbbb");
    for (int i = 0; i < hits.length; i++) {
      final String term = searcher.doc(hits[i].doc).get("field");
      //System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

    // not similar enough:
    query = new SlowFuzzyQuery(new Term("field", "xxxxx"), SlowFuzzyQuery.defaultMinSimilarity, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "aaccc"), SlowFuzzyQuery.defaultMinSimilarity, 0);   // edit distance to "aaaaa" = 3
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // query identical to a word in the index:
    query = new SlowFuzzyQuery(new Term("field", "aaaaa"), SlowFuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    // default allows for up to two edits:
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));

    // query similar to a word in the index:
    query = new SlowFuzzyQuery(new Term("field", "aaaac"), SlowFuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    
    // now with prefix
    query = new SlowFuzzyQuery(new Term("field", "aaaac"), SlowFuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new SlowFuzzyQuery(new Term("field", "aaaac"), SlowFuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new SlowFuzzyQuery(new Term("field", "aaaac"), SlowFuzzyQuery.defaultMinSimilarity, 3);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new SlowFuzzyQuery(new Term("field", "aaaac"), SlowFuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    query = new SlowFuzzyQuery(new Term("field", "aaaac"), SlowFuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    

    query = new SlowFuzzyQuery(new Term("field", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    
    // now with prefix
    query = new SlowFuzzyQuery(new Term("field", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new SlowFuzzyQuery(new Term("field", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new SlowFuzzyQuery(new Term("field", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 3);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new SlowFuzzyQuery(new Term("field", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new SlowFuzzyQuery(new Term("field", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    

    // different field = no match:
    query = new SlowFuzzyQuery(new Term("anotherfield", "ddddX"), SlowFuzzyQuery.defaultMinSimilarity, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    reader.close();
    directory.close();
  }

  public void testFuzzinessLong2() throws Exception {
     //Lucene-5033
     Directory directory = newDirectory();
     RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
     addDoc("abcdef", writer);
     addDoc("segment", writer);

     IndexReader reader = writer.getReader();
     IndexSearcher searcher = newSearcher(reader);
     writer.close();

     SlowFuzzyQuery query;
     
     query = new SlowFuzzyQuery(new Term("field", "abcxxxx"), 3f, 0);   
     ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
     assertEquals(0, hits.length);
     
     query = new SlowFuzzyQuery(new Term("field", "abcxxxx"), 4f, 0);   
     hits = searcher.search(query, 1000).scoreDocs;
     assertEquals(1, hits.length);
     reader.close();
     directory.close();
  }
  
  public void testFuzzinessLong() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("aaaaaaa", writer);
    addDoc("segment", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    SlowFuzzyQuery query;
    // not similar enough:
    query = new SlowFuzzyQuery(new Term("field", "xxxxx"), 0.5f, 0);   
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    // edit distance to "aaaaaaa" = 3, this matches because the string is longer than
    // in testDefaultFuzziness so a bigger difference is allowed:
    query = new SlowFuzzyQuery(new Term("field", "aaaaccc"), 0.5f, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaaaa"));
    
    // now with prefix
    query = new SlowFuzzyQuery(new Term("field", "aaaaccc"), 0.5f, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaaaa"));
    query = new SlowFuzzyQuery(new Term("field", "aaaaccc"), 0.5f, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaaaa"));
    query = new SlowFuzzyQuery(new Term("field", "aaaaccc"), 0.5f, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // no match, more than half of the characters is wrong:
    query = new SlowFuzzyQuery(new Term("field", "aaacccc"), 0.5f, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    // now with prefix
    query = new SlowFuzzyQuery(new Term("field", "aaacccc"), 0.5f, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // "student" and "stellent" are indeed similar to "segment" by default:
    query = new SlowFuzzyQuery(new Term("field", "student"), 0.5f, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "stellent"), 0.5f, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    
    // now with prefix
    query = new SlowFuzzyQuery(new Term("field", "student"), 0.5f, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "stellent"), 0.5f, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "student"), 0.5f, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query = new SlowFuzzyQuery(new Term("field", "stellent"), 0.5f, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    // "student" doesn't match anymore thanks to increased minimum similarity:
    query = new SlowFuzzyQuery(new Term("field", "student"), 0.6f, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    expectThrows(IllegalArgumentException.class, () -> {
      new SlowFuzzyQuery(new Term("field", "student"), 1.1f);
    });

    expectThrows(IllegalArgumentException.class, () -> {
      new SlowFuzzyQuery(new Term("field", "student"), -0.1f);
    });

    reader.close();
    directory.close();
  }
  
  /** 
   * MultiTermQuery provides (via attribute) information about which values
   * must be competitive to enter the priority queue. 
   * 
   * SlowFuzzyQuery optimizes itself around this information, if the attribute
   * is not implemented correctly, there will be problems!
   */
  public void testTieBreaker() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("a123456", writer);
    addDoc("c123456", writer);
    addDoc("d123456", writer);
    addDoc("e123456", writer);
    
    Directory directory2 = newDirectory();
    RandomIndexWriter writer2 = new RandomIndexWriter(random(), directory2);
    addDoc("a123456", writer2);
    addDoc("b123456", writer2);
    addDoc("b123456", writer2);
    addDoc("b123456", writer2);
    addDoc("c123456", writer2);
    addDoc("f123456", writer2);
    
    IndexReader ir1 = writer.getReader();
    IndexReader ir2 = writer2.getReader();
    
    MultiReader mr = new MultiReader(ir1, ir2);
    IndexSearcher searcher = newSearcher(mr);
    SlowFuzzyQuery fq = new SlowFuzzyQuery(new Term("field", "z123456"), 1f, 0, 2);
    TopDocs docs = searcher.search(fq, 2);
    assertEquals(5, docs.totalHits); // 5 docs, from the a and b's
    mr.close();
    ir1.close();
    ir2.close();
    writer.close();
    writer2.close();
    directory.close();
    directory2.close(); 
  }
  
  public void testTokenLengthOpt() throws IOException {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("12345678911", writer);
    addDoc("segment", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    Query query;
    // term not over 10 chars, so optimization shortcuts
    query = new SlowFuzzyQuery(new Term("field", "1234569"), 0.9f);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // 10 chars, so no optimization
    query = new SlowFuzzyQuery(new Term("field", "1234567891"), 0.9f);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    // over 10 chars, so no optimization
    query = new SlowFuzzyQuery(new Term("field", "12345678911"), 0.9f);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // over 10 chars, no match
    query = new SlowFuzzyQuery(new Term("field", "sdfsdfsdfsdf"), 0.9f);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    
    reader.close();
    directory.close();
  }
  
  /** Test the TopTermsBoostOnlyBooleanQueryRewrite rewrite method. */
  public void testBoostOnlyRewrite() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("Lucene", writer);
    addDoc("Lucene", writer);
    addDoc("Lucenne", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();
    
    SlowFuzzyQuery query = new SlowFuzzyQuery(new Term("field", "lucene"));
    query.setRewriteMethod(new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(50));
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    // normally, 'Lucenne' would be the first result as IDF will skew the score.
    assertEquals("Lucene", reader.document(hits[0].doc).get("field"));
    assertEquals("Lucene", reader.document(hits[1].doc).get("field"));
    assertEquals("Lucenne", reader.document(hits[2].doc).get("field"));
    reader.close();
    directory.close();
  }
  
  public void testGiga() throws Exception {

    Directory index = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), index);

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

    Query q = new SlowFuzzyQuery(new Term("field", "giga"), 0.9f);

    // 3. search
    IndexSearcher searcher = newSearcher(r);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("Giga byte", searcher.doc(hits[0].doc).get("field"));
    r.close();
    index.close();
  }
  
  public void testDistanceAsEditsSearching() throws Exception {
    Directory index = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), index);
    addDoc("foobar", w);
    addDoc("test", w);
    addDoc("working", w);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    w.close();
    
    SlowFuzzyQuery q = new SlowFuzzyQuery(new Term("field", "fouba"), 2);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("foobar", searcher.doc(hits[0].doc).get("field"));
    
    q = new SlowFuzzyQuery(new Term("field", "foubara"), 2);
    hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("foobar", searcher.doc(hits[0].doc).get("field"));
    
    q = new SlowFuzzyQuery(new Term("field", "t"), 3);
    hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("test", searcher.doc(hits[0].doc).get("field"));
    
    q = new SlowFuzzyQuery(new Term("field", "a"), 4f, 0, 50);
    hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("test", searcher.doc(hits[0].doc).get("field"));
    
    q = new SlowFuzzyQuery(new Term("field", "a"), 6f, 0, 50);
    hits = searcher.search(q, 10).scoreDocs;
    assertEquals(2, hits.length);

    // We cannot expect a particular order since both hits 0.0 score:
    Set<String> actual = new HashSet<>();
    actual.add(searcher.doc(hits[0].doc).get("field"));
    actual.add(searcher.doc(hits[1].doc).get("field"));

    Set<String> expected = new HashSet<>();
    expected.add("test");
    expected.add("foobar");
    
    assertEquals(expected, actual);
    
    reader.close();
    index.close();
  }

  private void addDoc(String text, RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("field", text, Field.Store.YES));
    writer.addDocument(doc);
  }
}
