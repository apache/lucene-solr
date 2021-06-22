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
package org.apache.lucene.search;

import static org.hamcrest.CoreMatchers.containsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.lucene.util.automaton.Operations;

import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Tests {@link FuzzyQuery}.
 *
 */
public class TestFuzzyQuery extends LuceneTestCase {

  public void testBasicPrefix() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("abc", writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    FuzzyQuery query = new FuzzyQuery(new Term("field", "abc"), FuzzyQuery.defaultMaxEdits, 1);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    reader.close();
    directory.close();
  }

  public void testFuzziness() throws Exception {
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

    FuzzyQuery query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 0);   
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    
    // same with prefix
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 3);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 6);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    
    // test scoring
    query = new FuzzyQuery(new Term("field", "bbbbb"), FuzzyQuery.defaultMaxEdits, 0);   
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
    query = new FuzzyQuery(new Term("field", "bbbbb"), FuzzyQuery.defaultMaxEdits, 0, 2, false); 
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("only 2 documents should match", 2, hits.length);
    order = Arrays.asList("bbbbb","abbbb");
    for (int i = 0; i < hits.length; i++) {
      final String term = searcher.doc(hits[i].doc).get("field");
      //System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

    // not similar enough:
    query = new FuzzyQuery(new Term("field", "xxxxx"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query = new FuzzyQuery(new Term("field", "aaccc"), FuzzyQuery.defaultMaxEdits, 0);   // edit distance to "aaaaa" = 3
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // query identical to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    // default allows for up to two edits:
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));

    // query similar to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 3);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(searcher.doc(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(searcher.doc(hits[1].doc).get("field"), ("aaaab"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    

    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    
    // now with prefix
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 1);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 2);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 3);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 4);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(searcher.doc(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 5);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    

    // different field = no match:
    query = new FuzzyQuery(new Term("anotherfield", "ddddX"), FuzzyQuery.defaultMaxEdits, 0);   
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    reader.close();
    directory.close();
  }
  
  public void test2() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    addDoc("LANGE", writer);
    addDoc("LUETH", writer);
    addDoc("PIRSING", writer);
    addDoc("RIEGEL", writer);
    addDoc("TRZECZIAK", writer);
    addDoc("WALKER", writer);
    addDoc("WBR", writer);
    addDoc("WE", writer);
    addDoc("WEB", writer);
    addDoc("WEBE", writer);
    addDoc("WEBER", writer);
    addDoc("WEBERE", writer);
    addDoc("WEBREE", writer);
    addDoc("WEBEREI", writer);
    addDoc("WBRE", writer);
    addDoc("WITTKOPF", writer);
    addDoc("WOJNAROWSKI", writer);
    addDoc("WRICKE", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    FuzzyQuery query = new FuzzyQuery(new Term("field", "WEBER"), 2, 1);
    //query.setRewriteMethod(FuzzyQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(8, hits.length);

    reader.close();
    directory.close();
  }
  
  public void testSingleQueryExactMatchScoresHighest() throws Exception {
    //See issue LUCENE-329 - IDF shouldn't wreck similarity ranking 
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smythe", writer);
    addDoc("smdssasd", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new ClassicSimilarity()); //avoid randomisation of similarity algo by test framework
    writer.close();
    String searchTerms[] = { "smith", "smythe", "smdssasd" };
    for (String searchTerm : searchTerms) {
      FuzzyQuery query = new FuzzyQuery(new Term("field", searchTerm), 2, 1);
      ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
      Document bestDoc = searcher.doc(hits[0].doc);
      assertTrue(hits.length > 0);
      String topMatch = bestDoc.get("field");
      assertEquals(searchTerm, topMatch);
      if (hits.length > 1) {
        Document worstDoc = searcher.doc(hits[hits.length - 1].doc);
        String worstMatch = worstDoc.get("field");
        assertNotSame(searchTerm, worstMatch);
      }
    }
    reader.close();
    directory.close();
  }
  
  public void testMultipleQueriesIdfWorks() throws Exception {
    // With issue LUCENE-329 - it could be argued a MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite
    // is the solution as it disables IDF.
    // However - IDF is still useful as in this case where there are multiple FuzzyQueries.
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    addDoc("michael smith", writer);
    addDoc("michael lucero", writer);
    addDoc("doug cutting", writer);
    addDoc("doug cuttin", writer);
    addDoc("michael wardle", writer);
    addDoc("micheal vegas", writer);
    addDoc("michael lydon", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new ClassicSimilarity()); //avoid randomisation of similarity algo by test framework

    writer.close();

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    String commonSearchTerm = "michael";
    FuzzyQuery commonQuery = new FuzzyQuery(new Term("field", commonSearchTerm), 2, 1);
    query.add(commonQuery, Occur.SHOULD);

    String rareSearchTerm = "cutting";
    FuzzyQuery rareQuery = new FuzzyQuery(new Term("field", rareSearchTerm), 2, 1);
    query.add(rareQuery, Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(query.build(), 1000).scoreDocs;

    // Matches on the rare surname should be worth more than matches on the common forename
    assertEquals(7, hits.length);
    Document bestDoc = searcher.doc(hits[0].doc);
    String topMatch = bestDoc.get("field");
    assertTrue(topMatch.contains(rareSearchTerm));

    Document runnerUpDoc = searcher.doc(hits[1].doc);
    String runnerUpMatch = runnerUpDoc.get("field");
    assertTrue(runnerUpMatch.contains("cuttin"));

    Document worstDoc = searcher.doc(hits[hits.length - 1].doc);
    String worstMatch = worstDoc.get("field");
    assertTrue(worstMatch.contains("micheal")); //misspelling of common name

    reader.close();
    directory.close();
  }

  /** 
   * MultiTermQuery provides (via attribute) information about which values
   * must be competitive to enter the priority queue. 
   * 
   * FuzzyQuery optimizes itself around this information, if the attribute
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
    FuzzyQuery fq = new FuzzyQuery(new Term("field", "z123456"), 1, 0, 2, false);
    TopDocs docs = searcher.search(fq, 2);
    assertEquals(5, docs.totalHits.value); // 5 docs, from the a and b's
    mr.close();
    ir1.close();
    ir2.close();
    writer.close();
    writer2.close();
    directory.close();
    directory2.close(); 
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
    
    FuzzyQuery query = new FuzzyQuery(new Term("field", "lucene"));
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

    Query q = new FuzzyQuery(new Term("field", "giga"), 0);

    // 3. search
    IndexSearcher searcher = newSearcher(r);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("Giga byte", searcher.doc(hits[0].doc).get("field"));
    r.close();
    w.close();
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
    
    FuzzyQuery q = new FuzzyQuery(new Term("field", "fouba"), 2);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("foobar", searcher.doc(hits[0].doc).get("field"));
    
    q = new FuzzyQuery(new Term("field", "foubara"), 2);
    hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("foobar", searcher.doc(hits[0].doc).get("field"));
    
    expectThrows(IllegalArgumentException.class, () -> {
      new FuzzyQuery(new Term("field", "t"), 3);
    });

    reader.close();
    index.close();
  }

  public void testValidation() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new FuzzyQuery(new Term("field", "foo"), -1, 0, 1, false);
    });
    assertTrue(expected.getMessage().contains("maxEdits"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new FuzzyQuery(new Term("field", "foo"), LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + 1, 0, 1, false);
    });
    assertTrue(expected.getMessage().contains("maxEdits must be between"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new FuzzyQuery(new Term("field", "foo"), 1, -1, 1, false);
    });
    assertTrue(expected.getMessage().contains("prefixLength cannot be negative"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new FuzzyQuery(new Term("field", "foo"), 1, 0, -1, false);
    });
    assertTrue(expected.getMessage().contains("maxExpansions must be positive"));

    expected = expectThrows(IllegalArgumentException.class, () -> {
      new FuzzyQuery(new Term("field", "foo"), 1, 0, -1, false);
    });
    assertTrue(expected.getMessage().contains("maxExpansions must be positive"));
  }

  private String randomRealisticMultiByteUnicode(int length) {
    while (true) {
      // There is 1 single-byte unicode block, and 194 multi-byte blocks
      String value = RandomizedTest.randomRealisticUnicodeOfCodepointLength(length);
      if (value.charAt(0) > Byte.MAX_VALUE) {
        return value;
      }
    }
  }

  public void testErrorMessage() {
    // 45 states per vector from Lev2TParametricDescription
    final int length = (Operations.DEFAULT_DETERMINIZE_WORK_LIMIT / 5) + 10;
    final String value = randomRealisticMultiByteUnicode(length);

    FuzzyTermsEnum.FuzzyTermsException expected =
        expectThrows(
            FuzzyTermsEnum.FuzzyTermsException.class,
            () -> {
              new FuzzyAutomatonBuilder(value, 2, 0, true).buildMaxEditAutomaton();
            });
    assertThat(expected.getMessage(), containsString(value));

    expected =
        expectThrows(
            FuzzyTermsEnum.FuzzyTermsException.class,
            () -> new FuzzyAutomatonBuilder(value, 2, 0, true).buildAutomatonSet());
    assertThat(expected.getMessage(), containsString(value));
  }

  private void addDoc(String text, RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("field", text, Field.Store.YES));
    writer.addDocument(doc);
  }

  private String randomSimpleString(int digits) {
    int termLength = TestUtil.nextInt(random(), 1, 8);
    char[] chars = new char[termLength];
    for(int i=0;i<termLength;i++) {
      chars[i] = (char) ('a' + random().nextInt(digits));
    }
    return new String(chars);
  }

  @SuppressWarnings({"unchecked","rawtypes"})
  public void testRandom() throws Exception {
    int digits = TestUtil.nextInt(random(), 2, 3);
    // underestimated total number of unique terms that randomSimpleString
    // maybe generate, it assumes all terms have a length of 7
    int vocabularySize = digits << 7;
    int numTerms = Math.min(atLeast(100), vocabularySize);
    Set<String> terms = new HashSet<>();
    while (terms.size() < numTerms) {
      terms.add(randomSimpleString(digits));
    }

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    for(String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.YES));
      w.addDocument(doc);
    }
    DirectoryReader r = w.getReader();
    w.close();
    //System.out.println("TEST: reader=" + r);
    IndexSearcher s = newSearcher(r);
    int iters = atLeast(200);
    for(int iter=0;iter<iters;iter++) {
      String queryTerm = randomSimpleString(digits);
      int prefixLength = random().nextInt(queryTerm.length());
      String queryPrefix = queryTerm.substring(0, prefixLength);

      // we don't look at scores here:
      List<TermAndScore>[] expected = new List[3];
      for(int ed=0;ed<3;ed++) {
        expected[ed] = new ArrayList<TermAndScore>();
      }
      for(String term : terms) {
        if (term.startsWith(queryPrefix) == false) {
          continue;
        }
        int ed = getDistance(term, queryTerm);
        float score = 1f - (float) ed / (float) Math.min(queryTerm.length(), term.length());
        while (ed < 3) {
          expected[ed].add(new TermAndScore(term, score));
          ed++;
        }
      }

      for(int ed=0;ed<3;ed++) {
        Collections.sort(expected[ed]);
        int queueSize = TestUtil.nextInt(random(), 1, terms.size());
        /*
        System.out.println("\nTEST: query=" + queryTerm + " ed=" + ed + " queueSize=" + queueSize + " vs expected match size=" + expected[ed].size() + " prefixLength=" + prefixLength);
        for(TermAndScore ent : expected[ed]) {
          System.out.println("  " + ent);
        }
        */
        FuzzyQuery query = new FuzzyQuery(new Term("field", queryTerm), ed, prefixLength, queueSize, true);
        TopDocs hits = s.search(query, terms.size());
        Set<String> actual = new HashSet<>();
        for(ScoreDoc hit : hits.scoreDocs) {
          Document doc = s.doc(hit.doc);
          actual.add(doc.get("field"));
          //System.out.println("   actual: " + doc.get("field") + " score=" + hit.score);
        }
        Set<String> expectedTop = new HashSet<>();
        int limit = Math.min(queueSize, expected[ed].size());
        for(int i=0;i<limit;i++) {
          expectedTop.add(expected[ed].get(i).term);
        }
        
        if (actual.equals(expectedTop) == false) {
          StringBuilder sb = new StringBuilder();
          sb.append("FAILED: query=" + queryTerm + " ed=" + ed + " queueSize=" + queueSize + " vs expected match size=" + expected[ed].size() + " prefixLength=" + prefixLength + "\n");

          boolean first = true;
          for(String term : actual) {
            if (expectedTop.contains(term) == false) {
              if (first) {
                sb.append("  these matched but shouldn't:\n");
                first = false;
              }
              sb.append("    " + term + "\n");
            }
          }
          first = true;
          for(String term : expectedTop) {
            if (actual.contains(term) == false) {
              if (first) {
                sb.append("  these did not match but should:\n");
                first = false;
              }
              sb.append("    " + term + "\n");
            }
          }
          throw new AssertionError(sb.toString());
        }
      }
    }
    
    IOUtils.close(r, dir);
  }

  private static class TermAndScore implements Comparable<TermAndScore> {
    final String term;
    final float score;
    
    public TermAndScore(String term, float score) {
      this.term = term;
      this.score = score;
    }

    @Override
    public int compareTo(TermAndScore other) {
      // higher score sorts first, and if scores are tied, lower term sorts first
      if (score > other.score) {
        return -1;
      } else if (score < other.score) {
        return 1;
      } else {
        return term.compareTo(other.term);
      }
    }

    @Override
    public String toString() {
      return term + " score=" + score;
    }
  }

  // Poached from LuceneLevenshteinDistance.java (from suggest module): it supports transpositions (treats them as ed=1, not ed=2)
  private static int getDistance(String target, String other) {
    IntsRef targetPoints;
    IntsRef otherPoints;
    int n;
    int d[][]; // cost array
    
    // NOTE: if we cared, we could 3*m space instead of m*n space, similar to 
    // what LevenshteinDistance does, except cycling thru a ring of three 
    // horizontal cost arrays... but this comparator is never actually used by 
    // DirectSpellChecker, it's only used for merging results from multiple shards 
    // in "distributed spellcheck", and it's inefficient in other ways too...

    // cheaper to do this up front once
    targetPoints = toIntsRef(target);
    otherPoints = toIntsRef(other);
    n = targetPoints.length;
    final int m = otherPoints.length;
    d = new int[n+1][m+1];
    
    if (n == 0 || m == 0) {
      if (n == m) {
        return 0;
      }
      else {
        return Math.max(n, m);
      }
    } 

    // indexes into strings s and t
    int i; // iterates through s
    int j; // iterates through t

    int t_j; // jth character of t

    int cost; // cost

    for (i = 0; i<=n; i++) {
      d[i][0] = i;
    }
    
    for (j = 0; j<=m; j++) {
      d[0][j] = j;
    }

    for (j = 1; j<=m; j++) {
      t_j = otherPoints.ints[j-1];

      for (i=1; i<=n; i++) {
        cost = targetPoints.ints[i-1]==t_j ? 0 : 1;
        // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
        d[i][j] = Math.min(Math.min(d[i-1][j]+1, d[i][j-1]+1), d[i-1][j-1]+cost);
        // transposition
        if (i > 1 && j > 1 && targetPoints.ints[i-1] == otherPoints.ints[j-2] && targetPoints.ints[i-2] == otherPoints.ints[j-1]) {
          d[i][j] = Math.min(d[i][j], d[i-2][j-2] + cost);
        }
      }
    }
    
    return d[n][m];
  }
  
  private static IntsRef toIntsRef(String s) {
    IntsRef ref = new IntsRef(s.length()); // worst case
    int utf16Len = s.length();
    for (int i = 0, cp = 0; i < utf16Len; i += Character.charCount(cp)) {
      cp = ref.ints[ref.length++] = Character.codePointAt(s, i);
    }
    return ref;
  }

  public void testVisitor() {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "blob"), 2);
    AtomicBoolean visited = new AtomicBoolean(false);
    q.visit(new QueryVisitor() {
      @Override
      public void consumeTermsMatching(Query query, String field, Supplier<ByteRunAutomaton> automaton) {
        visited.set(true);
        ByteRunAutomaton a = automaton.get();
        assertMatches(a, "blob");
        assertMatches(a, "bolb");
        assertMatches(a, "blobby");
        assertNoMatches(a, "bolbby");
      }
    });
    assertTrue(visited.get());
  }

  private static void assertMatches(ByteRunAutomaton automaton, String text) {
    BytesRef b = new BytesRef(text);
    assertTrue(automaton.run(b.bytes, b.offset, b.length));
  }

  private static void assertNoMatches(ByteRunAutomaton automaton, String text) {
    BytesRef b = new BytesRef(text);
    assertFalse(automaton.run(b.bytes, b.offset, b.length));
  }
}
