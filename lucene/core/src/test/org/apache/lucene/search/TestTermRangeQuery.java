package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.codecs.autoprefix.AutoPrefixPostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;


public class TestTermRangeQuery extends LuceneTestCase {

  private int docCount = 0;
  private Directory dir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
  }
  
  @Override
  public void tearDown() throws Exception {
    dir.close();
    super.tearDown();
  }

  public void testExclusive() throws Exception {
    Query query = TermRangeQuery.newStringRange("content", "A", "C", false, false);
    initializeIndex(new String[] {"A", "B", "C", "D"});
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("A,B,C,D, only B in range", 1, hits.length);
    reader.close();

    initializeIndex(new String[] {"A", "B", "D"});
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("A,B,D, only B in range", 1, hits.length);
    reader.close();

    addDoc("C");
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("C added, still only B in range", 1, hits.length);
    reader.close();
  }
  
  public void testInclusive() throws Exception {
    Query query = TermRangeQuery.newStringRange("content", "A", "C", true, true);

    initializeIndex(new String[]{"A", "B", "C", "D"});
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("A,B,C,D - A,B,C in range", 3, hits.length);
    reader.close();

    initializeIndex(new String[]{"A", "B", "D"});
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("A,B,D - A and B in range", 2, hits.length);
    reader.close();

    addDoc("C");
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("C added - A, B, C in range", 3, hits.length);
    reader.close();
  }
  
  public void testAllDocs() throws Exception {
    initializeIndex(new String[]{"A", "B", "C", "D"});
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    TermRangeQuery query = new TermRangeQuery("content", null, null, true, true);
    assertEquals(4, searcher.search(query, 1000).scoreDocs.length);

    query = TermRangeQuery.newStringRange("content", "", null, true, true);
    assertEquals(4, searcher.search(query, 1000).scoreDocs.length);

    query = TermRangeQuery.newStringRange("content", "", null, true, false);
    assertEquals(4, searcher.search(query, 1000).scoreDocs.length);

    // and now another one
    query = TermRangeQuery.newStringRange("content", "B", null, true, true);
    assertEquals(3, searcher.search(query, 1000).scoreDocs.length);
    reader.close();
  }

  /** This test should not be here, but it tests the fuzzy query rewrite mode (TOP_TERMS_SCORING_BOOLEAN_REWRITE)
   * with constant score and checks, that only the lower end of terms is put into the range */
  public void testTopTermsRewrite() throws Exception {
    initializeIndex(new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"});

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    TermRangeQuery query = TermRangeQuery.newStringRange("content", "B", "J", true, true);
    checkBooleanTerms(searcher, query, "B", "C", "D", "E", "F", "G", "H", "I", "J");
    
    final int savedClauseCount = BooleanQuery.getMaxClauseCount();
    try {
      BooleanQuery.setMaxClauseCount(3);
      checkBooleanTerms(searcher, query, "B", "C", "D");
    } finally {
      BooleanQuery.setMaxClauseCount(savedClauseCount);
    }
    reader.close();
  }
  
  private void checkBooleanTerms(IndexSearcher searcher, TermRangeQuery query, String... terms) throws IOException {
    query.setRewriteMethod(new MultiTermQuery.TopTermsScoringBooleanQueryRewrite(50));
    final BooleanQuery bq = (BooleanQuery) searcher.rewrite(query);
    final Set<String> allowedTerms = asSet(terms);
    assertEquals(allowedTerms.size(), bq.clauses().size());
    for (BooleanClause c : bq.clauses()) {
      assertTrue(c.getQuery() instanceof TermQuery);
      final TermQuery tq = (TermQuery) c.getQuery();
      final String term = tq.getTerm().text();
      assertTrue("invalid term: "+ term, allowedTerms.contains(term));
      allowedTerms.remove(term); // remove to fail on double terms
    }
    assertEquals(0, allowedTerms.size());
  }

  public void testEqualsHashcode() {
    Query query = TermRangeQuery.newStringRange("content", "A", "C", true, true);
    
    query.setBoost(1.0f);
    Query other = TermRangeQuery.newStringRange("content", "A", "C", true, true);
    other.setBoost(1.0f);

    assertEquals("query equals itself is true", query, query);
    assertEquals("equivalent queries are equal", query, other);
    assertEquals("hashcode must return same value when equals is true", query.hashCode(), other.hashCode());

    other.setBoost(2.0f);
    assertFalse("Different boost queries are not equal", query.equals(other));

    other = TermRangeQuery.newStringRange("notcontent", "A", "C", true, true);
    assertFalse("Different fields are not equal", query.equals(other));

    other = TermRangeQuery.newStringRange("content", "X", "C", true, true);
    assertFalse("Different lower terms are not equal", query.equals(other));

    other = TermRangeQuery.newStringRange("content", "A", "Z", true, true);
    assertFalse("Different upper terms are not equal", query.equals(other));

    query = TermRangeQuery.newStringRange("content", null, "C", true, true);
    other = TermRangeQuery.newStringRange("content", null, "C", true, true);
    assertEquals("equivalent queries with null lowerterms are equal()", query, other);
    assertEquals("hashcode must return same value when equals is true", query.hashCode(), other.hashCode());

    query = TermRangeQuery.newStringRange("content", "C", null, true, true);
    other = TermRangeQuery.newStringRange("content", "C", null, true, true);
    assertEquals("equivalent queries with null upperterms are equal()", query, other);
    assertEquals("hashcode returns same value", query.hashCode(), other.hashCode());

    query = TermRangeQuery.newStringRange("content", null, "C", true, true);
    other = TermRangeQuery.newStringRange("content", "C", null, true, true);
    assertFalse("queries with different upper and lower terms are not equal", query.equals(other));

    query = TermRangeQuery.newStringRange("content", "A", "C", false, false);
    other = TermRangeQuery.newStringRange("content", "A", "C", true, true);
    assertFalse("queries with different inclusive are not equal", query.equals(other));
  }

  private static class SingleCharAnalyzer extends Analyzer {

    private static class SingleCharTokenizer extends Tokenizer {
      char[] buffer = new char[1];
      boolean done = false;
      CharTermAttribute termAtt;
      
      public SingleCharTokenizer() {
        super();
        termAtt = addAttribute(CharTermAttribute.class);
      }

      @Override
      public boolean incrementToken() throws IOException {
        if (done)
          return false;
        else {
          int count = input.read(buffer);
          clearAttributes();
          done = true;
          if (count == 1) {
            termAtt.copyBuffer(buffer, 0, 1);
          }
          return true;
        }
      }

      @Override
      public void reset() throws IOException {
        super.reset();
        done = false;
      }
    }

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new SingleCharTokenizer());
    }
  }

  private void initializeIndex(String[] values) throws IOException {
    initializeIndex(values, new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false));
  }

  private void initializeIndex(String[] values, Analyzer analyzer) throws IOException {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(analyzer).setOpenMode(OpenMode.CREATE));
    for (int i = 0; i < values.length; i++) {
      insertDoc(writer, values[i]);
    }
    writer.close();
  }

  // shouldnt create an analyzer for every doc?
  private void addDoc(String content) throws IOException {
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false)).setOpenMode(OpenMode.APPEND));
    insertDoc(writer, content);
    writer.close();
  }

  private void insertDoc(IndexWriter writer, String content) throws IOException {
    Document doc = new Document();

    doc.add(newStringField("id", "id" + docCount, Field.Store.YES));
    doc.add(newTextField("content", content, Field.Store.NO));

    writer.addDocument(doc);
    docCount++;
  }

  // LUCENE-38
  public void testExclusiveLowerNull() throws Exception {
    Analyzer analyzer = new SingleCharAnalyzer();
    //http://issues.apache.org/jira/browse/LUCENE-38
    Query query = TermRangeQuery.newStringRange("content", null, "C",
                                 false, false);
    initializeIndex(new String[] {"A", "B", "", "C", "D"}, analyzer);
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    int numHits = searcher.search(query, 1000).totalHits;
    // When Lucene-38 is fixed, use the assert on the next line:
    assertEquals("A,B,<empty string>,C,D => A, B & <empty string> are in range", 3, numHits);
    // until Lucene-38 is fixed, use this assert:
    //assertEquals("A,B,<empty string>,C,D => A, B & <empty string> are in range", 2, hits.length());

    reader.close();
    initializeIndex(new String[] {"A", "B", "", "D"}, analyzer);
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    numHits = searcher.search(query, 1000).totalHits;
    // When Lucene-38 is fixed, use the assert on the next line:
    assertEquals("A,B,<empty string>,D => A, B & <empty string> are in range", 3, numHits);
    // until Lucene-38 is fixed, use this assert:
    //assertEquals("A,B,<empty string>,D => A, B & <empty string> are in range", 2, hits.length());
    reader.close();
    addDoc("C");
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    numHits = searcher.search(query, 1000).totalHits;
    // When Lucene-38 is fixed, use the assert on the next line:
    assertEquals("C added, still A, B & <empty string> are in range", 3, numHits);
    // until Lucene-38 is fixed, use this assert
    //assertEquals("C added, still A, B & <empty string> are in range", 2, hits.length());
    reader.close();
  }

  // LUCENE-38
  public void testInclusiveLowerNull() throws Exception {
    //http://issues.apache.org/jira/browse/LUCENE-38
    Analyzer analyzer = new SingleCharAnalyzer();
    Query query = TermRangeQuery.newStringRange("content", null, "C", true, true);
    initializeIndex(new String[]{"A", "B", "","C", "D"}, analyzer);
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    int numHits = searcher.search(query, 1000).totalHits;
    // When Lucene-38 is fixed, use the assert on the next line:
    assertEquals("A,B,<empty string>,C,D => A,B,<empty string>,C in range", 4, numHits);
    // until Lucene-38 is fixed, use this assert
    //assertEquals("A,B,<empty string>,C,D => A,B,<empty string>,C in range", 3, hits.length());
    reader.close();
    initializeIndex(new String[]{"A", "B", "", "D"}, analyzer);
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    numHits = searcher.search(query, 1000).totalHits;
    // When Lucene-38 is fixed, use the assert on the next line:
    assertEquals("A,B,<empty string>,D - A, B and <empty string> in range", 3, numHits);
    // until Lucene-38 is fixed, use this assert
    //assertEquals("A,B,<empty string>,D => A, B and <empty string> in range", 2, hits.length());
    reader.close();
    addDoc("C");
    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    numHits = searcher.search(query, 1000).totalHits;
    // When Lucene-38 is fixed, use the assert on the next line:
    assertEquals("C added => A,B,<empty string>,C in range", 4, numHits);
    // until Lucene-38 is fixed, use this assert
    //assertEquals("C added => A,B,<empty string>,C in range", 3, hits.length());
     reader.close();
  }

  /** Make sure auto prefix terms are used with TermRangeQuery */
  public void testAutoPrefixTermsKickIn() throws Exception {

    List<String> prefixes = new ArrayList<>();
    for(int i=1;i<5;i++) {
      char[] chars = new char[i];
      Arrays.fill(chars, 'a');
      prefixes.add(new String(chars));
    }

    Set<String> randomTerms = new HashSet<>();
    int numTerms = atLeast(10000);
    while (randomTerms.size() < numTerms) {
      for(String prefix : prefixes) {
        randomTerms.add(prefix + TestUtil.randomSimpleString(random()));
      }
    }

    // We make term range aa<start> - aa<end>
    char start;
    char end;

    int actualCount;
    boolean startInclusive = random().nextBoolean();
    boolean endInclusive = random().nextBoolean();
    String startTerm;
    String endTerm;

    while (true) {
      start = (char) TestUtil.nextInt(random(), 'a', 'm');
      end = (char) TestUtil.nextInt(random(), start+1, 'z');

      actualCount = 0;

      startTerm = "aa" + start;
      endTerm = "aa" + end;

      for(String term : randomTerms) {
        int cmpStart = startTerm.compareTo(term);
        int cmpEnd = endTerm.compareTo(term);
        if ((cmpStart < 0 || (startInclusive && cmpStart == 0)) &&
            (cmpEnd > 0 || (endInclusive && cmpEnd == 0))) {
          actualCount++;
        }
      }

      if (actualCount > 2000) {
        break;
      }
    }

    if (VERBOSE) {
      System.out.println("start " + startTerm + " inclusive? " + startInclusive);
      System.out.println("end " + endTerm + " inclusive? " + endInclusive);
      System.out.println("actual count " + actualCount);
    }

    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
    int minTermsInBlock = TestUtil.nextInt(random(), 2, 100);
    int maxTermsInBlock = Math.max(2, (minTermsInBlock-1)*2 + random().nextInt(100));

    int minTermsAutoPrefix = TestUtil.nextInt(random(), 2, 100);
    int maxTermsAutoPrefix = random().nextBoolean() ? Math.max(2, (minTermsAutoPrefix-1)*2 + random().nextInt(100)) : Integer.MAX_VALUE;

    if (VERBOSE) {
      System.out.println("minTermsAutoPrefix " + minTermsAutoPrefix);
      System.out.println("maxTermsAutoPrefix " + maxTermsAutoPrefix);
    }

    iwc.setCodec(TestUtil.alwaysPostingsFormat(new AutoPrefixPostingsFormat(minTermsInBlock, maxTermsInBlock,
                                                                            minTermsAutoPrefix, maxTermsAutoPrefix)));
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    if (VERBOSE) {
      System.out.println("TEST: index terms");
    }
    for (String term : randomTerms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.NO));
      w.addDocument(doc);
      if (VERBOSE) {
        System.out.println("  " + term);
      }
    }

    if (VERBOSE) {
      System.out.println("TEST: now force merge");
    }

    w.forceMerge(1);
    IndexReader r = w.getReader();
    final Terms terms = MultiFields.getTerms(r, "field");
    IndexSearcher s = new IndexSearcher(r);
    final int finalActualCount = actualCount;
    if (VERBOSE) {
      System.out.println("start=" + startTerm + " end=" + endTerm + " startIncl=" + startInclusive + " endIncl=" + endInclusive);
    }
    TermRangeQuery q = new TermRangeQuery("field", new BytesRef(startTerm), new BytesRef(endTerm), startInclusive, endInclusive) {
      public TermRangeQuery checkTerms() throws IOException {
        TermsEnum termsEnum = getTermsEnum(terms, new AttributeSource());
        int count = 0;
        while (termsEnum.next() != null) {
          if (VERBOSE) {
            System.out.println("got term: " + termsEnum.term().utf8ToString());
          }
          count++;
        }
        if (VERBOSE) {
          System.out.println("count " + count + " vs finalActualCount=" + finalActualCount);
        }

        // Auto-prefix term(s) should have kicked in, so we should have visited fewer than the total number of aa* terms:
        assertTrue(count < finalActualCount);

        return this;
      }
    }.checkTerms();

    int maxClauseCount = BooleanQuery.getMaxClauseCount();

    try {

      if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_REWRITE);
        BooleanQuery.setMaxClauseCount(actualCount);
      } else if (random().nextBoolean()) {
        q.setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
        BooleanQuery.setMaxClauseCount(actualCount);
      }

      if (VERBOSE) {
        System.out.println("TEST: use rewrite method " + q.getRewriteMethod());
      }
      assertEquals(actualCount, s.search(q, 1).totalHits);
    } finally {
      BooleanQuery.setMaxClauseCount(maxClauseCount);
    }

    // Test when min == max:
    List<String> randomTermsList = new ArrayList<>(randomTerms);
    for(int iter=0;iter<100*RANDOM_MULTIPLIER;iter++) {
      String term = randomTermsList.get(random().nextInt(randomTermsList.size()));
      q = new TermRangeQuery("field", new BytesRef(term), new BytesRef(term), true, true);
      assertEquals(1, s.search(q, 1).totalHits);
    }

    r.close();
    w.close();
    dir.close();
  }
}
