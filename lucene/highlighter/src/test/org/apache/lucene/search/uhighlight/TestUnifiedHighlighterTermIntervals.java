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
package org.apache.lucene.search.uhighlight;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.HighlightFlag;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public class TestUnifiedHighlighterTermIntervals extends LuceneTestCase {

  private final FieldType fieldType; // for "body" generally, but not necessarily others. See constructor

  private MockAnalyzer indexAnalyzer;
  private Directory dir;

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    return UHTestHelper.parametersFactoryList();
  }

  public TestUnifiedHighlighterTermIntervals(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  @Before
  public void doBefore() throws IOException {
    indexAnalyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase
    dir = newDirectory();
  }

  @After
  public void doAfter() throws IOException {
    dir.close();
  }

  static UnifiedHighlighter randomUnifiedHighlighter(IndexSearcher searcher, Analyzer indexAnalyzer) {
    return randomUnifiedHighlighter(searcher, indexAnalyzer, EnumSet.noneOf(HighlightFlag.class), null);
  }

  static UnifiedHighlighter randomUnifiedHighlighter(IndexSearcher searcher, Analyzer indexAnalyzer,
                                                     EnumSet<HighlightFlag> mandatoryFlags, Boolean requireFieldMatch) {
    final UnifiedHighlighter uh = new UnifiedHighlighter(searcher, indexAnalyzer) {
      Set<HighlightFlag> flags; // consistently random set of flags for this test run
      @Override
      protected Set<HighlightFlag> getFlags(String field) {
        if (flags != null) {
          return flags;
        }
        final EnumSet<HighlightFlag> result = EnumSet.copyOf(mandatoryFlags);
        int r = random().nextInt();
        for (HighlightFlag highlightFlag : HighlightFlag.values()) {
          if (((1 << highlightFlag.ordinal()) & r) == 0) {
            result.add(highlightFlag);
          }
        }
        if (result.contains(HighlightFlag.WEIGHT_MATCHES)) {
          // these two are required for WEIGHT_MATCHES
          result.add(HighlightFlag.MULTI_TERM_QUERY);
          result.add(HighlightFlag.PHRASES);
        }
        return flags = result;
      }
    };
    uh.setCacheFieldValCharsThreshold(random().nextInt(100));
    if (requireFieldMatch == Boolean.FALSE || (requireFieldMatch == null && random().nextBoolean())) {
      uh.setFieldMatcher(f -> true); // requireFieldMatch==false
    }
    return uh;

  }

  //
  //  Tests below were ported from the PostingsHighlighter. Possibly augmented.  Far below are newer tests.
  //

  public void testBasics() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);
    ir.close();
  }

  public void testFormatWithMatchExceedingContentLength2() throws Exception {

    String bodyText = "123 TEST 01234 TEST";

    String[] snippets = formatWithMatchExceedingContentLength(bodyText);

    assertEquals(1, snippets.length);
    assertEquals("123 <b>TEST</b> 01234 TE", snippets[0]);
  }

  public void testFormatWithMatchExceedingContentLength3() throws Exception {

    String bodyText = "123 5678 01234 TEST TEST";

    String[] snippets = formatWithMatchExceedingContentLength(bodyText);

    assertEquals(1, snippets.length);
    assertEquals("123 5678 01234 TE", snippets[0]);
  }

  public void testFormatWithMatchExceedingContentLength() throws Exception {

    String bodyText = "123 5678 01234 TEST";

    String[] snippets = formatWithMatchExceedingContentLength(bodyText);

    assertEquals(1, snippets.length);
    // LUCENE-5166: no snippet
    assertEquals("123 5678 01234 TE", snippets[0]);
  }

  private String[] formatWithMatchExceedingContentLength(String bodyText) throws IOException {

    int maxLength = 17;

    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    final Field body = new Field("body", bodyText, fieldType);

    Document doc = new Document();
    doc.add(body);

    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);

    Query query = new IntervalQuery("body", Intervals.term("test"));

    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);

    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxLength(maxLength);
    String snippets[] = highlighter.highlight("body", query, topDocs);

    ir.close();
    return snippets;
  }

  // simple test highlighting last word.
  public void testHighlightLastWord() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.term("test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(1, snippets.length);
    assertEquals("This is a <b>test</b>", snippets[0]);

    ir.close();
  }

  // simple test with one sentence documents.
  public void testOneSentence() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.");
    iw.addDocument(doc);
    body.setStringValue("Test a one sentence document.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.term("test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>.", snippets[0]);
    assertEquals("<b>Test</b> a one sentence document.", snippets[1]);
    ir.close();
  }

  // simple test with multiple values that make a result longer than maxLength.
  public void testMaxLengthWithMultivalue() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();

    final String value = "This is a multivalued field. Sentencetwo field.";
    doc.add(new Field("body", value, fieldType));
    doc.add(new Field("body", value, fieldType));
    doc.add(new Field("body", value, fieldType));

    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxLength(value.length() * 2 + 1);
    Query query = new IntervalQuery("body", Intervals.term("field"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs, 10);
    assertEquals(1, snippets.length);
    String highlightedValue = "This is a multivalued <b>field</b>. Sentencetwo <b>field</b>.";
    assertEquals(highlightedValue + "... " + highlightedValue, snippets[0]);
    ir.close();
  }

  public void testMultipleTerms() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.or(
        Intervals.term("highlighting"),
        Intervals.term("just"),
        Intervals.term("first")));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(2, snippets.length);
    assertEquals("<b>Just</b> a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the <b>first</b> term. ", snippets[1]);
    ir.close();
  }

  public void testMultiplePassages() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("This test is another test. Not a good sentence. Test test test test.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.term("test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(2, snippets.length);
    assertEquals("This is a <b>test</b>. Just a <b>test</b> highlighting from postings. ", snippets[0]);
    assertEquals("This <b>test</b> is another <b>test</b>. ... <b>Test</b> <b>test</b> <b>test</b> <b>test</b>.", snippets[1]);
    ir.close();
  }

  public void testBuddhism() throws Exception {
    String text = "This eight-volume set brings together seminal papers in Buddhist studies from a vast " +
        "range of academic disciplines published over the last forty years. With a new introduction " +
        "by the editor, this collection is a unique and unrivalled research resource for both " +
        "student and scholar. Coverage includes: - Buddhist origins; early history of Buddhism in " +
        "South and Southeast Asia - early Buddhist Schools and Doctrinal History; Theravada Doctrine " +
        "- the Origins and nature of Mahayana Buddhism; some Mahayana religious topics - Abhidharma " +
        "and Madhyamaka - Yogacara, the Epistemological tradition, and Tathagatagarbha - Tantric " +
        "Buddhism (Including China and Japan); Buddhism in Nepal and Tibet - Buddhism in South and " +
        "Southeast Asia, and - Buddhism in China, East Asia, and Japan.";
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", text, fieldType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    Query query = new IntervalQuery("body", Intervals.phrase("buddhist", "origins"));
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits.value);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setHighlightPhrasesStrictly(false);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
      //  highlighter.getFlags("body").containsAll(EnumSet.of(HighlightFlag.WEIGHT_MATCHES, HighlightFlag.PHRASES))) {
      //assertTrue(snippets[0] + " " + query, snippets[0].contains("<b>Buddhist origins</b>"));
    assertTrue(snippets[0], snippets[0].contains("<b>Buddhist</b> <b>origins</b>"));
    ir.close();
  }

  public void testCuriousGeorge() throws Exception {
    String text = "It’s the formula for success for preschoolers—Curious George and fire trucks! " +
        "Curious George and the Firefighters is a story based on H. A. and Margret Rey’s " +
        "popular primate and painted in the original watercolor and charcoal style. " +
        "Firefighters are a famously brave lot, but can they withstand a visit from one curious monkey?";
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", text, fieldType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    Query query = new IntervalQuery("body", Intervals.phrase("curious", "george"));
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits.value);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setHighlightPhrasesStrictly(false);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
    assertFalse(snippets[0].contains("<b>Curious</b>Curious"));
    int matches = 0;
    final String snippet = "<b>Curious((</b> <b>)| )?George</b>";
    for (Matcher m = Pattern.compile(snippet).matcher(snippets[0]); m.find();) {
      matches++;
    }
    assertEquals(query +" is looking for "+snippet+" at "+snippets[0], 2, matches);
    ir.close();
  }
  
  public void testCambridgeMA() throws Exception {
    BufferedReader r = new BufferedReader(new InputStreamReader(
        this.getClass().getResourceAsStream("CambridgeMA.utf8"), StandardCharsets.UTF_8));
    String text = r.readLine();
    r.close();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    Field body = new Field("body", text, fieldType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    try {
    iw.close();
    IndexSearcher searcher = newSearcher(ir);
    Query query =  new IntervalQuery("body",
             Intervals.unordered(Intervals.term("porter"),
                 Intervals.term("square"),
                 Intervals.term("massachusetts")));
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits.value);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxLength(Integer.MAX_VALUE - 1);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
    assertTrue(snippets[0].contains("<b>Square</b>"));
    assertTrue(snippets[0].contains("<b>Porter</b>"));
    } finally{ir.close();}
  }

  public void testPassageRanking() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body",Intervals.term("test")) ;
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
    assertEquals(
        "This is a <b>test</b>.  ... Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.",
        snippets[0]);
    ir.close();
  }

  public void testBooleanMustNot() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "This sentence has both terms.  This sentence has only terms.", fieldType);
    Document document = new Document();
    document.add(body);
    iw.addDocument(document);
    IndexReader ir = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(ir);

    Query query = new IntervalQuery("body", Intervals.notContaining(Intervals.term("terms"), 
                Intervals.term("both")));
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(1, topDocs.totalHits.value);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxLength(Integer.MAX_VALUE - 1);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
    assertFalse(snippets[0].contains("<b>both</b>"));
    assertTrue(snippets[0].contains("<b>terms</b>"));
    ir.close();
  }

  public void testHighlightAllText() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected BreakIterator getBreakIterator(String field) {
        return new WholeBreakIterator();
      }
    };
    highlighter.setMaxLength(10000);
    Query query = new IntervalQuery("body", Intervals.term("test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
    assertEquals(
        "This is a <b>test</b>.  Just highlighting from postings. This is also a much sillier <b>test</b>.  Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.",
        snippets[0]);
    ir.close();
  }

  public void testSpecificDocIDs() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);
    body.setStringValue("Highlighting the first term. Hope it works.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(2, topDocs.totalHits.value);
    ScoreDoc[] hits = topDocs.scoreDocs;
    int[] docIDs = new int[2];
    docIDs[0] = hits[0].doc;
    docIDs[1] = hits[1].doc;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, docIDs, new int[] {1}).get("body");
    assertEquals(2, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);
    assertEquals("<b>Highlighting</b> the first term. ", snippets[1]);
    ir.close();
  }

  public void testCustomFieldValueSource() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();

    final String text = "This is a test.  Just highlighting from postings. This is also a much sillier test.  Feel free to test test test test test test test.";
    Field body = new Field("body", text, fieldType);
    doc.add(body);
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);

    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected List<CharSequence[]> loadFieldValues(String[] fields,
                                                     DocIdSetIterator docIter, int cacheCharsThreshold) throws IOException {
        assert fields.length == 1;
        assert docIter.cost() == 1;
        docIter.nextDoc();
        return Collections.singletonList(new CharSequence[]{text});
      }

      @Override
      protected BreakIterator getBreakIterator(String field) {
        return new WholeBreakIterator();
      }
    };

    Query query = new IntervalQuery("body", Intervals.term("test"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs, 2);
    assertEquals(1, snippets.length);
    assertEquals(
        "This is a <b>test</b>.  Just highlighting from postings. This is also a much sillier <b>test</b>.  Feel free to <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b> <b>test</b>.",
        snippets[0]);
    ir.close();
  }

  /**
   * Make sure highlighter returns first N sentences if
   * there were no hits.
   */
  public void testEmptyHighlights() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", fieldType);
    doc.add(body);
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, docIDs, new int[] {2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals("test this is.  another sentence this test has.  ", snippets[0]);
    ir.close();
  }

  /**
   * Not empty but nothing analyzes. Ensures we address null term-vectors.
   */
  public void testNothingAnalyzes() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();
    doc.add(new Field("body", " ", fieldType));// just a space! (thus not empty)
    doc.add(newTextField("id", "id", Field.Store.YES));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("body", "something", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    int[] docIDs = new int[1];
    docIDs[0] = docID;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, docIDs, new int[] {2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals(" ", snippets[0]);
    ir.close();
  }

  /**
   * Make sure highlighter we can customize how emtpy
   * highlight is returned.
   */
  public void testCustomEmptyHighlights() throws Exception {
    indexAnalyzer.setPositionIncrementGap(10);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", fieldType);
    doc.add(body);
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setMaxNoHighlightPassages(0);// don't want any default summary
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    int[] docIDs = new int[]{0};
    String snippets[] = highlighter.highlightFields(new String[]{"body"}, query, docIDs, new int[]{2}).get("body");
    assertEquals(1, snippets.length);
    assertNull(snippets[0]);
    ir.close();
  }

  /**
   * Make sure highlighter returns whole text when there
   * are no hits and BreakIterator is null.
   */
  public void testEmptyHighlightsWhole() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", fieldType);
    doc.add(body);
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected BreakIterator getBreakIterator(String field) {
        return new WholeBreakIterator();
      }
    };
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, docIDs, new int[] {2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals("test this is.  another sentence this test has.  far away is that planet.", snippets[0]);
    ir.close();
  }

  /**
   * Make sure highlighter is OK with entirely missing
   * field.
   */
  public void testFieldIsMissing() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();

    Field body = new Field("body", "test this is.  another sentence this test has.  far away is that planet.", fieldType);
    doc.add(body);
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("bogus", Intervals.term("highlighting"));
    int[] docIDs = new int[] {0};
    String snippets[] = highlighter.highlightFields(new String[] {"bogus"}, query, docIDs, new int[] {2}).get("bogus");
    assertEquals(1, snippets.length);
    assertNull(snippets[0]);
    ir.close();
  }

  public void testFieldIsJustSpace() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();
    doc.add(new Field("body", "   ", fieldType));
    doc.add(newTextField("id", "id", Field.Store.YES));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("body", "something", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    int[] docIDs = new int[1];
    docIDs[0] = docID;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, docIDs, new int[] {2}).get("body");
    assertEquals(1, snippets.length);
    assertEquals("   ", snippets[0]);
    ir.close();
  }

  public void testFieldIsEmptyString() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Document doc = new Document();
    doc.add(new Field("body", "", fieldType));
    doc.add(newTextField("id", "id", Field.Store.YES));
    iw.addDocument(doc);

    doc = new Document();
    doc.add(new Field("body", "something", fieldType));
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    int docID = searcher.search(new TermQuery(new Term("id", "id")), 1).scoreDocs[0].doc;

    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    int[] docIDs = new int[1];
    docIDs[0] = docID;
    String snippets[] = highlighter.highlightFields(new String[] {"body"}, query, docIDs, new int[] {2}).get("body");
    assertEquals(1, snippets.length);
    assertNull(snippets[0]);
    ir.close();
  }

  public void testMultipleDocs() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    int numDocs = atLeast(100);
    for (int i = 0; i < numDocs; i++) {
      Document doc = new Document();
      String content = "the answer is " + i;
      if ((i & 1) == 0) {
        content += " some more terms";
      }
      doc.add(new Field("body", content, fieldType));
      doc.add(newStringField("id", "" + i, Field.Store.YES));
      iw.addDocument(doc);

      if (random().nextInt(10) == 2) {
        iw.commit();
      }
    }

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, indexAnalyzer);
    highlighter.setCacheFieldValCharsThreshold(random().nextInt(10) * 10);// 0 thru 90 intervals of 10
    Query query = new IntervalQuery("body", Intervals.term("answer"));
    TopDocs hits = searcher.search(query, numDocs);
    assertEquals(numDocs, hits.totalHits.value);

    String snippets[] = highlighter.highlight("body", query, hits);
    assertEquals(numDocs, snippets.length);
    for (int hit = 0; hit < numDocs; hit++) {
      Document doc = searcher.doc(hits.scoreDocs[hit].doc);
      int id = Integer.parseInt(doc.get("id"));
      String expected = "the <b>answer</b> is " + id;
      if ((id & 1) == 0) {
        expected += " some more terms";
      }
      assertEquals(expected, snippets[hit]);
    }
    ir.close();
  }

  public void testEncode() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from <i>postings</i>. Feel free to ignore.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected PassageFormatter getFormatter(String field) {
        return new DefaultPassageFormatter("<b>", "</b>", "... ", true);
      }
    };
    
    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(1, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from &lt;i&gt;postings&lt;&#x2F;i&gt;. ", snippets[0]);
    ir.close();
  }

  // LUCENE-4906
  public void testObjectFormatter() throws Exception {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer) {
      @Override
      protected PassageFormatter getFormatter(String field) {
        return new PassageFormatter() {
          PassageFormatter defaultFormatter = new DefaultPassageFormatter();

          @Override
          public String[] format(Passage passages[], String content) {
            // Just turns the String snippet into a length 2
            // array of String
            return new String[]{"blah blah", defaultFormatter.format(passages, content).toString()};
          }
        };
      }
    };

    Query query = new IntervalQuery("body", Intervals.term("highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    int[] docIDs = new int[1];
    docIDs[0] = topDocs.scoreDocs[0].doc;
    Map<String,Object[]> snippets = highlighter.highlightFieldsAsObjects(new String[] {"body"}, query, docIDs,
        new int[] {1});
    Object[] bodySnippets = snippets.get("body");
    assertEquals(1, bodySnippets.length);
    assertTrue(Arrays.equals(new String[] {"blah blah", "Just a test <b>highlighting</b> from postings. "},
        (String[]) bodySnippets[0]));
    ir.close();
  }

  private IndexReader indexSomeFields() throws IOException {
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);
    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.NONE);
    ft.setTokenized(false);
    ft.setStored(true);
    ft.freeze();

    Field title = new Field("title", "", fieldType);
    Field text = new Field("text", "", fieldType);
    Field category = new Field("category", "", fieldType);

    Document doc = new Document();
    doc.add(title);
    doc.add(text);
    doc.add(category);
    title.setStringValue("This is the title field.");
    text.setStringValue("This is the text field. You can put some text if you want.");
    category.setStringValue("This is the category field.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();
    return ir;
  }

  public void testMatchesSlopBug() throws IOException {
    IndexReader ir = indexSomeFields();
    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
    Query query = new IntervalQuery("title", Intervals.maxgaps(random().nextBoolean() ? 1 : 2,
        Intervals.ordered(
            Intervals.term("this"), Intervals.term("is"), Intervals.term("the"), Intervals.term("field"))));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String[] snippets = highlighter.highlight("title", query, topDocs, 10);
    assertEquals(1, snippets.length);
    if (highlighter.getFlags("title").contains(HighlightFlag.WEIGHT_MATCHES)) {
      assertEquals("" + highlighter.getFlags("title"),
          "<b>This is the title field</b>.", snippets[0]);
    } else {
      assertEquals("" + highlighter.getFlags("title"),
          "<b>This</b> <b>is</b> <b>the</b> title <b>field</b>.", snippets[0]);
    }
    ir.close();
  }

  public void testNotReanalyzed() throws Exception {
    if (fieldType == UHTestHelper.reanalysisType) {
      return; // we're testing the *other* cases
    }

    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, indexAnalyzer);

    Field body = new Field("body", "", fieldType);
    Document doc = new Document();
    doc.add(body);

    body.setStringValue("This is a test. Just a test highlighting from postings. Feel free to ignore.");
    iw.addDocument(doc);

    IndexReader ir = iw.getReader();
    iw.close();

    IndexSearcher searcher = newSearcher(ir);
    UnifiedHighlighter highlighter = randomUnifiedHighlighter(searcher, new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        throw new AssertionError("shouldn't be called");
      }
    });
    Query query =  new IntervalQuery("body", Intervals.term("highlighting"));
    TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
    assertEquals(1, topDocs.totalHits.value);
    String snippets[] = highlighter.highlight("body", query, topDocs);
    assertEquals(1, snippets.length);
    assertEquals("Just a test <b>highlighting</b> from postings. ", snippets[0]);

    ir.close();
  }
}
