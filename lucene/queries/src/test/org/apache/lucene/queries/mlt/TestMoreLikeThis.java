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
package org.apache.lucene.queries.mlt;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;

import static org.hamcrest.core.Is.is;

public class TestMoreLikeThis extends LuceneTestCase {

  private static final String SHOP_TYPE = "type";
  private static final String FOR_SALE = "weSell";
  private static final String NOT_FOR_SALE = "weDontSell";
  private static final int MIN_DOC_FREQ = 0;
  private static final int MIN_WORD_LEN = 1;
  private static final int MIN_TERM_FREQ = 1;

  private Directory directory;
  private IndexReader reader;
  private IndexSearcher searcher;
  private MoreLikeThis mlt;
  private Analyzer analyzer;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    
    // Add series of docs with specific information for MoreLikeThis
    addDoc(writer, "text", "lucene");
    addDoc(writer, "text", "lucene release");
    addDoc(writer, "text", "apache");
    addDoc(writer, "text", "apache lucene");

    // one more time to increase the doc frequencies
    addDoc(writer, "text","lucene2");
    addDoc(writer, "text", "lucene2 release2");
    addDoc(writer, "text", "apache2");
    addDoc(writer, "text", "apache2 lucene2");

    addDoc(writer, "text2","lucene2");
    addDoc(writer, "text2", "lucene2 release2");
    addDoc(writer, "text2", "apache2");
    addDoc(writer, "text2", "apache2 lucene2");

    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(reader);
    analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt = this.getDefaultMoreLikeThis(reader);
  }

  private MoreLikeThis getDefaultMoreLikeThis(IndexReader reader) {
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMinDocFreq(MIN_DOC_FREQ);
    mlt.setMinTermFreq(MIN_TERM_FREQ);
    mlt.setMinWordLen(MIN_WORD_LEN);
    return mlt;
  }
  
  
  @Override
  public void tearDown() throws Exception {
    analyzer.close();
    reader.close();
    directory.close();
    super.tearDown();
  }
  
  private void addDoc(RandomIndexWriter writer, String fieldName, String text) throws IOException {
    Document doc = new Document();
    doc.add(newTextField(fieldName, text, Field.Store.YES));
    writer.addDocument(doc);
  }

  private void addDoc(RandomIndexWriter writer, String fieldName, String[] texts) throws IOException {
    Document doc = new Document();
    for (String text : texts) {
      doc.add(newTextField(fieldName, text, Field.Store.YES));
    }
    writer.addDocument(doc);
  }

  public void testSmallSampleFromCorpus() throws Throwable {
    // add series of docs with terms of decreasing df
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < 1980; i++) {
      Document doc = new Document();
      doc.add(newTextField("text", "filler", Field.Store.YES));
      writer.addDocument(doc);
    }
    for (int i = 0; i < 18; i++) {
      Document doc = new Document();
      doc.add(newTextField("one_percent", "all", Field.Store.YES));
      writer.addDocument(doc);
    }
    for (int i = 0; i < 2; i++) {
      Document doc = new Document();
      doc.add(newTextField("one_percent", "all", Field.Store.YES));
      doc.add(newTextField("one_percent", "tenth", Field.Store.YES));
      writer.addDocument(doc);
    }
    IndexReader reader = writer.getReader();
    writer.close();

    // setup MLT query
    MoreLikeThis mlt = new MoreLikeThis(reader);
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    mlt.setAnalyzer(analyzer);
    mlt.setMaxQueryTerms(3);
    mlt.setMinDocFreq(1);
    mlt.setMinTermFreq(1);
    mlt.setMinWordLen(1);
    mlt.setFieldNames(new String[]{"one_percent"});

    BooleanQuery query = (BooleanQuery) mlt.like("one_percent", new StringReader("tenth tenth all"));
    Collection<BooleanClause> clauses = query.clauses();

    assertTrue(clauses.size() == 2);
    Term term = ((TermQuery) ((List<BooleanClause>) clauses).get(0).getQuery()).getTerm();
    assertTrue(term.text().equals("all"));
    term = ((TermQuery) ((List<BooleanClause>) clauses).get(1).getQuery()).getTerm();
    assertTrue(term.text().equals("tenth"));


    query = (BooleanQuery) mlt.like("one_percent", new StringReader("tenth all all"));
    clauses = query.clauses();

    assertTrue(clauses.size() == 2);
    term = ((TermQuery) ((List<BooleanClause>) clauses).get(0).getQuery()).getTerm();
    assertTrue(term.text().equals("all"));
    term = ((TermQuery) ((List<BooleanClause>) clauses).get(1).getQuery()).getTerm();
    assertTrue(term.text().equals("tenth"));

    // clean up
    reader.close();
    dir.close();
    analyzer.close();
  }

  public void testBoostFactor() throws Throwable {
    Map<String,Float> originalValues = getOriginalValues();
    mlt.setFieldNames(new String[] {"text"});
    mlt.setBoost(true);
    
    // this mean that every term boost factor will be multiplied by this
    // number
    float boostFactor = 5;
    mlt.setBoostFactor(boostFactor);
    
    BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(
        "lucene release"));
    Collection<BooleanClause> clauses = query.clauses();
    
    assertEquals("Expected " + originalValues.size() + " clauses.",
        originalValues.size(), clauses.size());

    for (BooleanClause clause : clauses) {
      BoostQuery bq = (BoostQuery) clause.getQuery();
      TermQuery tq = (TermQuery) bq.getQuery();
      Float termBoost = originalValues.get(tq.getTerm().text());
      assertNotNull("Expected term " + tq.getTerm().text(), termBoost);

      float totalBoost = termBoost * boostFactor;
      assertEquals("Expected boost of " + totalBoost + " for term '"
          + tq.getTerm().text() + "' got " + bq.getBoost(), totalBoost, bq
          .getBoost(), 0.0001);
    }
  }
  
  private Map<String,Float> getOriginalValues() throws IOException {
    Map<String,Float> originalValues = new HashMap<>();
    mlt.setFieldNames(new String[] {"text"});
    mlt.setBoost(true);
    BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(
        "lucene release"));
    Collection<BooleanClause> clauses = query.clauses();

    for (BooleanClause clause : clauses) {
      BoostQuery bq = (BoostQuery) clause.getQuery();
      TermQuery tq = (TermQuery) bq.getQuery();
      originalValues.put(tq.getTerm().text(), bq.getBoost());
    }
    return originalValues;
  }
  
  // LUCENE-3326
  public void testMultiFields() throws Exception {
    mlt.setFieldNames(new String[] {"text", "foobar"});
    mlt.like("foobar", new StringReader("this is a test"));
  }

  // LUCENE-5725
  public void testMultiValues() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.KEYWORD, false);
    mlt.setAnalyzer(analyzer);
    mlt.setFieldNames(new String[] {"text"});
    
    BooleanQuery query = (BooleanQuery) mlt.like("text",
        new StringReader("lucene"), new StringReader("lucene release"),
        new StringReader("apache"), new StringReader("apache lucene"));
    Collection<BooleanClause> clauses = query.clauses();
    assertEquals("Expected 2 clauses only!", 2, clauses.size());
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertTrue(Arrays.asList(new Term("text", "lucene"), new Term("text", "apache")).contains(term));
    }
  }

  public void testSeedDocumentMap_minTermFrequencySet_shouldBuildQueryAccordingToCorrectTermFrequencies() throws Exception {
    mlt.setMinTermFreq(3);

    String mltField1 = "text";
    String mltField2 = "text2";
    mlt.setFieldNames(new String[]{mltField1, mltField2});

    Map<String, Collection<Object>> seedDocument = new HashMap<>();
    String textValue = "apache apache lucene lucene lucene";
    seedDocument.put(mltField1, Arrays.asList(textValue));
    seedDocument.put(mltField2, Arrays.asList(textValue));

    BooleanQuery query = (BooleanQuery) mlt.like(seedDocument);
    Collection<BooleanClause> clauses = query.clauses();
    assertEquals("Expected 1 clauses only!", 1, clauses.size());
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertThat(term, is(new Term(mltField1, "lucene")));
    }
    analyzer.close();
  }

  public void testSeedDocumentMap_minTermFrequencySetMltFieldSet_shouldBuildQueryAccordingToCorrectTermFrequenciesAndField() throws Exception {
    mlt.setMinTermFreq(3);

    String mltField = "text";
    mlt.setFieldNames(new String[]{mltField});

    Map<String, Collection<Object>> seedDocument = new HashMap<>();
    String sampleField2 = "text2";
    String textValue1 = "apache apache lucene lucene";
    String textValue2 = "apache2 apache2 lucene2 lucene2 lucene2";
    seedDocument.put(mltField, Arrays.asList(textValue1));
    seedDocument.put(sampleField2, Arrays.asList(textValue2));

    BooleanQuery query = (BooleanQuery) mlt.like(seedDocument);
    Collection<BooleanClause> clauses = query.clauses();

    HashSet<Term> unexpectedTerms = new HashSet<>();
    unexpectedTerms.add(new Term(mltField, "apache"));//Term Frequency < Minimum Accepted Term Frequency
    unexpectedTerms.add(new Term(mltField, "lucene"));//Term Frequency < Minimum Accepted Term Frequency
    unexpectedTerms.add(new Term(mltField, "apache2"));//Term Frequency < Minimum Accepted Term Frequency
    unexpectedTerms.add(new Term(mltField, "lucene2"));//Wrong Field

    //None of the Not Expected terms is in the query
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertFalse("Unexpected term '" + term + "' found in query terms", unexpectedTerms.contains(term));
    }

    assertEquals("Expected 0 clauses only!", 0, clauses.size());

    analyzer.close();
  }

  public void testSeedDocumentMap_queryFieldsSet_shouldBuildQueryFromSpecifiedFieldnamesOnly() throws Exception {
    mlt.setMinTermFreq(2);

    String mltField = "text";

    mlt.setFieldNames(new String[]{mltField});

    Map<String, Collection<Object>> seedDocument = new HashMap<>();
    String notMltField = "text2";
    String textValue1 = "apache apache lucene lucene";
    String textValue2 = "apache2 apache2 lucene2 lucene2 lucene2";
    seedDocument.put(mltField, Arrays.asList(textValue1));
    seedDocument.put(notMltField, Arrays.asList(textValue2));

    HashSet<Term> expectedTerms = new HashSet<>();
    expectedTerms.add(new Term(mltField, "apache"));
    expectedTerms.add(new Term(mltField, "lucene"));

    HashSet<Term> unexpectedTerms = new HashSet<>();
    unexpectedTerms.add(new Term(mltField, "apache2"));
    unexpectedTerms.add(new Term(mltField, "lucene2"));
    unexpectedTerms.add(new Term(notMltField, "apache2"));
    unexpectedTerms.add(new Term(notMltField, "lucene2"));

    BooleanQuery query = (BooleanQuery) mlt.like(seedDocument);
    Collection<BooleanClause> clauses = query.clauses();
    HashSet<Term> clausesTerms = new HashSet<>();
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      clausesTerms.add(term);
    }

    assertEquals("Expected 2 clauses only!", 2, clauses.size());

    //None of the Not Expected terms is in the query
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertFalse("Unexpected term '" + term + "' found in query terms", unexpectedTerms.contains(term));
    }

    //All of the Expected terms are in the query
    for (Term expectedTerm : expectedTerms) {
      assertTrue("Expected term '" + expectedTerm + "' is not found in query terms", clausesTerms.contains(expectedTerm));
    }

  }

  // just basic equals/hashcode etc
  public void testMoreLikeThisQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    Query query = new MoreLikeThisQuery("this is a test", new String[] { "text" }, analyzer, "text");
    QueryUtils.check(random(), query, searcher);
    analyzer.close();
  }

  public void testTopN() throws Exception {
    int numDocs = 100;
    int topN = 25;

    // add series of docs with terms of decreasing df
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < numDocs; i++) {
      addDoc(writer, "text", generateStrSeq(0, i + 1));
    }
    IndexReader reader = writer.getReader();
    writer.close();

    // setup MLT query
    mlt = this.getDefaultMoreLikeThis(reader);
    mlt.setMaxQueryTerms(topN);
    mlt.setFieldNames(new String[]{"text"});

    // perform MLT query
    String likeText = "";
    for (String text : generateStrSeq(0, numDocs)) {
      likeText += text + " ";
    }
    BooleanQuery query = (BooleanQuery) mlt.like("text", new StringReader(likeText));

    // check best terms are topN of highest idf
    Collection<BooleanClause> clauses = query.clauses();
    assertEquals("Expected" + topN + "clauses only!", topN, clauses.size());

    Term[] expectedTerms = new Term[topN];
    int idx = 0;
    for (String text : generateStrSeq(numDocs - topN, topN)) {
      expectedTerms[idx++] = new Term("text", text);
    }
    for (BooleanClause clause : clauses) {
      Term term = ((TermQuery) clause.getQuery()).getTerm();
      assertTrue(Arrays.asList(expectedTerms).contains(term));
    }

    // clean up
    reader.close();
    dir.close();
    analyzer.close();
  }

  private String[] generateStrSeq(int from, int size) {
    String[] generatedStrings = new String[size];
    for (int i = 0; i < generatedStrings.length; i++) {
      generatedStrings[i] = String.valueOf(from + i);
    }
    return generatedStrings;
  }

  private int addShopDoc(RandomIndexWriter writer, String type, String[] weSell, String[] weDontSell) throws IOException {
    Document doc = new Document();
    doc.add(newTextField(SHOP_TYPE, type, Field.Store.YES));
    for (String item : weSell) {
      doc.add(newTextField(FOR_SALE, item, Field.Store.YES));
    }
    for (String item : weDontSell) {
      doc.add(newTextField(NOT_FOR_SALE, item, Field.Store.YES));
    }
    writer.addDocument(doc);
    return writer.getDocStats().numDocs - 1;
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-7161")
  public void testMultiFieldShouldReturnPerFieldBooleanQuery() throws Exception {
    IndexReader reader = null;
    Directory dir = newDirectory();
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
    try {
      int maxQueryTerms = 25;

      String[] itShopItemForSale = new String[]{"watch", "ipod", "asrock", "imac", "macbookpro", "monitor", "keyboard", "mouse", "speakers"};
      String[] itShopItemNotForSale = new String[]{"tie", "trousers", "shoes", "skirt", "hat"};

      String[] clothesShopItemForSale = new String[]{"tie", "trousers", "shoes", "skirt", "hat"};
      String[] clothesShopItemNotForSale = new String[]{"watch", "ipod", "asrock", "imac", "macbookpro", "monitor", "keyboard", "mouse", "speakers"};

      // add series of shop docs
      RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
      for (int i = 0; i < 300; i++) {
        addShopDoc(writer, "it", itShopItemForSale, itShopItemNotForSale);
      }
      for (int i = 0; i < 300; i++) {
        addShopDoc(writer, "clothes", clothesShopItemForSale, clothesShopItemNotForSale);
      }
      // Input Document is a clothes shop
      int inputDocId = addShopDoc(writer, "clothes", clothesShopItemForSale, clothesShopItemNotForSale);
      reader = writer.getReader();
      writer.close();

      // setup MLT query
      MoreLikeThis mlt = this.getDefaultMoreLikeThis(reader);

      mlt.setMaxQueryTerms(maxQueryTerms);
      mlt.setFieldNames(new String[]{FOR_SALE, NOT_FOR_SALE});

      // perform MLT query
      BooleanQuery query = (BooleanQuery) mlt.like(inputDocId);
      Collection<BooleanClause> clauses = query.clauses();

      Collection<BooleanClause> expectedClothesShopClauses = new ArrayList<BooleanClause>();
      for (String itemForSale : clothesShopItemForSale) {
        BooleanClause booleanClause = new BooleanClause(new TermQuery(new Term(FOR_SALE, itemForSale)), BooleanClause.Occur.SHOULD);
        expectedClothesShopClauses.add(booleanClause);
      }
      for (String itemNotForSale : clothesShopItemNotForSale) {
        BooleanClause booleanClause = new BooleanClause(new TermQuery(new Term(NOT_FOR_SALE, itemNotForSale)), BooleanClause.Occur.SHOULD);
        expectedClothesShopClauses.add(booleanClause);
      }

      for (BooleanClause expectedClause : expectedClothesShopClauses) {
        assertTrue(clauses.contains(expectedClause));
      }
    } finally {
      // clean up
      if (reader != null) {
        reader.close();
      }
      dir.close();
      analyzer.close();
    }
  }

  public void testCustomFrequecy() throws IOException {
    // define an analyzer with delimited term frequency, e.g. "foo|2 bar|3"
    Analyzer analyzer = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false, 100);
        MockTokenFilter filt = new MockTokenFilter(tokenizer, MockTokenFilter.EMPTY_STOPSET);
        return new TokenStreamComponents(tokenizer, addCustomTokenFilter(filt));
      }

      TokenStream addCustomTokenFilter(TokenStream input) {
        return new TokenFilter(input) {
          final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
          final TermFrequencyAttribute tfAtt = addAttribute(TermFrequencyAttribute.class);

          @Override
          public boolean incrementToken() throws IOException {
            if (input.incrementToken()) {
              final char[] buffer = termAtt.buffer();
              final int length = termAtt.length();
              for (int i = 0; i < length; i++) {
                if (buffer[i] == '|') {
                  termAtt.setLength(i);
                  i++;
                  tfAtt.setTermFrequency(ArrayUtil.parseInt(buffer, i, length - i));
                  return true;
                }
              }
              return true;
            }
            return false;
          }
        };
      }
    };

    mlt.setAnalyzer(analyzer);
    mlt.setFieldNames(new String[] {"text"});
    mlt.setBoost(true);

    final double boost10 = ((BooleanQuery) mlt.like("text", new StringReader("lucene|10 release|1")))
        .clauses()
        .stream()
        .map(BooleanClause::getQuery)
        .map(BoostQuery.class::cast)
        .filter(x -> ((TermQuery) x.getQuery()).getTerm().text().equals("lucene"))
        .mapToDouble(BoostQuery::getBoost)
        .sum();

    final double boost1 = ((BooleanQuery) mlt.like("text", new StringReader("lucene|1 release|1")))
        .clauses()
        .stream()
        .map(BooleanClause::getQuery)
        .map(BoostQuery.class::cast)
        .filter(x -> ((TermQuery) x.getQuery()).getTerm().text().equals("lucene"))
        .mapToDouble(BoostQuery::getBoost)
        .sum();

    // mlt should use the custom frequencies provided by the analyzer so "lucene|10" should be boosted more than "lucene|1"
    assertTrue(String.format(Locale.ROOT, "%s should be grater than %s", boost10, boost1), boost10 > boost1);
  }

  // TODO: add tests for the MoreLikeThisQuery
}
