package org.apache.lucene.search.suggest.document;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TopSuggestDocs.*;
import static org.hamcrest.core.IsEqual.equalTo;

public class SuggestFieldTest extends LuceneTestCase {

  public Directory dir;

  @Before
  public void before() throws Exception {
    dir = newDirectory();
  }

  @After
  public void after() throws Exception {
    dir.close();
  }

  @Test
  public void testSimple() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(newSuggestField("suggest_field", "abc", 3l));
    document.add(newSuggestField("suggest_field", "abd", 4l));
    document.add(newSuggestField("suggest_field", "The Foo Fighters", 2l));
    iw.addDocument(document);
    document = new Document();
    document.add(newSuggestField("suggest_field", "abcdd", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs lookupDocs = suggestIndexSearcher.suggest("suggest_field", "ab", 3);
    assertSuggestions(lookupDocs, new Entry("abcdd", 5), new Entry("abd", 4), new Entry("abc", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleSuggestFieldsPerDoc() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "sug_field_1", "sug_field_2"));

    Document document = new Document();
    document.add(newSuggestField("sug_field_1", "apple", 4));
    document.add(newSuggestField("sug_field_2", "april", 3));
    iw.addDocument(document);
    document = new Document();
    document.add(newSuggestField("sug_field_1", "aples", 3));
    document.add(newSuggestField("sug_field_2", "apartment", 2));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggestDocs1 = suggestIndexSearcher.suggest("sug_field_1", "ap", 4);
    assertSuggestions(suggestDocs1, new Entry("apple", 4), new Entry("aples", 3));
    TopSuggestDocs suggestDocs2 = suggestIndexSearcher.suggest("sug_field_2", "ap", 4);
    assertSuggestions(suggestDocs2, new Entry("april", 3), new Entry("apartment", 2));

    // check that the doc ids are consistent
    for (int i = 0; i < suggestDocs1.scoreDocs.length; i++) {
      ScoreDoc suggestScoreDoc = suggestDocs1.scoreDocs[i];
      assertThat(suggestScoreDoc.doc, equalTo(suggestDocs2.scoreDocs[i].doc));
    }

    reader.close();
    iw.close();
  }

  @Test
  public void testDupSuggestFieldValues() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(300));
    long[] weights = new long[num];
    for(int i = 0; i < num; i++) {
      Document document = new Document();
      weights[i] = Math.abs(random().nextLong());
      document.add(newSuggestField("suggest_field", "abc", weights[i]));
      iw.addDocument(document);
    }
    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    Entry[] expectedEntries = new Entry[num];
    Arrays.sort(weights);
    for (int i = 1; i <= num; i++) {
      expectedEntries[i - 1] = new Entry("abc", weights[num - i]);
    }

    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs lookupDocs = suggestIndexSearcher.suggest("suggest_field", "abc", num);
    assertSuggestions(lookupDocs, expectedEntries);

    reader.close();
    iw.close();
  }

  @Test
  public void testNRTDeletedDocFiltering() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // using IndexWriter instead of RandomIndexWriter
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(10));

    Document document = new Document();
    int numLive = 0;
    List<Entry> expectedEntries = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      document.add(newSuggestField("suggest_field", "abc_" + i, num - i));
      if (i % 2 == 0) {
        document.add(newStringField("str_field", "delete", Field.Store.YES));
      } else {
        numLive++;
        expectedEntries.add(new Entry("abc_" + i, num - i));
        document.add(newStringField("str_field", "no_delete", Field.Store.YES));
      }
      iw.addDocument(document);
      document = new Document();
      if (usually()) {
        iw.commit();
      }
    }

    iw.deleteDocuments(new Term("str_field", "delete"));

    DirectoryReader reader = DirectoryReader.open(iw, true);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", numLive);
    assertSuggestions(suggest, expectedEntries.toArray(new Entry[expectedEntries.size()]));

    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnAllFilteredDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(newSuggestField("suggest_field", "abc_" + i, i));
      document.add(newStringField("str_fld", "deleted", Field.Store.NO));
      iw.addDocument(document);
      if (usually()) {
        iw.commit();
      }
    }

    Filter filter = new QueryWrapperFilter(new TermsQuery("str_fld", new BytesRef("non_existent")));
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    // no random access required;
    // calling suggest with filter that does not match any documents should early terminate
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", num, filter);
    assertThat(suggest.totalHits, equalTo(0));
    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnAllDeletedDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // using IndexWriter instead of RandomIndexWriter
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(newSuggestField("suggest_field", "abc_" + i, i));
      document.add(newStringField("delete", "delete", Field.Store.NO));
      iw.addDocument(document);
      if (usually()) {
        iw.commit();
      }
    }

    iw.deleteDocuments(new Term("delete", "delete"));

    DirectoryReader reader = DirectoryReader.open(iw, true);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", num);
    assertThat(suggest.totalHits, equalTo(0));

    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnMostlyDeletedDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    // using IndexWriter instead of RandomIndexWriter
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 1; i <= num; i++) {
      Document document = new Document();
      document.add(newSuggestField("suggest_field", "abc_" + i, i));
      document.add(new IntField("weight_fld", i, Field.Store.YES));
      iw.addDocument(document);
      if (usually()) {
        iw.commit();
      }
    }

    iw.deleteDocuments(NumericRangeQuery.newIntRange("weight_fld", 2, null, true, false));

    DirectoryReader reader = DirectoryReader.open(iw, true);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", 1);
    assertSuggestions(suggest, new Entry("abc_1", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testSuggestOnMostlyFilteredOutDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(newSuggestField("suggest_field", "abc_" + i, i));
      document.add(new IntField("filter_int_fld", i, Field.Store.NO));
      iw.addDocument(document);
      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);

    int topScore = num/2;
    QueryWrapperFilter filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", 0, topScore, true, true));
    Filter filter = randomAccessFilter(filterWrapper);
    // if at most half of the top scoring documents have been filtered out
    // the search should be admissible
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", 1, filter);
    assertSuggestions(suggest, new Entry("abc_" + topScore, topScore));

    filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", 0, 0, true, true));
    filter = randomAccessFilter(filterWrapper);
    // if more than half of the top scoring documents have been filtered out
    // search is not admissible, so # of suggestions requested is num instead of 1
    suggest = indexSearcher.suggest("suggest_field", "abc_", num, filter);
    assertSuggestions(suggest, new Entry("abc_0", 0));

    filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", num - 1, num - 1, true, true));
    filter = randomAccessFilter(filterWrapper);
    // if only lower scoring documents are filtered out
    // search is admissible
    suggest = indexSearcher.suggest("suggest_field", "abc_", 1, filter);
    assertSuggestions(suggest, new Entry("abc_" + (num - 1), num - 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testEarlyTermination() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));

    // have segments of 4 documents
    // with descending suggestion weights
    // suggest should early terminate for
    // segments with docs having lower suggestion weights
    for (int i = num; i > 0; i--) {
      Document document = new Document();
      document.add(newSuggestField("suggest_field", "abc_" + i, i));
      iw.addDocument(document);
      if (i % 4 == 0) {
        iw.commit();
      }
    }
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", 1);
    assertSuggestions(suggest, new Entry("abc_" + num, num));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultipleSegments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    List<Entry> entries = new ArrayList<>();

    // ensure at least some segments have no suggest field
    for (int i = num; i > 0; i--) {
      Document document = new Document();
      if (random().nextInt(4) == 1) {
        document.add(newSuggestField("suggest_field", "abc_" + i, i));
        entries.add(new Entry("abc_" + i, i));
      }
      document.add(new IntField("weight_fld", i, Field.Store.YES));
      iw.addDocument(document);
      if (usually()) {
        iw.commit();
      }
    }
    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", (entries.size() == 0) ? 1 : entries.size());
    assertSuggestions(suggest, entries.toArray(new Entry[entries.size()]));

    reader.close();
    iw.close();
  }

  @Test
  public void testDocFiltering() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    Document document = new Document();
    document.add(new IntField("filter_int_fld", 9, Field.Store.NO));
    document.add(newSuggestField("suggest_field", "apples", 3));
    iw.addDocument(document);

    document = new Document();
    document.add(new IntField("filter_int_fld", 10, Field.Store.NO));
    document.add(newSuggestField("suggest_field", "applle", 4));
    iw.addDocument(document);

    document = new Document();
    document.add(new IntField("filter_int_fld", 4, Field.Store.NO));
    document.add(newSuggestField("suggest_field", "apple", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);

    // suggest without filter
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "app", 3);
    assertSuggestions(suggest, new Entry("apple", 5), new Entry("applle", 4), new Entry("apples", 3));

    // suggest with filter
    QueryWrapperFilter filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", 5, 12, true, true));
    Filter filter = randomAccessFilter(filterWrapper);
    suggest = indexSearcher.suggest("suggest_field", "app", 3, filter);
    assertSuggestions(suggest, new Entry("applle", 4), new Entry("apples", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testReturnedDocID() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(newSuggestField("suggest_field", "abc_" + i, num));
      document.add(new IntField("int_field", i, Field.Store.YES));
      iw.addDocument(document);

      if (random().nextBoolean()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", "abc_", num);
    assertEquals(num, suggest.totalHits);
    for (SuggestScoreDoc suggestScoreDoc : suggest.scoreLookupDocs()) {
      String key = suggestScoreDoc.key.toString();
      assertTrue(key.startsWith("abc_"));
      String substring = key.substring(4);
      int fieldValue = Integer.parseInt(substring);
      Document doc = reader.document(suggestScoreDoc.doc);
      assertEquals(doc.getField("int_field").numericValue().intValue(), fieldValue);
    }

    reader.close();
    iw.close();
  }

  @Test
  public void testCompletionAnalyzerOptions() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    Map<String, Analyzer> map = new HashMap<>();
    map.put("suggest_field_default", new CompletionAnalyzer(analyzer));
    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(analyzer, false, true);
    map.put("suggest_field_no_p_sep", completionAnalyzer);
    completionAnalyzer = new CompletionAnalyzer(analyzer, true, false);
    map.put("suggest_field_no_p_pos_inc", completionAnalyzer);
    completionAnalyzer = new CompletionAnalyzer(analyzer, false, false);
    map.put("suggest_field_no_p_sep_or_pos_inc", completionAnalyzer);
    PerFieldAnalyzerWrapper analyzerWrapper = new PerFieldAnalyzerWrapper(analyzer, map);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzerWrapper, map.keySet()));

    Document document = new Document();
    document.add(newSuggestField("suggest_field_default", "foobar", 7));
    document.add(newSuggestField("suggest_field_default", "foo bar", 8));
    document.add(newSuggestField("suggest_field_default", "the fo", 9));
    document.add(newSuggestField("suggest_field_default", "the foo bar", 10));

    document.add(newSuggestField("suggest_field_no_p_sep", "foobar", 7));
    document.add(newSuggestField("suggest_field_no_p_sep", "foo bar", 8));
    document.add(newSuggestField("suggest_field_no_p_sep", "the fo", 9));
    document.add(newSuggestField("suggest_field_no_p_sep", "the foo bar", 10));

    document.add(newSuggestField("suggest_field_no_p_pos_inc", "foobar", 7));
    document.add(newSuggestField("suggest_field_no_p_pos_inc", "foo bar", 8));
    document.add(newSuggestField("suggest_field_no_p_pos_inc", "the fo", 9));
    document.add(newSuggestField("suggest_field_no_p_pos_inc", "the foo bar", 10));

    document.add(newSuggestField("suggest_field_no_p_sep_or_pos_inc", "foobar", 7));
    document.add(newSuggestField("suggest_field_no_p_sep_or_pos_inc", "foo bar", 8));
    document.add(newSuggestField("suggest_field_no_p_sep_or_pos_inc", "the fo", 9));
    document.add(newSuggestField("suggest_field_no_p_sep_or_pos_inc", "the foo bar", 10));
    iw.addDocument(document);

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);

    TopSuggestDocs suggest;
    suggest = indexSearcher.suggest("suggest_field_default", "fo", 4);
    assertSuggestions(suggest, new Entry("foo bar", 8), new Entry("foobar", 7));
    suggest = indexSearcher.suggest("suggest_field_default", "foob", 4);
    assertSuggestions(suggest, new Entry("foobar", 7));

    suggest = indexSearcher.suggest("suggest_field_no_p_sep", "fo", 4); // matches all 4
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("the fo", 9), new Entry("foo bar", 8), new Entry("foobar", 7));
    suggest = indexSearcher.suggest("suggest_field_no_p_sep", "foob", 4); // except the fo
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("foo bar", 8), new Entry("foobar", 7));

    suggest = indexSearcher.suggest("suggest_field_no_p_pos_inc", "fo", 4); //matches all 4
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("the fo", 9), new Entry("foo bar", 8), new Entry("foobar", 7));
    suggest = indexSearcher.suggest("suggest_field_no_p_pos_inc", "foob", 4); // only foobar
    assertSuggestions(suggest, new Entry("foobar", 7));

    suggest = indexSearcher.suggest("suggest_field_no_p_sep_or_pos_inc", "fo", 4); // all 4
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("the fo", 9), new Entry("foo bar", 8), new Entry("foobar", 7));
    suggest = indexSearcher.suggest("suggest_field_no_p_sep_or_pos_inc", "foob", 4); // not the fo
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("foo bar", 8), new Entry("foobar", 7));

    reader.close();
    iw.close();
  }

  @Test
  public void testScoring() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    int num = Math.min(1000, atLeast(10));
    String[] prefixes = {"abc", "bac", "cab"};
    Map<String, Long> mappings = new HashMap<>();
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      String suggest = prefixes[i % 3] + TestUtil.randomSimpleString(random(), 10) + "_" +String.valueOf(i);
      long weight = Math.abs(random().nextLong());
      document.add(newSuggestField("suggest_field", suggest, weight));
      mappings.put(suggest, weight);
      iw.addDocument(document);
      if (rarely()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    for (String prefix : prefixes) {
      TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", prefix, num);
      assertTrue(suggest.totalHits > 0);
      float topScore = -1;
      for (SuggestScoreDoc scoreDoc : suggest.scoreLookupDocs()) {
        if (topScore != -1) {
          assertTrue(topScore >= scoreDoc.score);
        }
        topScore = scoreDoc.score;
        assertThat((float) mappings.get(scoreDoc.key.toString()), equalTo(scoreDoc.score));
        assertNotNull(mappings.remove(scoreDoc.key.toString()));
      }
    }

    assertThat(mappings.size(), equalTo(0));
    reader.close();
    iw.close();
  }

  @Test
  public void testRealisticKeys() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    LineFileDocs lineFileDocs = new LineFileDocs(random());
    int num = Math.min(1000, atLeast(10));
    Map<String, Long> mappings = new HashMap<>();
    for (int i = 0; i < num; i++) {
      Document document = lineFileDocs.nextDoc();
      String title = document.getField("title").stringValue();
      long weight = Math.abs(random().nextLong());
      Long prevWeight = mappings.get(title);
      if (prevWeight == null || prevWeight < weight) {
        mappings.put(title, weight);
      }
      Document doc = new Document();
      doc.add(newSuggestField("suggest_field", title, weight));
      iw.addDocument(doc);

      if (rarely()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);

    for (Map.Entry<String, Long> entry : mappings.entrySet()) {
      String title = entry.getKey();

      TopSuggestDocs suggest = indexSearcher.suggest("suggest_field", title, mappings.size());
      assertTrue(suggest.totalHits > 0);
      boolean matched = false;
      for (ScoreDoc scoreDoc : suggest.scoreDocs) {
        matched = Float.compare(scoreDoc.score, (float) entry.getValue()) == 0;
        if (matched) {
          break;
        }
      }
      assertTrue("at least one of the entries should have the score", matched);
    }

    reader.close();
    iw.close();
  }

  @Test
  public void testThreads() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field_1", "suggest_field_2", "suggest_field_3"));
    final int num = Math.min(1000, atLeast(10));
    final String prefix1 = "abc1_";
    final String prefix2 = "abc2_";
    final String prefix3 = "abc3_";
    final Entry[] entries1 = new Entry[num];
    final Entry[] entries2 = new Entry[num];
    final Entry[] entries3 = new Entry[num];
    for (int i = 0; i < num; i++) {
      int weight = num - (i + 1);
      entries1[i] = new Entry(prefix1 + weight, weight);
      entries2[i] = new Entry(prefix2 + weight, weight);
      entries3[i] = new Entry(prefix3 + weight, weight);
    }
    for (int i = 0; i < num; i++) {
      Document doc = new Document();
      doc.add(newSuggestField("suggest_field_1", prefix1 + i, i));
      doc.add(newSuggestField("suggest_field_2", prefix2 + i, i));
      doc.add(newSuggestField("suggest_field_3", prefix3 + i, i));
      iw.addDocument(doc);

      if (rarely()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    int numThreads = TestUtil.nextInt(random(), 2, 7);
    Thread threads[] = new Thread[numThreads];
    final CyclicBarrier startingGun = new CyclicBarrier(numThreads+1);
    final CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
    final SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader, analyzer);
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            TopSuggestDocs suggest = indexSearcher.suggest("suggest_field_1", prefix1, num);
            assertSuggestions(suggest, entries1);
            suggest = indexSearcher.suggest("suggest_field_2", prefix2, num);
            assertSuggestions(suggest, entries2);
            suggest = indexSearcher.suggest("suggest_field_3", prefix3, num);
            assertSuggestions(suggest, entries3);
          } catch (Throwable e) {
            errors.add(e);
          }
        }
      };
      threads[i].start();
    }

    startingGun.await();
    for (Thread t : threads) {
      t.join();
    }
    assertTrue(errors.toString(), errors.isEmpty());

    reader.close();
    iw.close();
  }

  private static Filter randomAccessFilter(final Filter filter) {
    return new Filter() {
      @Override
      public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
        final DocIdSet docIdSet = filter.getDocIdSet(context, acceptDocs);
        final DocIdSetIterator iterator = docIdSet.iterator();
        final FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
        if (iterator != null) {
          int doc;
          while((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            bits.set(doc);
          }
        }
        return new DocIdSet() {
          @Override
          public DocIdSetIterator iterator() throws IOException {
            return iterator;
          }

          @Override
          public Bits bits() throws IOException {
            return bits;
          }

          @Override
          public long ramBytesUsed() {
            return docIdSet.ramBytesUsed();
          }
        };
      }

      @Override
      public String toString(String field) {
        return filter.toString(field);
      }
    };
  }

  private static class Entry {
    private final String output;
    private final float value;

    private Entry(String output, float value) {
      this.output = output;
      this.value = value;
    }
  }

  private void assertSuggestions(TopDocs actual, Entry... expected) {
    SuggestScoreDoc[] suggestScoreDocs = (SuggestScoreDoc[]) actual.scoreDocs;
    assertThat(suggestScoreDocs.length, equalTo(expected.length));
    for (int i = 0; i < suggestScoreDocs.length; i++) {
      SuggestScoreDoc lookupDoc = suggestScoreDocs[i];
      assertThat(lookupDoc.key.toString(), equalTo(expected[i].output));
      assertThat(lookupDoc.score, equalTo(expected[i].value));
    }
  }

  private SuggestField newSuggestField(String name, String value, long weight) throws IOException {
    return new SuggestField(name, value, weight);
  }

  private IndexWriterConfig iwcWithSuggestField(Analyzer analyzer, String... suggestFields) {
    return iwcWithSuggestField(analyzer, asSet(suggestFields));
  }

  private IndexWriterConfig iwcWithSuggestField(Analyzer analyzer, final Set<String> suggestFields) {
    IndexWriterConfig iwc = newIndexWriterConfig(random(), analyzer);
    iwc.setMergePolicy(newLogMergePolicy());
    Codec filterCodec = new Lucene50Codec() {
      PostingsFormat postingsFormat = new Completion50PostingsFormat();

      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        if (suggestFields.contains(field)) {
          return postingsFormat;
        }
        return super.getPostingsFormatForField(field);
      }
    };
    iwc.setCodec(filterCodec);
    return iwc;
  }
}
