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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestPrefixCompletionQuery extends LuceneTestCase {
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

    document.add(new SuggestField("suggest_field", "abc", 3));
    document.add(new SuggestField("suggest_field", "abd", 4));
    document.add(new SuggestField("suggest_field", "The Foo Fighters", 2));
    iw.addDocument(document);

    document = new Document();
    document.add(new SuggestField("suggest_field", "abcdd", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab"));
    TopSuggestDocs lookupDocs = suggestIndexSearcher.suggest(query, 3);
    assertSuggestions(lookupDocs, new Entry("abcdd", 5), new Entry("abd", 4), new Entry("abc", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testMostlyFilteredOutDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, i));
      document.add(new IntField("filter_int_fld", i, Field.Store.NO));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);

    int topScore = num/2;
    QueryWrapperFilter filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", 0, topScore, true, true));
    Filter filter = randomAccessFilter(filterWrapper);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    // if at most half of the top scoring documents have been filtered out
    // the search should be admissible for a single segment
    TopSuggestDocs suggest = indexSearcher.suggest(query, num);
    assertTrue(suggest.totalHits >= 1);
    assertThat(suggest.scoreLookupDocs()[0].key.toString(), equalTo("abc_" + topScore));
    assertThat(suggest.scoreLookupDocs()[0].score, equalTo((float) topScore));

    filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", 0, 0, true, true));
    filter = randomAccessFilter(filterWrapper);
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    // if more than half of the top scoring documents have been filtered out
    // search is not admissible, so # of suggestions requested is num instead of 1
    suggest = indexSearcher.suggest(query, num);
    assertSuggestions(suggest, new Entry("abc_0", 0));

    filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", num - 1, num - 1, true, true));
    filter = randomAccessFilter(filterWrapper);
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    // if only lower scoring documents are filtered out
    // search is admissible
    suggest = indexSearcher.suggest(query, 1);
    assertSuggestions(suggest, new Entry("abc_" + (num - 1), num - 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testDocFiltering() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    Document document = new Document();
    document.add(new IntField("filter_int_fld", 9, Field.Store.NO));
    document.add(new SuggestField("suggest_field", "apples", 3));
    iw.addDocument(document);

    document = new Document();
    document.add(new IntField("filter_int_fld", 10, Field.Store.NO));
    document.add(new SuggestField("suggest_field", "applle", 4));
    iw.addDocument(document);

    document = new Document();
    document.add(new IntField("filter_int_fld", 4, Field.Store.NO));
    document.add(new SuggestField("suggest_field", "apple", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);

    // suggest without filter
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "app"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, 3);
    assertSuggestions(suggest, new Entry("apple", 5), new Entry("applle", 4), new Entry("apples", 3));

    // suggest with filter
    QueryWrapperFilter filterWrapper = new QueryWrapperFilter(NumericRangeQuery.newIntRange("filter_int_fld", 5, 12, true, true));
    Filter filter = randomAccessFilter(filterWrapper);
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "app"), filter);
    suggest = indexSearcher.suggest(query, 3);
    assertSuggestions(suggest, new Entry("applle", 4), new Entry("apples", 3));

    reader.close();
    iw.close();
  }

  @Test
  public void testAnalyzerWithoutPreservePosAndSep() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(analyzer, false, false);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(completionAnalyzer, "suggest_field_no_p_sep_or_pos_inc"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field_no_p_sep_or_pos_inc", "foobar", 7));
    document.add(new SuggestField("suggest_field_no_p_sep_or_pos_inc", "foo bar", 8));
    document.add(new SuggestField("suggest_field_no_p_sep_or_pos_inc", "the fo", 9));
    document.add(new SuggestField("suggest_field_no_p_sep_or_pos_inc", "the foo bar", 10));
    iw.addDocument(document);

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_no_p_sep_or_pos_inc", "fo"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, 4); // all 4
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("the fo", 9), new Entry("foo bar", 8), new Entry("foobar", 7));
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_no_p_sep_or_pos_inc", "foob"));
    suggest = indexSearcher.suggest(query, 4); // not the fo
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("foo bar", 8), new Entry("foobar", 7));
    reader.close();
    iw.close();
  }

  @Test
  public void testAnalyzerWithSepAndNoPreservePos() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(analyzer, true, false);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(completionAnalyzer, "suggest_field_no_p_pos_inc"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field_no_p_pos_inc", "foobar", 7));
    document.add(new SuggestField("suggest_field_no_p_pos_inc", "foo bar", 8));
    document.add(new SuggestField("suggest_field_no_p_pos_inc", "the fo", 9));
    document.add(new SuggestField("suggest_field_no_p_pos_inc", "the foo bar", 10));
    iw.addDocument(document);

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_no_p_pos_inc", "fo"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, 4); //matches all 4
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("the fo", 9), new Entry("foo bar", 8), new Entry("foobar", 7));
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_no_p_pos_inc", "foob"));
    suggest = indexSearcher.suggest(query, 4); // only foobar
    assertSuggestions(suggest, new Entry("foobar", 7));
    reader.close();
    iw.close();
  }

  @Test
  public void testAnalyzerWithPreservePosAndNoSep() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, true, MockTokenFilter.ENGLISH_STOPSET);
    CompletionAnalyzer completionAnalyzer = new CompletionAnalyzer(analyzer, false, true);
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(completionAnalyzer, "suggest_field_no_p_sep"));
    Document document = new Document();
    document.add(new SuggestField("suggest_field_no_p_sep", "foobar", 7));
    document.add(new SuggestField("suggest_field_no_p_sep", "foo bar", 8));
    document.add(new SuggestField("suggest_field_no_p_sep", "the fo", 9));
    document.add(new SuggestField("suggest_field_no_p_sep", "the foo bar", 10));
    iw.addDocument(document);

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_no_p_sep", "fo"));
    TopSuggestDocs suggest = indexSearcher.suggest(query, 4); // matches all 4
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("the fo", 9), new Entry("foo bar", 8), new Entry("foobar", 7));
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field_no_p_sep", "foob"));
    suggest = indexSearcher.suggest(query, 4); // except the fo
    assertSuggestions(suggest, new Entry("the foo bar", 10), new Entry("foo bar", 8), new Entry("foobar", 7));
    reader.close();
    iw.close();
  }

  private static class RandomAccessFilter extends Filter {
    private final Filter in;

    private RandomAccessFilter(Filter in) {
      this.in = in;
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      DocIdSet docIdSet = in.getDocIdSet(context, acceptDocs);
      DocIdSetIterator iterator = docIdSet.iterator();
      FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
      if (iterator != null) {
        bits.or(iterator);
      }
      return new BitDocIdSet(bits);
    }

    @Override
    public String toString(String field) {
      return in.toString(field);
    }

    @Override
    public boolean equals(Object obj) {
      if (super.equals(obj) == false) {
        return false;
      }
      return in.equals(((RandomAccessFilter) obj).in);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + in.hashCode();
    }
  }

  private static Filter randomAccessFilter(Filter filter) {
    return new RandomAccessFilter(filter);
  }

}
