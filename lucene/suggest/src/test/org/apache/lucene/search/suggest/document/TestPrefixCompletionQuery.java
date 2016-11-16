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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.suggest.BitsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestPrefixCompletionQuery extends LuceneTestCase {

  private static class NumericRangeBitsProducer extends BitsProducer {

    private final String field;
    private final long min, max;

    public NumericRangeBitsProducer(String field, long min, long max) {
      this.field = field;
      this.min = min;
      this.max = max;
    }

    @Override
    public String toString() {
      return field + "[" + min + ".." + max + "]";
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      NumericRangeBitsProducer that = (NumericRangeBitsProducer) obj;
      return field.equals(that.field)
          && min == that.min
          && max == that.max;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getClass(), field, min, max);
    }

    @Override
    public Bits getBits(final LeafReaderContext context) throws IOException {
      final int maxDoc = context.reader().maxDoc();
      FixedBitSet bits = new FixedBitSet(maxDoc);
      final SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
      int docID;
      while ((docID = values.nextDoc()) != NO_MORE_DOCS) {
        final int count = values.docValueCount();
        for (int i = 0; i < count; ++i) {
          final long v = values.nextValue();
          if (v >= min && v <= max) {
            bits.set(docID);
            break;
          }
        }
      }
      return bits;
    }
  }

  public Directory dir;

  @Before
  public void before() throws Exception {
    dir = newDirectory();
  }

  @After
  public void after() throws Exception {
    dir.close();
  }

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

  public void testMostlyFilteredOutDocuments() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int num = Math.min(1000, atLeast(10));
    for (int i = 0; i < num; i++) {
      Document document = new Document();
      document.add(new SuggestField("suggest_field", "abc_" + i, i));
      document.add(new NumericDocValuesField("filter_int_fld", i));
      iw.addDocument(document);

      if (usually()) {
        iw.commit();
      }
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);

    int topScore = num/2;
    BitsProducer filter = new NumericRangeBitsProducer("filter_int_fld", 0, topScore);
    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    // if at most half of the top scoring documents have been filtered out
    // the search should be admissible for a single segment
    TopSuggestDocs suggest = indexSearcher.suggest(query, num);
    assertTrue(suggest.totalHits >= 1);
    assertThat(suggest.scoreLookupDocs()[0].key.toString(), equalTo("abc_" + topScore));
    assertThat(suggest.scoreLookupDocs()[0].score, equalTo((float) topScore));

    filter = new NumericRangeBitsProducer("filter_int_fld", 0, 0);
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    // if more than half of the top scoring documents have been filtered out
    // search is not admissible, so # of suggestions requested is num instead of 1
    suggest = indexSearcher.suggest(query, num);
    assertSuggestions(suggest, new Entry("abc_0", 0));

    filter = new NumericRangeBitsProducer("filter_int_fld", num - 1, num - 1);
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "abc_"), filter);
    // if only lower scoring documents are filtered out
    // search is admissible
    suggest = indexSearcher.suggest(query, 1);
    assertSuggestions(suggest, new Entry("abc_" + (num - 1), num - 1));

    reader.close();
    iw.close();
  }

  public void testDocFiltering() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));

    Document document = new Document();
    document.add(new NumericDocValuesField("filter_int_fld", 9));
    document.add(new SuggestField("suggest_field", "apples", 3));
    iw.addDocument(document);

    document = new Document();
    document.add(new NumericDocValuesField("filter_int_fld", 10));
    document.add(new SuggestField("suggest_field", "applle", 4));
    iw.addDocument(document);

    document = new Document();
    document.add(new NumericDocValuesField("filter_int_fld", 4));
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
    BitsProducer filter = new NumericRangeBitsProducer("filter_int_fld", 5, 12);
    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "app"), filter);
    suggest = indexSearcher.suggest(query, 3);
    assertSuggestions(suggest, new Entry("applle", 4), new Entry("apples", 3));

    reader.close();
    iw.close();
  }

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

  public void testGhostField() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    IndexWriter iw = new IndexWriter(dir, iwcWithSuggestField(analyzer, "suggest_field", "suggest_field2", "suggest_field3"));

    Document document = new Document();
    document.add(new StringField("id", "0", Field.Store.NO));
    document.add(new SuggestField("suggest_field", "apples", 3));
    iw.addDocument(document);
    // need another document so whole segment isn't deleted
    iw.addDocument(new Document());
    iw.commit();

    document = new Document();
    document.add(new StringField("id", "1", Field.Store.NO));
    document.add(new SuggestField("suggest_field2", "apples", 3));
    iw.addDocument(document);
    iw.commit();

    iw.deleteDocuments(new Term("id", "0"));
    // first force merge is OK
    iw.forceMerge(1);
    
    // second force merge causes MultiFields to include "suggest_field" in its iteration, yet a null Terms is returned (no documents have
    // this field anymore)
    iw.addDocument(new Document());
    iw.forceMerge(1);

    DirectoryReader reader = DirectoryReader.open(iw);
    SuggestIndexSearcher indexSearcher = new SuggestIndexSearcher(reader);

    PrefixCompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "app"));
    assertEquals(0, indexSearcher.suggest(query, 3).totalHits);

    query = new PrefixCompletionQuery(analyzer, new Term("suggest_field2", "app"));
    assertSuggestions(indexSearcher.suggest(query, 3), new Entry("apples", 3));

    reader.close();
    iw.close();
  }
}
