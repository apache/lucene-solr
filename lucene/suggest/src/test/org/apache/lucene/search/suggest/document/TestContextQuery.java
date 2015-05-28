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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;

public class TestContextQuery extends LuceneTestCase {
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
  public void testIllegalInnerQuery() throws Exception {
    try {
      new ContextQuery(new ContextQuery(
          new PrefixCompletionQuery(new MockAnalyzer(random()), new Term("suggest_field", "sugg"))));
      fail("should error out trying to nest a Context query within another Context query");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains(ContextQuery.class.getSimpleName()));
    }
  }

  @Test
  public void testSimpleContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type1"), "suggestion1", 8));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 7));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 6));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 1);
    query.addContext("type2", 2);
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion4", "type4", 5 * 4),
        new Entry("suggestion3", "type3", 6 * 3),
        new Entry("suggestion2", "type2", 7 * 2),
        new Entry("suggestion1", "type1", 8 * 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testContextQueryOnSuggestField() throws Exception {
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
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "ab")));
    try {
      suggestIndexSearcher.suggest(query, 4);
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("SuggestField"));
    }
    reader.close();
    iw.close();
  }

  @Test
  public void testNonExactContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type1"), "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type", 1, false);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4),
        new Entry("suggestion2", "type2", 3),
        new Entry("suggestion3", "type3", 2),
        new Entry("suggestion4", "type4", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testContextPrecedenceBoost() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("typetype"), "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type"), "suggestion2", 3));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type", 1);
    query.addContext("typetype", 2);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", "typetype", 4 * 2),
        new Entry("suggestion2", "type", 3 * 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testEmptyContext() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", null, "suggestion_no_ctx", 4));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion_no_ctx", null, 4),
        new Entry("suggestion", "type4", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testEmptyContextWithBoosts() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", null, "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>emptyList(), "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", null, "suggestion3", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);

    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type4", 10);
    query.addContext("*");
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion4", "type4", 1 * 10),
        new Entry("suggestion1", null, 4),
        new Entry("suggestion2", null, 3),
        new Entry("suggestion3", null, 2)
    );
    reader.close();
    iw.close();
  }

  @Test
  public void testSameSuggestionMultipleContext() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Arrays.<CharSequence>asList("type1", "type2", "type3"), "suggestion", 4));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 10);
    query.addContext("type2", 2);
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion", "type1", 4 * 10),
        new Entry("suggestion", "type3", 4 * 3),
        new Entry("suggestion", "type2", 4 * 2),
        new Entry("suggestion", "type4", 1 * 4)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testMixedContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type1"), "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 7);
    query.addContext("type2", 6);
    query.addContext("*", 5);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4 * 7),
        new Entry("suggestion2", "type2", 3 * 6),
        new Entry("suggestion3", "type3", 2 * 5),
        new Entry("suggestion4", "type4", 1 * 5)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFilteringContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type1"), "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion3", "type3", 2 * 3),
        new Entry("suggestion4", "type4", 1 * 4)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testContextQueryRewrite() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type1"), "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4),
        new Entry("suggestion2", "type2", 3),
        new Entry("suggestion3", "type3", 2),
        new Entry("suggestion4", "type4", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testMultiContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Arrays.<CharSequence>asList("type1", "type3"), "suggestion1", 8));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 7));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 6));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 5));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    query.addContext("type1", 1);
    query.addContext("type2", 2);
    query.addContext("type3", 3);
    query.addContext("type4", 4);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type3", 8 * 3),
        new Entry("suggestion4", "type4", 5 * 4),
        new Entry("suggestion3", "type3", 6 * 3),
        new Entry("suggestion2", "type2", 7 * 2),
        new Entry("suggestion1", "type1", 8 * 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testAllContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type1"), "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type2"), "suggestion2", 3));
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type3"), "suggestion3", 2));
    iw.addDocument(document);
    document = new Document();
    document.add(new ContextSuggestField("suggest_field", Collections.<CharSequence>singletonList("type4"), "suggestion4", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4),
        new Entry("suggestion2", "type2", 3),
        new Entry("suggestion3", "type3", 2),
        new Entry("suggestion4", "type4", 1));

    reader.close();
    iw.close();
  }

  @Test
  public void testRandomContextQueryScoring() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    int numSuggestions = atLeast(20);
    int numContexts = atLeast(5);

    Set<Integer> seenWeights = new HashSet<>();
    List<Entry> expectedEntries = new ArrayList<>();
    List<CharSequence> contexts = new ArrayList<>();
    for (int i = 1; i <= numContexts; i++) {
      CharSequence context = TestUtil.randomSimpleString(random(), 10) + i;
      contexts.add(context);
      for (int j = 1; j <= numSuggestions; j++) {
        String suggestion = "sugg_" + TestUtil.randomSimpleString(random(), 10) + j;
        int weight = TestUtil.nextInt(random(), 1, 1000 * numContexts * numSuggestions);
        while (seenWeights.contains(weight)) {
          weight = TestUtil.nextInt(random(), 1, 1000 * numContexts * numSuggestions);
        }
        seenWeights.add(weight);
        Document document = new Document();
        document.add(new ContextSuggestField("suggest_field", Collections.singletonList(context), suggestion, weight));
        iw.addDocument(document);
        expectedEntries.add(new Entry(suggestion, context.toString(), i * weight));
      }
      if (rarely()) {
        iw.commit();
      }
    }
    Entry[] expectedResults = expectedEntries.toArray(new Entry[expectedEntries.size()]);

    ArrayUtil.introSort(expectedResults, new Comparator<Entry>() {
      @Override
      public int compare(Entry o1, Entry o2) {
        int cmp = Float.compare(o2.value, o1.value);
        if (cmp != 0) {
          return cmp;
        } else {
          return o1.output.compareTo(o2.output);
        }
      }
    });

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    ContextQuery query = new ContextQuery(new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg")));
    for (int i = 0; i < contexts.size(); i++) {
      query.addContext(contexts.get(i), i + 1);
    }
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4);
    assertSuggestions(suggest, Arrays.copyOfRange(expectedResults, 0, 4));

    reader.close();
    iw.close();
  }
}
