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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;

public class TestContextSuggestField extends LuceneTestCase {

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
  public void testEmptySuggestion() throws Exception {
    try {
      new ContextSuggestField("suggest_field", "", 1, "type1");
      fail("no exception thrown when indexing zero length suggestion");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("value"));
    }
  }

  @Test
  public void testReservedChars() throws Exception {
    CharsRefBuilder charsRefBuilder = new CharsRefBuilder();
    charsRefBuilder.append("sugg");
    charsRefBuilder.setCharAt(2, (char) ContextSuggestField.CONTEXT_SEPARATOR);

    Analyzer analyzer = new MockAnalyzer(random());
    Document document = new Document();
    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "name"))) {
      document.add(new ContextSuggestField("name", "sugg", 1, charsRefBuilder.toString()));
      iw.addDocument(document);
      iw.commit();
      fail("no exception thrown for context value containing CONTEXT_SEPARATOR:" + ContextSuggestField.CONTEXT_SEPARATOR);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("[0x1d]"));
    }
    document = new Document();

    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "name"))) {
      document.add(new ContextSuggestField("name", charsRefBuilder.toString(), 1, "sugg"));
      iw.addDocument(document);
      iw.commit();
      fail("no exception thrown for value containing CONTEXT_SEPARATOR:" + ContextSuggestField.CONTEXT_SEPARATOR);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("[0x1d]"));
    }
  }

  @Test
  public void testMixedSuggestFields() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    Document document = new Document();
    document.add(new SuggestField("suggest_field", "suggestion1", 4));
    document.add(new ContextSuggestField("suggest_field", "suggestion2", 3));

    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir,
        iwcWithSuggestField(analyzer, "suggest_field"))) {
      iw.addDocument(document);
      iw.commit();
      fail("mixing suggest field types for same field name should error out");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("mixed types"));
    }
  }

  @Test
  public void testWithSuggestFields() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir,
        iwcWithSuggestField(analyzer, "suggest_field", "context_suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "suggestion1", 4));
    document.add(new SuggestField("suggest_field", "suggestion2", 3));
    document.add(new SuggestField("suggest_field", "suggestion3", 2));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion1", 4, "type1"));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion2", 3, "type2"));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion3", 2, "type3"));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "suggestion4", 1));
    document.add(new ContextSuggestField("context_suggest_field", "suggestion4", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);

    CompletionQuery query = new PrefixCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 10);
    assertSuggestions(suggest,
        new Entry("suggestion1", 4),
        new Entry("suggestion2", 3),
        new Entry("suggestion3", 2),
        new Entry("suggestion4", 1));

    query = new PrefixCompletionQuery(analyzer, new Term("context_suggest_field", "sugg"));
    suggest = suggestIndexSearcher.suggest(query, 10);
    assertSuggestions(suggest,
        new Entry("suggestion1", "type1", 4),
        new Entry("suggestion2", "type2", 3),
        new Entry("suggestion3", "type3", 2),
        new Entry("suggestion4", "type4", 1));

    reader.close();
    iw.close();
  }
}
