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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.lucene.search.suggest.document.TestSuggestField.Entry;
import static org.apache.lucene.search.suggest.document.TestSuggestField.assertSuggestions;
import static org.apache.lucene.search.suggest.document.TestSuggestField.iwcWithSuggestField;

public class TestFuzzyCompletionQuery extends LuceneTestCase {
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
  public void testFuzzyQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new SuggestField("suggest_field", "suggestion", 2));
    document.add(new SuggestField("suggest_field", "suaggestion", 4));
    document.add(new SuggestField("suggest_field", "ssuggestion", 1));
    iw.addDocument(document);
    document = new Document();
    document.add(new SuggestField("suggest_field", "sugfoo", 1));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugg"));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 4, false);
    assertSuggestions(suggest,
        new Entry("suaggestion", 4 * 2),
        new Entry("suggestion", 2 * 3),
        new Entry("sugfoo", 1 * 3),
        new Entry("ssuggestion", 1 * 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "sduggestion", 1, "type1"));
    document.add(new ContextSuggestField("suggest_field", "sudggestion", 1, "type2"));
    document.add(new ContextSuggestField("suggest_field", "sugdgestion", 1, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggdestion", 1, "type4"));
    document.add(new ContextSuggestField("suggest_field", "suggestion", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery query =  new ContextQuery(new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugge")));
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(query, 5, false);
    assertSuggestions(suggest,
        new Entry("suggdestion", "type4", 4),
        new Entry("suggestion", "type4", 4),
        new Entry("sugdgestion", "type3", 3),
        new Entry("sudggestion", "type2", 2),
        new Entry("sduggestion", "type1", 1)
    );

    reader.close();
    iw.close();
  }

  @Test
  public void testFuzzyFilteredContextQuery() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwcWithSuggestField(analyzer, "suggest_field"));
    Document document = new Document();

    document.add(new ContextSuggestField("suggest_field", "sduggestion", 1, "type1"));
    document.add(new ContextSuggestField("suggest_field", "sudggestion", 1, "type2"));
    document.add(new ContextSuggestField("suggest_field", "sugdgestion", 1, "type3"));
    iw.addDocument(document);

    document = new Document();
    document.add(new ContextSuggestField("suggest_field", "suggdestion", 1, "type4"));
    document.add(new ContextSuggestField("suggest_field", "suggestion", 1, "type4"));
    iw.addDocument(document);

    if (rarely()) {
      iw.commit();
    }

    DirectoryReader reader = iw.getReader();
    SuggestIndexSearcher suggestIndexSearcher = new SuggestIndexSearcher(reader);
    CompletionQuery fuzzyQuery = new FuzzyCompletionQuery(analyzer, new Term("suggest_field", "sugge"));
    ContextQuery contextQuery = new ContextQuery(fuzzyQuery);
    contextQuery.addContext("type1", 6);
    contextQuery.addContext("type3", 2);
    TopSuggestDocs suggest = suggestIndexSearcher.suggest(contextQuery, 5, false);
    assertSuggestions(suggest,
        new Entry("sduggestion", "type1", 1 * (1 + 6)),
        new Entry("sugdgestion", "type3", 1 * (3 + 2))
    );

    reader.close();
    iw.close();
  }
}
