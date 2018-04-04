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

import java.io.IOException;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class TestUnifiedHighlighterReanalysis extends LuceneTestCase {

  private MockAnalyzer indexAnalyzer =
      new MockAnalyzer(random(), MockTokenizer.SIMPLE, true);//whitespace, punctuation, lowercase;

  @Test
  public void testWithoutIndexSearcher() throws IOException {
    String text = "This is a test. Just a test highlighting without a searcher. Feel free to ignore.";
    BooleanQuery query = new BooleanQuery.Builder()
        .add(new TermQuery(new Term("body", "highlighting")), BooleanClause.Occur.SHOULD)
        .add(new TermQuery(new Term("title", "test")), BooleanClause.Occur.SHOULD)
        .build();

    UnifiedHighlighter highlighter = new UnifiedHighlighter(null, indexAnalyzer);
    String snippet = highlighter.highlightWithoutSearcher("body", query, text, 1).toString();

    assertEquals("Just a test <b>highlighting</b> without a searcher. ", snippet);

    assertEquals("test single space", " ", highlighter.highlightWithoutSearcher("body", query, " ", 1));

    assertEquals("Hello", highlighter.highlightWithoutSearcher("nonexistent", query, "Hello", 1));
  }

  @Test(expected = IllegalStateException.class)
  public void testIndexSearcherNullness() throws IOException {
    String text = "This is a test. Just a test highlighting without a searcher. Feel free to ignore.";
    Query query = new TermQuery(new Term("body", "highlighting"));

    try (Directory directory = newDirectory();
         RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
         IndexReader indexReader = indexWriter.getReader()) {
      IndexSearcher searcher = newSearcher(indexReader);
      UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, indexAnalyzer);
      highlighter.highlightWithoutSearcher("body", query, text, 1);//should throw
    }
  }

}
