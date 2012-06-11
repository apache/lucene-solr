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

package org.apache.lucene.benchmark.byTask.tasks;

import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.search.Query;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * Test Search task which counts number of searches.
 */
public class CountingHighlighterTestTask extends SearchTravRetHighlightTask {

  public static int numHighlightedResults = 0;
  public static int numDocsRetrieved = 0;

  public CountingHighlighterTestTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  protected Document retrieveDoc(IndexReader ir, int id) throws IOException {
    Document document = ir.document(id);
    if (document != null) {
      numDocsRetrieved++;
    }
    return document;
  }

  @Override
  public BenchmarkHighlighter getBenchmarkHighlighter(Query q) {
    highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(q));
    return new BenchmarkHighlighter() {
      @Override
      public int doHighlight(IndexReader reader, int doc, String field, Document document, Analyzer analyzer, String text) throws Exception {
        TokenStream ts = TokenSources.getAnyTokenStream(reader, doc, field, document, analyzer);
        TextFragment[] frag = highlighter.getBestTextFragments(ts, text, mergeContiguous, maxFrags);
        numHighlightedResults += frag != null ? frag.length : 0;
        return frag != null ? frag.length : 0;
      }
    };
  }
}
