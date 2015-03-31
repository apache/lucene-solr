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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.automaton.Automaton;

import static org.apache.lucene.search.suggest.document.CompletionFieldsProducer.CompletionTerms;

/**
 * Adds document suggest capabilities to IndexSearcher
 *
 * @lucene.experimental
 */
public class SuggestIndexSearcher extends IndexSearcher {

  private final Analyzer queryAnalyzer;

  /**
   * Creates a searcher with document suggest capabilities
   * for <code>reader</code>.
   * <p>
   * Suggestion <code>key</code> is analyzed with <code>queryAnalyzer</code>
   */
  public SuggestIndexSearcher(IndexReader reader, Analyzer queryAnalyzer) {
    super(reader);
    this.queryAnalyzer = queryAnalyzer;
  }

  /**
   * Calls {@link #suggest(String, CharSequence, int, Filter)}
   * with no document filter
   */
  public TopSuggestDocs suggest(String field, CharSequence key, int num) throws IOException {
    return suggest(field, key, num, (Filter) null);
  }

  /**
   * Calls {@link #suggest(String, CharSequence, int, Filter, TopSuggestDocsCollector)}
   * with no document filter
   */
  public void suggest(String field, CharSequence key, int num, TopSuggestDocsCollector collector) throws IOException {
    suggest(field, key, num, null, collector);
  }

  /**
   * Suggests at most <code>num</code> documents filtered by <code>filter</code>
   * that completes to <code>key</code> for a suggest <code>field</code>
   * <p>
   * Returns at most Top <code>num</code> document ids with corresponding completion and weight pair
   *
   * @throws java.lang.IllegalArgumentException if <code>filter</code> does not provide a random access
   *                                            interface or if <code>field</code> is not a {@link SuggestField}
   */
  public TopSuggestDocs suggest(String field, CharSequence key, int num, Filter filter) throws IOException {
    TopSuggestDocsCollector collector = new TopSuggestDocsCollector(num);
    suggest(field, key, num, filter, collector);
    return collector.get();
  }

  /**
   * Suggests at most <code>num</code> documents filtered by <code>filter</code>
   * that completes to <code>key</code> for a suggest <code>field</code>
   * <p>
   * Collect completions with {@link TopSuggestDocsCollector}
   * The completions are collected in order of the suggest <code>field</code> weight.
   * There can be more than one collection of the same document, if the <code>key</code>
   * matches multiple <code>field</code> values of the same document
   *
   * @throws java.lang.IllegalArgumentException if <code>filter</code> does not provide a random access
   *                                            interface or if <code>field</code> is not a {@link SuggestField}
   */
  public void suggest(String field, CharSequence key, int num, Filter filter, TopSuggestDocsCollector collector) throws IOException {
    // verify input
    if (field == null) {
      throw new IllegalArgumentException("'field' can not be null");
    }
    if (num <= 0) {
      throw new IllegalArgumentException("'num' should be > 0");
    }
    if (collector == null) {
      throw new IllegalArgumentException("'collector' can not be null");
    }

    // build query automaton
    CompletionAnalyzer analyzer;
    if (queryAnalyzer instanceof CompletionAnalyzer) {
      analyzer = (CompletionAnalyzer) queryAnalyzer;
    } else {
      analyzer = new CompletionAnalyzer(queryAnalyzer);
    }
    final Automaton automaton = analyzer.toAutomaton(field, key);

    // collect results
    for (LeafReaderContext context : getIndexReader().leaves()) {
      TopSuggestDocsCollector leafCollector = (TopSuggestDocsCollector) collector.getLeafCollector(context);
      LeafReader reader = context.reader();
      Terms terms = reader.terms(field);
      if (terms == null) {
        continue;
      }
      NRTSuggester suggester;
      if (terms instanceof CompletionTerms) {
        CompletionTerms completionTerms = (CompletionTerms) terms;
        suggester = completionTerms.suggester();
      } else {
        throw new IllegalArgumentException(field + " is not a SuggestField");
      }
      if (suggester == null) {
        // a segment can have a null suggester
        // i.e. no FST was built
        continue;
      }

      DocIdSet docIdSet = null;
      if (filter != null) {
        docIdSet = filter.getDocIdSet(context, reader.getLiveDocs());
        if (docIdSet == null) {
          // filter matches no docs in current leave
          continue;
        }
      }
      suggester.lookup(reader, automaton, num, docIdSet, leafCollector);
    }
  }
}
