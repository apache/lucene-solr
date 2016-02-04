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
package org.apache.lucene.analysis.query;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.UnicodeUtil;

/**
 * An {@link Analyzer} used primarily at query time to wrap another analyzer and provide a layer of protection
 * which prevents very common words from being passed into queries. 
 * <p>
 * For very large indexes the cost
 * of reading TermDocs for a very common word can be  high. This analyzer was created after experience with
 * a 38 million doc index which had a term in around 50% of docs and was causing TermQueries for 
 * this term to take 2 seconds.
 * </p>
 */
public final class QueryAutoStopWordAnalyzer extends AnalyzerWrapper {

  private final Analyzer delegate;
  private final Map<String, Set<String>> stopWordsPerField = new HashMap<>();
  //The default maximum percentage (40%) of index documents which
  //can contain a term, after which the term is considered to be a stop word.
  public static final float defaultMaxDocFreqPercent = 0.4f;

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for all
   * indexed fields from terms with a document frequency percentage greater than
   * {@link #defaultMaxDocFreqPercent}
   *
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Analyzer delegate,
      IndexReader indexReader) throws IOException {
    this(delegate, indexReader, defaultMaxDocFreqPercent);
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for all
   * indexed fields from terms with a document frequency greater than the given
   * maxDocFreq
   *
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param maxDocFreq Document frequency terms should be above in order to be stopwords
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Analyzer delegate,
      IndexReader indexReader,
      int maxDocFreq) throws IOException {
    this(delegate, indexReader, MultiFields.getIndexedFields(indexReader), maxDocFreq);
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for all
   * indexed fields from terms with a document frequency percentage greater than
   * the given maxPercentDocs
   *
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                      contain a term, after which the word is considered to be a stop word
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Analyzer delegate,
      IndexReader indexReader,
      float maxPercentDocs) throws IOException {
    this(delegate, indexReader, MultiFields.getIndexedFields(indexReader), maxPercentDocs);
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for the
   * given selection of fields from terms with a document frequency percentage
   * greater than the given maxPercentDocs
   *
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param fields Selection of fields to calculate stopwords for
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                      contain a term, after which the word is considered to be a stop word
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Analyzer delegate,
      IndexReader indexReader,
      Collection<String> fields,
      float maxPercentDocs) throws IOException {
    this(delegate, indexReader, fields, (int) (indexReader.numDocs() * maxPercentDocs));
  }

  /**
   * Creates a new QueryAutoStopWordAnalyzer with stopwords calculated for the
   * given selection of fields from terms with a document frequency greater than
   * the given maxDocFreq
   *
   * @param delegate Analyzer whose TokenStream will be filtered
   * @param indexReader IndexReader to identify the stopwords from
   * @param fields Selection of fields to calculate stopwords for
   * @param maxDocFreq Document frequency terms should be above in order to be stopwords
   * @throws IOException Can be thrown while reading from the IndexReader
   */
  public QueryAutoStopWordAnalyzer(
      Analyzer delegate,
      IndexReader indexReader,
      Collection<String> fields,
      int maxDocFreq) throws IOException {
    super(delegate.getReuseStrategy());
    this.delegate = delegate;
    
    for (String field : fields) {
      Set<String> stopWords = new HashSet<>();
      Terms terms = MultiFields.getTerms(indexReader, field);
      CharsRefBuilder spare = new CharsRefBuilder();
      if (terms != null) {
        TermsEnum te = terms.iterator();
        BytesRef text;
        while ((text = te.next()) != null) {
          if (te.docFreq() > maxDocFreq) {
            spare.copyUTF8Bytes(text);
            stopWords.add(spare.toString());
          }
        }
      }
      stopWordsPerField.put(field, stopWords);
    }
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return delegate;
  }

  @Override
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    Set<String> stopWords = stopWordsPerField.get(fieldName);
    if (stopWords == null) {
      return components;
    }
    StopFilter stopFilter = new StopFilter(components.getTokenStream(), 
        new CharArraySet(stopWords, false));
    return new TokenStreamComponents(components.getTokenizer(), stopFilter);
  }

  /**
   * Provides information on which stop words have been identified for a field
   *
   * @param fieldName The field for which stop words identified in "addStopWords"
   *                  method calls will be returned
   * @return the stop words identified for a field
   */
  public String[] getStopWords(String fieldName) {
    Set<String> stopWords = stopWordsPerField.get(fieldName);
    return stopWords != null ? stopWords.toArray(new String[stopWords.size()]) : new String[0];
  }

  /**
   * Provides information on which stop words have been identified for all fields
   *
   * @return the stop words (as terms)
   */
  public Term[] getStopWords() {
    List<Term> allStopWords = new ArrayList<>();
    for (String fieldName : stopWordsPerField.keySet()) {
      Set<String> stopWords = stopWordsPerField.get(fieldName);
      for (String text : stopWords) {
        allStopWords.add(new Term(fieldName, text));
      }
    }
    return allStopWords.toArray(new Term[allStopWords.size()]);
  }

}
