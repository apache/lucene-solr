package org.apache.lucene.analysis.query;
/**
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.StopFilter;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

/*
 * An analyzer used primarily at query time to wrap another analyzer and provide a layer of protection
 * which prevents very common words from being passed into queries. For very large indexes the cost
 * of reading TermDocs for a very common word can be  high. This analyzer was created after experience with
 * a 38 million doc index which had a term in around 50% of docs and was causing TermQueries for 
 * this term to take 2 seconds.
 *
 * Use the various "addStopWords" methods in this class to automate the identification and addition of 
 * stop words found in an already existing index.
 * 
 * 
 * 

 */
public class QueryAutoStopWordAnalyzer extends Analyzer {
  Analyzer delegate;
  HashMap stopWordsPerField = new HashMap();
  //The default maximum percentage (40%) of index documents which
  //can contain a term, after which the term is considered to be a stop word.
  public static final float defaultMaxDocFreqPercent = 0.4f;

  /**
   * Initializes this analyzer with the Analyzer object that actual produces the tokens
   *
   * @param delegate The choice of analyzer that is used to produce the token stream which needs filtering
   */
  public QueryAutoStopWordAnalyzer(Analyzer delegate) {
    this.delegate = delegate;
  }

  /**
   * Automatically adds stop words for all fields with terms exceeding the defaultMaxDocFreqPercent
   *
   * @param reader The IndexReader class which will be consulted to identify potential stop words that
   *               exceed the required document frequency
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int addStopWords(IndexReader reader) throws IOException {
    return addStopWords(reader, defaultMaxDocFreqPercent);
  }

  /**
   * Automatically adds stop words for all fields with terms exceeding the maxDocFreqPercent
   *
   * @param reader     The IndexReader class which will be consulted to identify potential stop words that
   *                   exceed the required document frequency
   * @param maxDocFreq The maximum number of index documents which can contain a term, after which
   *                   the term is considered to be a stop word
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int addStopWords(IndexReader reader, int maxDocFreq) throws IOException {
    int numStopWords = 0;
    Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.INDEXED);
    for (Iterator iter = fieldNames.iterator(); iter.hasNext();) {
      String fieldName = (String) iter.next();
      numStopWords += addStopWords(reader, fieldName, maxDocFreq);
    }
    return numStopWords;
  }

  /**
   * Automatically adds stop words for all fields with terms exceeding the maxDocFreqPercent
   *
   * @param reader        The IndexReader class which will be consulted to identify potential stop words that
   *                      exceed the required document frequency
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                      contain a term, after which the word is considered to be a stop word.
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int addStopWords(IndexReader reader, float maxPercentDocs) throws IOException {
    int numStopWords = 0;
    Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.INDEXED);
    for (Iterator iter = fieldNames.iterator(); iter.hasNext();) {
      String fieldName = (String) iter.next();
      numStopWords += addStopWords(reader, fieldName, maxPercentDocs);
    }
    return numStopWords;
  }

  /**
   * Automatically adds stop words for the given field with terms exceeding the maxPercentDocs
   *
   * @param reader         The IndexReader class which will be consulted to identify potential stop words that
   *                       exceed the required document frequency
   * @param fieldName      The field for which stopwords will be added
   * @param maxPercentDocs The maximum percentage (between 0.0 and 1.0) of index documents which
   *                       contain a term, after which the word is considered to be a stop word.
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int addStopWords(IndexReader reader, String fieldName, float maxPercentDocs) throws IOException {
    return addStopWords(reader, fieldName, (int) (reader.numDocs() * maxPercentDocs));
  }

  /**
   * Automatically adds stop words for the given field with terms exceeding the maxPercentDocs
   *
   * @param reader     The IndexReader class which will be consulted to identify potential stop words that
   *                   exceed the required document frequency
   * @param fieldName  The field for which stopwords will be added
   * @param maxDocFreq The maximum number of index documents which
   *                   can contain a term, after which the term is considered to be a stop word.
   * @return The number of stop words identified.
   * @throws IOException
   */
  public int addStopWords(IndexReader reader, String fieldName, int maxDocFreq) throws IOException {
    HashSet stopWords = new HashSet();
    String internedFieldName = fieldName.intern();
    TermEnum te = reader.terms(new Term(fieldName));
    Term term = te.term();
    while (term != null) {
      if (term.field() != internedFieldName) {
        break;
      }
      if (te.docFreq() > maxDocFreq) {
        stopWords.add(term.text());
      }
      if (!te.next()) {
        break;
      }
      term = te.term();
    }
    stopWordsPerField.put(fieldName, stopWords);
    return stopWords.size();
  }

  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = delegate.tokenStream(fieldName, reader);
    HashSet stopWords = (HashSet) stopWordsPerField.get(fieldName);
    if (stopWords != null) {
      result = new StopFilter(result, stopWords);
    }
    return result;
  }

  /**
   * Provides information on which stop words have been identified for a field
   *
   * @param fieldName The field for which stop words identified in "addStopWords"
   *                  method calls will be returned
   * @return the stop words identified for a field
   */
  public String[] getStopWords(String fieldName) {
    String[] result;
    HashSet stopWords = (HashSet) stopWordsPerField.get(fieldName);
    if (stopWords != null) {
      result = (String[]) stopWords.toArray(new String[stopWords.size()]);
    } else {
      result = new String[0];
    }
    return result;
  }

  /**
   * Provides information on which stop words have been identified for all fields
   *
   * @return the stop words (as terms)
   */
  public Term[] getStopWords() {
    ArrayList allStopWords = new ArrayList();
    for (Iterator iter = stopWordsPerField.keySet().iterator(); iter.hasNext();) {
      String fieldName = (String) iter.next();
      HashSet stopWords = (HashSet) stopWordsPerField.get(fieldName);
      for (Iterator iterator = stopWords.iterator(); iterator.hasNext();) {
        String text = (String) iterator.next();
        allStopWords.add(new Term(fieldName, text));
      }
    }
    return (Term[]) allStopWords.toArray(new Term[allStopWords.size()]);
	}

}
