package org.apache.lucene.analysis.shingle;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

/**
 * A ShingleAnalyzerWrapper wraps a {@link ShingleFilter} around another {@link Analyzer}.
 * <p>
 * A shingle is another name for a token based n-gram.
 * </p>
 */
public final class ShingleAnalyzerWrapper extends Analyzer {

  private final Analyzer defaultAnalyzer;
  private final int maxShingleSize;
  private final int minShingleSize;
  private final String tokenSeparator;
  private final boolean outputUnigrams;
  private final boolean outputUnigramsIfNoShingles;

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
  }

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer, int maxShingleSize) {
    this(defaultAnalyzer, ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE, maxShingleSize);
  }

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer, int minShingleSize, int maxShingleSize) {
    this(defaultAnalyzer, minShingleSize, maxShingleSize, ShingleFilter.TOKEN_SEPARATOR, true, false);
  }

  /**
   * Creates a new ShingleAnalyzerWrapper
   *
   * @param defaultAnalyzer Analyzer whose TokenStream is to be filtered
   * @param minShingleSize Min shingle (token ngram) size
   * @param maxShingleSize Max shingle size
   * @param tokenSeparator Used to separate input stream tokens in output shingles
   * @param outputUnigrams Whether or not the filter shall pass the original
   *        tokens to the output stream
   * @param outputUnigramsIfNoShingles Overrides the behavior of outputUnigrams==false for those
   *        times when no shingles are available (because there are fewer than
   *        minShingleSize tokens in the input stream)?
   *        Note that if outputUnigrams==true, then unigrams are always output,
   *        regardless of whether any shingles are available.
   */
  public ShingleAnalyzerWrapper(
      Analyzer defaultAnalyzer,
      int minShingleSize,
      int maxShingleSize,
      String tokenSeparator,
      boolean outputUnigrams,
      boolean outputUnigramsIfNoShingles) {
    this.defaultAnalyzer = defaultAnalyzer;

    if (maxShingleSize < 2) {
      throw new IllegalArgumentException("Max shingle size must be >= 2");
    }
    this.maxShingleSize = maxShingleSize;

    if (minShingleSize < 2) {
      throw new IllegalArgumentException("Min shingle size must be >= 2");
    }
    if (minShingleSize > maxShingleSize) {
      throw new IllegalArgumentException
        ("Min shingle size must be <= max shingle size");
    }
    this.minShingleSize = minShingleSize;

    this.tokenSeparator = (tokenSeparator == null ? "" : tokenSeparator);
    this.outputUnigrams = outputUnigrams;
    this.outputUnigramsIfNoShingles = outputUnigramsIfNoShingles;
  }

  /**
   * Wraps {@link StandardAnalyzer}. 
   */
  public ShingleAnalyzerWrapper(Version matchVersion) {
    this(matchVersion, ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE, ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
  }

  /**
   * Wraps {@link StandardAnalyzer}. 
   */
  public ShingleAnalyzerWrapper(Version matchVersion, int minShingleSize, int maxShingleSize) {
    this(new StandardAnalyzer(matchVersion), minShingleSize, maxShingleSize);
  }

  /**
   * The max shingle (token ngram) size
   * 
   * @return The max shingle (token ngram) size
   */
  public int getMaxShingleSize() {
    return maxShingleSize;
  }

  /**
   * The min shingle (token ngram) size
   * 
   * @return The min shingle (token ngram) size
   */
  public int getMinShingleSize() {
    return minShingleSize;
  }

  public String getTokenSeparator() {
    return tokenSeparator;
  }
  
  public boolean isOutputUnigrams() {
    return outputUnigrams;
  }
  
  public boolean isOutputUnigramsIfNoShingles() {
    return outputUnigramsIfNoShingles;
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream wrapped;
    try {
      wrapped = defaultAnalyzer.reusableTokenStream(fieldName, reader);
    } catch (IOException e) {
      wrapped = defaultAnalyzer.tokenStream(fieldName, reader);
    }
    ShingleFilter filter = new ShingleFilter(wrapped, minShingleSize, maxShingleSize);
    filter.setMinShingleSize(minShingleSize);
    filter.setMaxShingleSize(maxShingleSize);
    filter.setTokenSeparator(tokenSeparator);
    filter.setOutputUnigrams(outputUnigrams);
    filter.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
    return filter;
  }
  
  private class SavedStreams {
    TokenStream wrapped;
    ShingleFilter shingle;
  }
  
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.wrapped = defaultAnalyzer.reusableTokenStream(fieldName, reader);
      streams.shingle = new ShingleFilter(streams.wrapped);
      setPreviousTokenStream(streams);
    } else {
      TokenStream result = defaultAnalyzer.reusableTokenStream(fieldName, reader);
      if (result != streams.wrapped) {
        /* the wrapped analyzer did not, create a new shingle around the new one */
        streams.wrapped = result;
        streams.shingle = new ShingleFilter(streams.wrapped);
      }
    }
    streams.shingle.setMaxShingleSize(maxShingleSize);
    streams.shingle.setMinShingleSize(minShingleSize);
    streams.shingle.setTokenSeparator(tokenSeparator);
    streams.shingle.setOutputUnigrams(outputUnigrams);
    streams.shingle.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
    return streams.shingle;
  }
}
