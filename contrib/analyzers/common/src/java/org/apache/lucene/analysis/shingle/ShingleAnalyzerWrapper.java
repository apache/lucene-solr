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
public class ShingleAnalyzerWrapper extends Analyzer {

  protected Analyzer defaultAnalyzer;
  protected int maxShingleSize = 2;
  protected boolean outputUnigrams = true;

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer) {
    super();
    this.defaultAnalyzer = defaultAnalyzer;
    setOverridesTokenStreamMethod(ShingleAnalyzerWrapper.class);
  }

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer, int maxShingleSize) {
    this(defaultAnalyzer);
    this.maxShingleSize = maxShingleSize;
  }

  /**
   * Wraps {@link StandardAnalyzer}. 
   */
  public ShingleAnalyzerWrapper(Version matchVersion) {
    super();
    this.defaultAnalyzer = new StandardAnalyzer(matchVersion);
    setOverridesTokenStreamMethod(ShingleAnalyzerWrapper.class);
  }

  /**
   * Wraps {@link StandardAnalyzer}. 
   */
  public ShingleAnalyzerWrapper(Version matchVersion, int nGramSize) {
    this(matchVersion);
    this.maxShingleSize = nGramSize;
  }

  /**
   * The max shingle (ngram) size
   * 
   * @return The max shingle (ngram) size
   */
  public int getMaxShingleSize() {
    return maxShingleSize;
  }

  /**
   * Set the maximum size of output shingles
   * 
   * @param maxShingleSize max shingle size
   */
  public void setMaxShingleSize(int maxShingleSize) {
    this.maxShingleSize = maxShingleSize;
  }

  public boolean isOutputUnigrams() {
    return outputUnigrams;
  }

  /**
   * Shall the filter pass the original tokens (the "unigrams") to the output
   * stream?
   * 
   * @param outputUnigrams Whether or not the filter shall pass the original
   *        tokens to the output stream
   */
  public void setOutputUnigrams(boolean outputUnigrams) {
    this.outputUnigrams = outputUnigrams;
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream wrapped;
    try {
      wrapped = defaultAnalyzer.reusableTokenStream(fieldName, reader);
    } catch (IOException e) {
      wrapped = defaultAnalyzer.tokenStream(fieldName, reader);
    }
    ShingleFilter filter = new ShingleFilter(wrapped);
    filter.setMaxShingleSize(maxShingleSize);
    filter.setOutputUnigrams(outputUnigrams);
    return filter;
  }
  
  private class SavedStreams {
    TokenStream wrapped;
    ShingleFilter shingle;
  };
  
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    if (overridesTokenStreamMethod) {
      // LUCENE-1678: force fallback to tokenStream() if we
      // have been subclassed and that subclass overrides
      // tokenStream but not reusableTokenStream
      return tokenStream(fieldName, reader);
    }
    
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.wrapped = defaultAnalyzer.reusableTokenStream(fieldName, reader);
      streams.shingle = new ShingleFilter(streams.wrapped);
      setPreviousTokenStream(streams);
    } else {
      TokenStream result = defaultAnalyzer.reusableTokenStream(fieldName, reader);
      if (result == streams.wrapped) {
        /* the wrapped analyzer reused the stream */
        streams.shingle.reset(); 
      } else {
        /* the wrapped analyzer did not, create a new shingle around the new one */
        streams.wrapped = result;
        streams.shingle = new ShingleFilter(streams.wrapped);
      }
    }
    streams.shingle.setMaxShingleSize(maxShingleSize);
    streams.shingle.setOutputUnigrams(outputUnigrams);
    return streams.shingle;
  }
}
