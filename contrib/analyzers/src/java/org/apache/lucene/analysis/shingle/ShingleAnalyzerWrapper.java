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

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * A ShingleAnalyzerWrapper wraps a ShingleFilter around another analyzer. A
 * shingle is another namefor a token based n-gram.
 */
public class ShingleAnalyzerWrapper extends Analyzer {

  protected Analyzer defaultAnalyzer;
  protected int maxShingleSize = 2;
  protected boolean outputUnigrams = true;

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer) {
    super();
    this.defaultAnalyzer = defaultAnalyzer;
  }

  public ShingleAnalyzerWrapper(Analyzer defaultAnalyzer, int maxShingleSize) {
    this(defaultAnalyzer);
    this.maxShingleSize = maxShingleSize;
  }

  /**
   * Wraps {@link StandardAnalyzer}. 
   */
  public ShingleAnalyzerWrapper() {
    super();
    this.defaultAnalyzer = new StandardAnalyzer();
  }

  public ShingleAnalyzerWrapper(int nGramSize) {
    this();
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

  public TokenStream tokenStream(String fieldName, Reader reader) {
    ShingleFilter filter = new ShingleFilter(defaultAnalyzer.tokenStream(
        fieldName, reader));
    filter.setMaxShingleSize(maxShingleSize);
    filter.setOutputUnigrams(outputUnigrams);
    return filter;
  }
}
