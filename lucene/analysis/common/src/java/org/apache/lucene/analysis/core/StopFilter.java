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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.TokenStream;

/**
 * Removes stop words from a token stream.
 * <p>
 * This class moved to Lucene Core, but a reference in the {@code analysis/common} module
 * is preserved for documentation purposes and consistency with filter factory.
 * @see org.apache.lucene.analysis.StopFilter
 * @see StopFilterFactory
 */
public final class StopFilter extends org.apache.lucene.analysis.StopFilter {

  /**
   * Constructs a filter which removes words from the input TokenStream that are
   * named in the Set.
   * 
   * @param in
   *          Input stream
   * @param stopWords
   *          A {@link org.apache.lucene.analysis.util.CharArraySet} representing the stopwords.
   * @see #makeStopSet(java.lang.String...)
   */
  public StopFilter(TokenStream in, org.apache.lucene.analysis.util.CharArraySet stopWords) {
    super(in, stopWords);
  }

  /**
   * Constructs a filter which removes words from the input TokenStream that are
   * named in the Set.
   * 
   * @param in
   *          Input stream
   * @param stopWords
   *          A {@link org.apache.lucene.analysis.CharArraySet} representing the stopwords.
   * @see #makeStopSet(java.lang.String...)
   */
  public StopFilter(TokenStream in, org.apache.lucene.analysis.CharArraySet stopWords) {
    super(in, stopWords);
  }

}
