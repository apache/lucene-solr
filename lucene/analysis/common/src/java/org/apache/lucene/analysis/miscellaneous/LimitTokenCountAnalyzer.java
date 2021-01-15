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
package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * This Analyzer limits the number of tokens while indexing. It is a replacement for the maximum
 * field length setting inside {@link org.apache.lucene.index.IndexWriter}.
 *
 * @see LimitTokenCountFilter
 * @since 3.1
 */
public final class LimitTokenCountAnalyzer extends AnalyzerWrapper {
  private final Analyzer delegate;
  private final int maxTokenCount;
  private final boolean consumeAllTokens;

  /**
   * Build an analyzer that limits the maximum number of tokens per field. This analyzer will not
   * consume any tokens beyond the maxTokenCount limit
   *
   * @see #LimitTokenCountAnalyzer(Analyzer,int,boolean)
   */
  public LimitTokenCountAnalyzer(Analyzer delegate, int maxTokenCount) {
    this(delegate, maxTokenCount, false);
  }
  /**
   * Build an analyzer that limits the maximum number of tokens per field.
   *
   * @param delegate the analyzer to wrap
   * @param maxTokenCount max number of tokens to produce
   * @param consumeAllTokens whether all tokens from the delegate should be consumed even if
   *     maxTokenCount is reached.
   */
  public LimitTokenCountAnalyzer(Analyzer delegate, int maxTokenCount, boolean consumeAllTokens) {
    super(delegate.getReuseStrategy());
    this.delegate = delegate;
    this.maxTokenCount = maxTokenCount;
    this.consumeAllTokens = consumeAllTokens;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return delegate;
  }

  @Override
  protected TokenStreamComponents wrapComponents(
      String fieldName, TokenStreamComponents components) {
    return new TokenStreamComponents(
        components.getSource(),
        new LimitTokenCountFilter(components.getTokenStream(), maxTokenCount, consumeAllTokens));
  }

  @Override
  public String toString() {
    return "LimitTokenCountAnalyzer("
        + delegate.toString()
        + ", maxTokenCount="
        + maxTokenCount
        + ", consumeAllTokens="
        + consumeAllTokens
        + ")";
  }
}
