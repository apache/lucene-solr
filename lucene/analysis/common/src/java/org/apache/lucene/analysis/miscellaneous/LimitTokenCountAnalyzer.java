package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;

/**
 * This Analyzer limits the number of tokens while indexing. It is
 * a replacement for the maximum field length setting inside {@link org.apache.lucene.index.IndexWriter}.
 */
public final class LimitTokenCountAnalyzer extends AnalyzerWrapper {
  private final Analyzer delegate;
  private final int maxTokenCount;

  /**
   * Build an analyzer that limits the maximum number of tokens per field.
   */
  public LimitTokenCountAnalyzer(Analyzer delegate, int maxTokenCount) {
    this.delegate = delegate;
    this.maxTokenCount = maxTokenCount;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String fieldName) {
    return delegate;
  }

  @Override
  protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
    return new TokenStreamComponents(components.getTokenizer(),
        new LimitTokenCountFilter(components.getTokenStream(), maxTokenCount));
  }
  
  @Override
  public String toString() {
    return "LimitTokenCountAnalyzer(" + delegate.toString() + ", maxTokenCount=" + maxTokenCount + ")";
  }
}
