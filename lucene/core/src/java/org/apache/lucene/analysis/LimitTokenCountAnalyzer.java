package org.apache.lucene.analysis;

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

import org.apache.lucene.document.Fieldable;

import java.io.Reader;
import java.io.IOException;

/**
 * This Analyzer limits the number of tokens while indexing. It is
 * a replacement for the maximum field length setting inside {@link org.apache.lucene.index.IndexWriter}.
 */
public final class LimitTokenCountAnalyzer extends Analyzer {
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
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return new LimitTokenCountFilter(
      delegate.tokenStream(fieldName, reader), maxTokenCount
    );
  }
  
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    return new LimitTokenCountFilter(
      delegate.reusableTokenStream(fieldName, reader), maxTokenCount
    );
  }
  
  @Override
  public int getPositionIncrementGap(String fieldName) {
    return delegate.getPositionIncrementGap(fieldName);
  }

  @Override
  public int getOffsetGap(Fieldable field) {
    return delegate.getOffsetGap(field);
  }
  
  @Override
  public String toString() {
    return "LimitTokenCountAnalyzer(" + delegate.toString() + ", maxTokenCount=" + maxTokenCount + ")";
  }
}
