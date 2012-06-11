package org.apache.lucene.facet.index.streaming;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.index.params.FacetIndexingParams;

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

/**
 * A base class for category list tokenizers, which add category list tokens to
 * category streams.
 * 
 * @lucene.experimental
 */
public abstract class CategoryListTokenizer extends CategoryTokenizerBase {

  /**
   * @see CategoryTokenizerBase#CategoryTokenizerBase(TokenStream, FacetIndexingParams)
   */
  public CategoryListTokenizer(TokenStream input,
      FacetIndexingParams indexingParams) {
    super(input, indexingParams);
  }

  /**
   * A method invoked once when the input stream begins, for subclass-specific
   * processing. Subclass implementations must invoke this one, too!
   */
  protected void handleStartOfInput() throws IOException {
    // In this class, we do nothing.
  }

  /**
   * A method invoked once when the input stream ends, for subclass-specific
   * processing.
   */
  protected void handleEndOfInput() throws IOException {
    // In this class, we do nothing.
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    handleStartOfInput();
  }

  @Override
  public abstract boolean incrementToken() throws IOException;

}
