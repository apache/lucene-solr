package org.apache.lucene.facet.index.streaming;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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
 * Basic class for setting the {@link CharTermAttribute}s and
 * {@link PayloadAttribute}s of category tokens.
 * 
 * @lucene.experimental
 */
public class CategoryTokenizer extends CategoryTokenizerBase {

  /**
   * @see CategoryTokenizerBase#CategoryTokenizerBase(TokenStream,
   *      FacetIndexingParams)
   */
  public CategoryTokenizer(TokenStream input,
      FacetIndexingParams indexingParams) {
    super(input, indexingParams);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (categoryAttribute != null && categoryAttribute.getCategoryPath() != null) {
        CategoryPath categoryPath = categoryAttribute.getCategoryPath();
        char[] termBuffer = termAttribute.resizeBuffer(categoryPath.charsNeededForFullPath());
        int nChars = indexingParams.drillDownTermText(categoryPath, termBuffer);
        termAttribute.setLength(nChars);
        setPayload();
      }
      return true;
    }
    return false;
  }

  /**
   * Set the payload of the current category token.
   */
  protected void setPayload() {
  }

}
