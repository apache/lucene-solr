package org.apache.lucene.facet.index.streaming;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;

import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.index.attributes.CategoryAttribute;
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
 * A base class for all token filters which add term and payload attributes to
 * tokens and are to be used in {@link CategoryDocumentBuilder}. Contains three
 * attributes: {@link CategoryAttribute}, {@link CharTermAttribute} and
 * {@link PayloadAttribute}.
 * 
 * @lucene.experimental
 */
public abstract class CategoryTokenizerBase extends TokenFilter {

  /** The stream's category attributes. */
  protected CategoryAttribute categoryAttribute;

  /** The stream's payload attribute. */
  protected PayloadAttribute payloadAttribute;

  /** The stream's term attribute. */
  protected CharTermAttribute termAttribute;

  /** The object used for constructing payloads. */
  protected BytesRef payload = new BytesRef();

  /** Indexing params for creating term text **/
  protected FacetIndexingParams indexingParams;

  /**
   * Constructor.
   * 
   * @param input
   *            The input stream, either {@link CategoryParentsStream} or an
   *            extension of {@link CategoryTokenizerBase}.
   * @param indexingParams
   *            The indexing params to use.
   */
  public CategoryTokenizerBase(TokenStream input,
      FacetIndexingParams indexingParams) {
    super(input);
    this.categoryAttribute = this.addAttribute(CategoryAttribute.class);
    this.termAttribute = this.addAttribute(CharTermAttribute.class);
    this.payloadAttribute = this.addAttribute(PayloadAttribute.class);
    this.indexingParams = indexingParams;
  }

  @Override
  public abstract boolean incrementToken() throws IOException;

}
