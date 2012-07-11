package org.apache.lucene.facet.enhancements.association;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.CategoryListPayloadStream;
import org.apache.lucene.facet.index.attributes.OrdinalProperty;
import org.apache.lucene.facet.index.streaming.CategoryListTokenizer;
import org.apache.lucene.util.encoding.SimpleIntEncoder;

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
 * Tokenizer for associations of a category
 * 
 * @lucene.experimental
 */
public class AssociationListTokenizer extends CategoryListTokenizer {

  protected CategoryListPayloadStream payloadStream;

  private String categoryListTermText;

  public AssociationListTokenizer(TokenStream input,
      EnhancementsIndexingParams indexingParams, CategoryEnhancement enhancement) {
    super(input, indexingParams);
    categoryListTermText = enhancement.getCategoryListTermText();
  }

  @Override
  protected void handleStartOfInput() throws IOException {
    payloadStream = null;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (categoryAttribute != null) {
        AssociationProperty associationProperty = AssociationEnhancement
            .getAssociationProperty(categoryAttribute);
        if (associationProperty != null
            && associationProperty.hasBeenSet()) {
          OrdinalProperty ordinalProperty = (OrdinalProperty) categoryAttribute
              .getProperty(OrdinalProperty.class);
          if (ordinalProperty == null) {
            throw new IOException(
                "Error: Association without ordinal");
          }

          if (payloadStream == null) {
            payloadStream = new CategoryListPayloadStream(
                new SimpleIntEncoder());
          }
          payloadStream.appendIntToStream(ordinalProperty
              .getOrdinal());
          payloadStream.appendIntToStream(associationProperty
              .getAssociation());
        }
      }
      return true;
    }
    if (payloadStream != null) {
      termAttribute.setEmpty().append(categoryListTermText);
      payload.bytes = payloadStream.convertStreamToByteArray();
      payload.offset = 0;
      payload.length = payload.bytes.length;
      payloadAttribute.setPayload(payload);
      payloadStream = null;
      return true;
    }
    return false;
  }

}
