package org.apache.lucene.facet.index.streaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.index.CategoryListPayloadStream;
import org.apache.lucene.facet.index.attributes.OrdinalProperty;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.util.PartitionsUtils;
import org.apache.lucene.util.encoding.IntEncoder;

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
 * {@link CategoryListTokenizer} for facet counting
 * 
 * @lucene.experimental
 */
public class CountingListTokenizer extends CategoryListTokenizer {

  /** A table for retrieving payload streams by category-list name. */
  protected HashMap<String, CategoryListPayloadStream> payloadStreamsByName = 
    new HashMap<String, CategoryListPayloadStream>();

  /** An iterator over the payload streams */
  protected Iterator<Entry<String, CategoryListPayloadStream>> payloadStreamIterator;

  public CountingListTokenizer(TokenStream input,
      FacetIndexingParams indexingParams) {
    super(input, indexingParams);
    this.payloadStreamsByName = new HashMap<String, CategoryListPayloadStream>();
  }

  @Override
  protected void handleStartOfInput() throws IOException {
    payloadStreamsByName.clear();
    payloadStreamIterator = null;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (this.categoryAttribute != null) {
        OrdinalProperty ordinalProperty = (OrdinalProperty) this.categoryAttribute
            .getProperty(OrdinalProperty.class);
        if (ordinalProperty != null && legalCategory()) {
          CategoryPath categoryPath = this.categoryAttribute
              .getCategoryPath();
          int ordinal = ordinalProperty.getOrdinal();
          CategoryListPayloadStream payloadStream = getPayloadStream(
              categoryPath, ordinal);
          int partitionSize = indexingParams.getPartitionSize();
          payloadStream.appendIntToStream(ordinal % partitionSize);
        }
      }
      return true;
    }
    if (this.payloadStreamIterator == null) {
      this.handleEndOfInput();
      this.payloadStreamIterator = this.payloadStreamsByName.entrySet()
          .iterator();
    }
    if (this.payloadStreamIterator.hasNext()) {
      Entry<String, CategoryListPayloadStream> entry = this.payloadStreamIterator
          .next();
      String countingListName = entry.getKey();
      int length = countingListName.length();
      this.termAttribute.resizeBuffer(length);
      countingListName.getChars(0, length, termAttribute.buffer(), 0);
      this.termAttribute.setLength(length);
      CategoryListPayloadStream payloadStream = entry.getValue();
      payload.bytes = payloadStream.convertStreamToByteArray();
      payload.offset = 0;
      payload.length = payload.bytes.length;
      this.payloadAttribute.setPayload(payload);
      return true;
    }
    return false;
  }

  /**
   * A method which allows extending classes to filter the categories going
   * into the counting list.
   * 
   * @return By default returns {@code true}, meaning the current category is
   *         to be part of the counting list. For categories that should be
   *         filtered, return {@code false}.
   */
  protected boolean legalCategory() {
    return true;
  }

  protected CategoryListPayloadStream getPayloadStream(
      CategoryPath categoryPath, int ordinal) throws IOException {
    CategoryListParams clParams = this.indexingParams.getCategoryListParams(categoryPath);
    String name = PartitionsUtils.partitionNameByOrdinal(indexingParams, clParams, ordinal); 
    CategoryListPayloadStream fps = payloadStreamsByName.get(name);
    if (fps == null) {
      IntEncoder encoder = clParams.createEncoder();
      fps = new CategoryListPayloadStream(encoder);
      payloadStreamsByName.put(name, fps);
    }
    return fps;
  }

}
