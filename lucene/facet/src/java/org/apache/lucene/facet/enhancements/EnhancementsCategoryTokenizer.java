package org.apache.lucene.facet.enhancements;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;

import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.streaming.CategoryTokenizer;
import org.apache.lucene.util.Vint8;

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
 * A tokenizer which adds to each category token payload according to the
 * {@link CategoryEnhancement}s defined in the given
 * {@link EnhancementsIndexingParams}.
 * 
 * @lucene.experimental
 */
public class EnhancementsCategoryTokenizer extends CategoryTokenizer {

  /**
   * The data buffer used for payload instance.
   */
  protected byte[] payloadBytes;

  /**
   * The category enhancements to handle
   */
  protected List<CategoryEnhancement> enhancements;

  /**
   * Buffers for enhancement payload bytes
   */
  protected byte[][] enhancementBytes;

  private int nStart;

  /**
   * Constructor.
   * 
   * @param input
   *            The stream of category tokens.
   * @param indexingParams
   *            The indexing params to use.
   * @throws IOException
   */
  public EnhancementsCategoryTokenizer(TokenStream input,
      EnhancementsIndexingParams indexingParams) throws IOException {
    super(input, indexingParams);
    payloadBytes = new byte[Vint8.MAXIMUM_BYTES_NEEDED
        * (indexingParams.getCategoryEnhancements().size() + 1)];
    enhancements = indexingParams.getCategoryEnhancements();
    if (enhancements != null) {
      // create array of bytes per enhancement
      enhancementBytes = new byte[enhancements.size()][];
      // write once the number of enhancements in the payload bytes
      nStart = Vint8.encode(enhancements.size(), payloadBytes, 0);
    }
  }

  @Override
  protected void setPayload() {
    this.payloadAttribute.setPayload(null);
    if (enhancements == null) {
      return;
    }
    // clear previous payload content
    int nBytes = nStart;
    int i = 0;
    int nEnhancementBytes = 0;
    for (CategoryEnhancement enhancement : enhancements) {
      // get payload bytes from each enhancement
      enhancementBytes[i] = enhancement
          .getCategoryTokenBytes(categoryAttribute);
      // write the number of bytes in the payload
      if (enhancementBytes[i] == null) {
        nBytes += Vint8.encode(0, payloadBytes, nBytes);
      } else {
        nBytes += Vint8.encode(enhancementBytes[i].length,
            payloadBytes, nBytes);
        nEnhancementBytes += enhancementBytes[i].length;
      }
      i++;
    }
    if (nEnhancementBytes > 0) {
      // make sure we have space for all bytes
      if (payloadBytes.length < nBytes + nEnhancementBytes) {
        byte[] temp = new byte[(nBytes + nEnhancementBytes) * 2];
        System.arraycopy(payloadBytes, 0, temp, 0, nBytes);
        payloadBytes = temp;
      }
      for (i = 0; i < enhancementBytes.length; i++) {
        // add the enhancement payload bytes after the existing bytes
        if (enhancementBytes[i] != null) {
          System.arraycopy(enhancementBytes[i], 0, payloadBytes,
              nBytes, enhancementBytes[i].length);
          nBytes += enhancementBytes[i].length;
        }
      }
      payload.bytes = payloadBytes;
      payload.offset = 0;
      payload.length = nBytes;
      payloadAttribute.setPayload(payload);
    }
  }
}
