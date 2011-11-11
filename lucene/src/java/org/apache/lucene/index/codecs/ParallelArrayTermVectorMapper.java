package org.apache.lucene.index.codecs;

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

import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.apache.lucene.util.BytesRef;

/**
 * Used by {@link DefaultTermVectorsReader} to retrieve term vectors.
 */
// Models the existing parallel array structure
public class ParallelArrayTermVectorMapper extends TermVectorMapper {

  private BytesRef[] terms;
  private int[] termFreqs;
  private int positions[][];
  private TermVectorOffsetInfo offsets[][];
  private int currentPosition;
  private boolean storingOffsets;
  private boolean storingPositions;
  private String field;

  @Override
  public void setExpectations(String field, int numTerms, boolean storeOffsets, boolean storePositions) {
    this.field = field;
    terms = new BytesRef[numTerms];
    termFreqs = new int[numTerms];
    this.storingOffsets = storeOffsets;
    this.storingPositions = storePositions;
    if(storePositions)
      this.positions = new int[numTerms][];
    if(storeOffsets)
      this.offsets = new TermVectorOffsetInfo[numTerms][];
  }

  @Override
  public void map(BytesRef term, int frequency, TermVectorOffsetInfo[] offsets, int[] positions) {
    terms[currentPosition] = term;
    termFreqs[currentPosition] = frequency;
    if (storingOffsets)
    {
      this.offsets[currentPosition] = offsets;
    }
    if (storingPositions)
    {
      this.positions[currentPosition] = positions; 
    }
    currentPosition++;
  }

  /**
   * Construct the vector
   * @return The {@link TermFreqVector} based on the mappings.
   */
  public TermFreqVector materializeVector() {
    SegmentTermVector tv = null;
    if (field != null && terms != null) {
      if (storingPositions || storingOffsets) {
        tv = new SegmentTermPositionVector(field, terms, termFreqs, positions, offsets);
      } else {
        tv = new SegmentTermVector(field, terms, termFreqs);
      }
    }
    return tv;
  }
}