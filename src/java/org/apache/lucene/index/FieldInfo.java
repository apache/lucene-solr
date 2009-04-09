package org.apache.lucene.index;

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

final class FieldInfo {
  String name;
  boolean isIndexed;
  int number;

  // true if term vector for this field should be stored
  boolean storeTermVector;
  boolean storeOffsetWithTermVector;
  boolean storePositionWithTermVector;

  boolean omitNorms; // omit norms associated with indexed fields  
  boolean omitTermFreqAndPositions;
  
  boolean storePayloads; // whether this field stores payloads together with term positions

  FieldInfo(String na, boolean tk, int nu, boolean storeTermVector, 
            boolean storePositionWithTermVector,  boolean storeOffsetWithTermVector, 
            boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions) {
    name = na;
    isIndexed = tk;
    number = nu;
    if (isIndexed) {
      this.storeTermVector = storeTermVector;
      this.storeOffsetWithTermVector = storeOffsetWithTermVector;
      this.storePositionWithTermVector = storePositionWithTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
      this.omitTermFreqAndPositions = omitTermFreqAndPositions;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storeOffsetWithTermVector = false;
      this.storePositionWithTermVector = false;
      this.storePayloads = false;
      this.omitNorms = true;
      this.omitTermFreqAndPositions = false;
    }
  }

  public Object clone() {
    return new FieldInfo(name, isIndexed, number, storeTermVector, storePositionWithTermVector,
                         storeOffsetWithTermVector, omitNorms, storePayloads, omitTermFreqAndPositions);
  }

  void update(boolean isIndexed, boolean storeTermVector, boolean storePositionWithTermVector, 
              boolean storeOffsetWithTermVector, boolean omitNorms, boolean storePayloads, boolean omitTermFreqAndPositions) {
    if (this.isIndexed != isIndexed) {
      this.isIndexed = true;                      // once indexed, always index
    }
    if (isIndexed) { // if updated field data is not for indexing, leave the updates out
      if (this.storeTermVector != storeTermVector) {
        this.storeTermVector = true;                // once vector, always vector
      }
      if (this.storePositionWithTermVector != storePositionWithTermVector) {
        this.storePositionWithTermVector = true;                // once vector, always vector
      }
      if (this.storeOffsetWithTermVector != storeOffsetWithTermVector) {
        this.storeOffsetWithTermVector = true;                // once vector, always vector
      }
      if (this.storePayloads != storePayloads) {
        this.storePayloads = true;
      }
      if (this.omitNorms != omitNorms) {
        this.omitNorms = false;                // once norms are stored, always store
      }
      if (this.omitTermFreqAndPositions != omitTermFreqAndPositions) {
        this.omitTermFreqAndPositions = true;                // if one require omitTermFreqAndPositions at least once, it remains off for life
      }
    }
  }
}
