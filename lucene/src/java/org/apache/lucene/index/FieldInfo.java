package org.apache.lucene.index;

import org.apache.lucene.index.values.ValueType;

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

/** @lucene.experimental */
public final class FieldInfo {
  public final String name;
  public final int number;

  public boolean isIndexed;
  private ValueType docValues;


  // true if term vector for this field should be stored
  public boolean storeTermVector;
  public boolean storeOffsetWithTermVector;
  public boolean storePositionWithTermVector;

  public boolean omitNorms; // omit norms associated with indexed fields  
  public IndexOptions indexOptions;

  public boolean storePayloads; // whether this field stores payloads together with term positions

  /**
   * Controls how much information is stored in the postings lists.
   * @lucene.experimental
   */
  public static enum IndexOptions { 
    /** only documents are indexed: term frequencies and positions are omitted */
    // TODO: maybe rename to just DOCS?
    DOCS_ONLY,
    /** only documents and term frequencies are indexed: positions are omitted */  
    DOCS_AND_FREQS,
    /** full postings: documents, frequencies, and positions */
    DOCS_AND_FREQS_AND_POSITIONS 
  };

  /**
   * @lucene.experimental
   */
  public FieldInfo(String name, boolean isIndexed, int number, boolean storeTermVector, 
            boolean storePositionWithTermVector,  boolean storeOffsetWithTermVector, 
            boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, ValueType docValues) {
    this.name = name;
    this.isIndexed = isIndexed;
    this.number = number;
    this.docValues = docValues;
    if (isIndexed) {
      this.storeTermVector = storeTermVector;
      this.storeOffsetWithTermVector = storeOffsetWithTermVector;
      this.storePositionWithTermVector = storePositionWithTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
      this.indexOptions = indexOptions;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storeOffsetWithTermVector = false;
      this.storePositionWithTermVector = false;
      this.storePayloads = false;
      this.omitNorms = false;
      this.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    }
    assert indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS || !storePayloads;
  }
  
  @Override
  public Object clone() {
    FieldInfo clone = new FieldInfo(name, isIndexed, number, storeTermVector, storePositionWithTermVector,
                         storeOffsetWithTermVector, omitNorms, storePayloads, indexOptions, docValues);
    return clone;
  }

  // should only be called by FieldInfos#addOrUpdate
  void update(boolean isIndexed, boolean storeTermVector, boolean storePositionWithTermVector, 
              boolean storeOffsetWithTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions) {

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
        this.omitNorms = true;                // if one require omitNorms at least once, it remains off for life
      }
      if (this.indexOptions != indexOptions) {
        // downgrade
        this.indexOptions = this.indexOptions.compareTo(indexOptions) < 0 ? this.indexOptions : indexOptions;
        this.storePayloads = false;
      }
    }
    assert this.indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS || !this.storePayloads;
  }

  void setDocValuesType(ValueType v) {
    if (docValues == null) {
      docValues = v;
    }
  }
  
  public void resetDocValuesType(ValueType v) {
    if (docValues != null) {
      docValues = v;
    }
  }
  
  public boolean hasDocValues() {
    return docValues != null;
  }

  public ValueType getDocValuesType() {
    return docValues;
  }

  public void setStoreTermVectors(boolean withPositions, boolean withOffsets) {
    storeTermVector = true;
    storePositionWithTermVector |= withPositions;
    storeOffsetWithTermVector |= withOffsets;
  }
}
