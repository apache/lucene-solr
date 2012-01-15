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

/** @lucene.experimental */
public final class FieldInfo {
  public final String name;
  public final int number;

  public boolean isIndexed;
  private DocValues.Type docValues;

  // True if any document indexed term vectors
  public boolean storeTermVector;

  public boolean omitNorms; // omit norms associated with indexed fields  
  public IndexOptions indexOptions;
  public boolean storePayloads; // whether this field stores payloads together with term positions

  /**
   * Controls how much information is stored in the postings lists.
   * @lucene.experimental
   */
  public static enum IndexOptions { 
    // NOTE: order is important here; FieldInfo uses this
    // order to merge two conflicting IndexOptions (always
    // "downgrades" by picking the lowest).
    /** only documents are indexed: term frequencies and positions are omitted */
    // TODO: maybe rename to just DOCS?
    DOCS_ONLY,
    /** only documents and term frequencies are indexed: positions are omitted */  
    DOCS_AND_FREQS,
    /** documents, frequencies and positions */
    DOCS_AND_FREQS_AND_POSITIONS,
    /** documents, frequencies, positions and offsets */
    DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
  };

  /**
   * @lucene.experimental
   */
  public FieldInfo(String name, boolean isIndexed, int number, boolean storeTermVector, 
            boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValues.Type docValues) {
    this.name = name;
    this.isIndexed = isIndexed;
    this.number = number;
    this.docValues = docValues;
    if (isIndexed) {
      this.storeTermVector = storeTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
      this.indexOptions = indexOptions;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storePayloads = false;
      this.omitNorms = false;
      this.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    }
    assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !storePayloads;
  }
  
  @Override
  public Object clone() {
    return new FieldInfo(name, isIndexed, number, storeTermVector,
                         omitNorms, storePayloads, indexOptions, docValues);
  }

  // should only be called by FieldInfos#addOrUpdate
  void update(boolean isIndexed, boolean storeTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions) {

    if (this.isIndexed != isIndexed) {
      this.isIndexed = true;                      // once indexed, always index
    }
    if (isIndexed) { // if updated field data is not for indexing, leave the updates out
      if (this.storeTermVector != storeTermVector) {
        this.storeTermVector = true;                // once vector, always vector
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
        if (this.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
          // cannot store payloads if we don't store positions:
          this.storePayloads = false;
        }
      }
    }
    assert this.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !this.storePayloads;
  }

  void setDocValuesType(DocValues.Type v) {
    if (docValues == null) {
      docValues = v;
    }
  }
  
  public void resetDocValuesType(DocValues.Type v) {
    if (docValues != null) {
      docValues = v;
    }
  }
  
  public boolean hasDocValues() {
    return docValues != null;
  }

  public DocValues.Type getDocValuesType() {
    return docValues;
  }

  public void setStoreTermVectors() {
    storeTermVector = true;
  }
}
