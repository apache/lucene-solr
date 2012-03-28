package org.apache.lucene.index;

import org.apache.lucene.index.DocValues.Type;


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

/**
 *  Access to the Fieldable Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Fieldable Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 **/
public final class FieldInfo {
  public final String name;
  public final int number;

  public boolean isIndexed;
  private DocValues.Type docValueType;

  // True if any document indexed term vectors
  public boolean storeTermVector;

  private DocValues.Type normType;
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
            boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValues.Type docValues, DocValues.Type normsType) {
    this.name = name;
    this.isIndexed = isIndexed;
    this.number = number;
    this.docValueType = docValues;
    if (isIndexed) {
      this.storeTermVector = storeTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
      this.indexOptions = indexOptions;
      this.normType = !omitNorms ? normsType : null;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storePayloads = false;
      this.omitNorms = false;
      this.indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
      this.normType = null;
    }
    assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !storePayloads;
  }
  
  @Override
  public FieldInfo clone() {
    return new FieldInfo(name, isIndexed, number, storeTermVector,
                         omitNorms, storePayloads, indexOptions, docValueType, normType);
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

  void setDocValuesType(DocValues.Type type, boolean force) {
    if (docValueType == null || force) {
      docValueType = type;
    } else if (type != docValueType) {
      throw new IllegalArgumentException("DocValues type already set to " + docValueType + " but was: " + type);
    }
  }
  
  /**
   * @return true if this field has any docValues.
   */
  public boolean hasDocValues() {
    return docValueType != null;
  }

  /**
   * @return {@link DocValues.Type} of the docValues. this may be null if the field has no docvalues.
   */
  public DocValues.Type getDocValuesType() {
    return docValueType;
  }
  
  /**
   * @return {@link DocValues.Type} of the norm. this may be null if the field has no norms.
   */
  public DocValues.Type getNormType() {
    return normType;
  }

  public void setStoreTermVectors() {
    storeTermVector = true;
  }

  public void setNormValueType(Type type, boolean force) {
    if (normType == null || force) {
      normType = type;
    } else if (type != normType) {
      throw new IllegalArgumentException("Norm type already set to " + normType);
    }
  }
  
  /**
   * @return true if norms are explicitly omitted for this field
   */
  public boolean omitNorms() {
    return omitNorms;
  }
  
  /**
   * @return true if this field actually has any norms.
   */
  public boolean hasNorms() {
    return isIndexed && !omitNorms && normType != null;
  }
  
}
