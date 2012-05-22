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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DocValues.Type;

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

  private boolean indexed;
  private DocValues.Type docValueType;

  // True if any document indexed term vectors
  private boolean storeTermVector;

  private DocValues.Type normType;
  private boolean omitNorms; // omit norms associated with indexed fields  
  private IndexOptions indexOptions;
  private boolean storePayloads; // whether this field stores payloads together with term positions

  private Map<String,String> attributes;

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
  public FieldInfo(String name, boolean indexed, int number, boolean storeTermVector, 
            boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValues.Type docValues, DocValues.Type normsType, Map<String,String> attributes) {
    this.name = name;
    this.indexed = indexed;
    this.number = number;
    this.docValueType = docValues;
    if (indexed) {
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
    this.attributes = attributes;
    assert checkConsistency();
    assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !storePayloads;
  }

  private boolean checkConsistency() {
    if (!indexed) {
      assert !storeTermVector;
      assert !storePayloads;
      assert !omitNorms;
      assert normType == null;
      assert indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    } else {
      assert indexOptions != null;
      if (omitNorms) {
        assert normType == null;
      }
    }

    // Cannot store payloads unless positions are indexed:
    assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !this.storePayloads;

    return true;
  }

  // should only be called by FieldInfos#addOrUpdate
  void update(boolean indexed, boolean storeTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions) {

    if (this.indexed != indexed) {
      this.indexed = true;                      // once indexed, always index
    }
    if (indexed) { // if updated field data is not for indexing, leave the updates out
      if (this.storeTermVector != storeTermVector) {
        this.storeTermVector = true;                // once vector, always vector
      }
      if (this.storePayloads != storePayloads) {
        this.storePayloads = true;
      }
      if (this.omitNorms != omitNorms) {
        this.omitNorms = true;                // if one require omitNorms at least once, it remains off for life
        this.normType = null;
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
    assert checkConsistency();
  }

  void setDocValuesType(DocValues.Type type) {
    docValueType = type;
    assert checkConsistency();
  }
  
  /** @return IndexOptions for the field */
  public IndexOptions getIndexOptions() {
    return indexOptions;
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

  void setStoreTermVectors() {
    storeTermVector = true;
    assert checkConsistency();
  }
  
  void setStorePayloads() {
    if (indexed && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      storePayloads = true;
    }
    assert checkConsistency();
  }

  void setNormValueType(Type type) {
    normType = type;
    assert checkConsistency();
  }
  
  /**
   * @return true if norms are explicitly omitted for this field
   */
  public boolean omitsNorms() {
    return omitNorms;
  }
  
  /**
   * @return true if this field actually has any norms.
   */
  public boolean hasNorms() {
    return normType != null;
  }
  
  /**
   * @return true if this field is indexed.
   */
  public boolean isIndexed() {
    return indexed;
  }
  
  /**
   * @return true if any payloads exist for this field.
   */
  public boolean hasPayloads() {
    return storePayloads;
  }
  
  /**
   * @return true if any term vectors exist for this field.
   */
  public boolean hasVectors() {
    return storeTermVector;
  }
  
  /**
   * Get a codec attribute value, or null if it does not exist
   */
  public String getAttribute(String key) {
    if (attributes == null) {
      return null;
    } else {
      return attributes.get(key);
    }
  }
  
  /**
   * Puts a codec attribute value.
   * <p>
   * This is a key-value mapping for the field that the codec can use
   * to store additional metadata, and will be available to the codec
   * when reading the segment via {@link #getAttribute(String)}
   * <p>
   * If a value already exists for the field, it will be replaced with 
   * the new value.
   */
  public String putAttribute(String key, String value) {
    if (attributes == null) {
      attributes = new HashMap<String,String>();
    }
    return attributes.put(key, value);
  }
  
  /**
   * @return internal codec attributes map. May be null if no mappings exist.
   */
  public Map<String,String> attributes() {
    return attributes;
  }
}
