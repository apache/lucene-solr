package org.apache.lucene.index;

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

import java.util.HashMap;
import java.util.Map;

/**
 *  Access to the Field Info file that describes document fields and whether or
 *  not they are indexed. Each segment has a separate Field Info file. Objects
 *  of this class are thread-safe for multiple readers, but only one thread can
 *  be adding documents at a time, with no other reader or writer threads
 *  accessing this object.
 **/

public final class FieldInfo {
  /** Field's name */
  public final String name;
  /** Internal field number */
  public final int number;

  private boolean indexed;
  private DocValuesType docValueType;

  // True if any document indexed term vectors
  private boolean storeTermVector;

  private DocValuesType normType;
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
    /** 
     * Only documents are indexed: term frequencies and positions are omitted.
     * Phrase and other positional queries on the field will throw an exception, and scoring
     * will behave as if any term in the document appears only once.
     */
    // TODO: maybe rename to just DOCS?
    DOCS_ONLY,
    /** 
     * Only documents and term frequencies are indexed: positions are omitted. 
     * This enables normal scoring, except Phrase and other positional queries
     * will throw an exception.
     */  
    DOCS_AND_FREQS,
    /** 
     * Indexes documents, frequencies and positions.
     * This is a typical default for full-text search: full scoring is enabled
     * and positional queries are supported.
     */
    DOCS_AND_FREQS_AND_POSITIONS,
    /** 
     * Indexes documents, frequencies, positions and offsets.
     * Character offsets are encoded alongside the positions. 
     */
    DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
  };
  
  /**
   * DocValues types.
   * Note that DocValues is strongly typed, so a field cannot have different types
   * across different documents.
   */
  public static enum DocValuesType {
    /** 
     * A per-document Number
     */
    NUMERIC,
    /**
     * A per-document byte[].
     */
    BINARY,
    /** 
     * A pre-sorted byte[]. Fields with this type only store distinct byte values 
     * and store an additional offset pointer per document to dereference the shared 
     * byte[]. The stored byte[] is presorted and allows access via document id, 
     * ordinal and by-value.
     */
    SORTED,
    /** 
     * A pre-sorted Set&lt;byte[]&gt;. Fields with this type only store distinct byte values 
     * and store additional offset pointers per document to dereference the shared 
     * byte[]s. The stored byte[] is presorted and allows access via document id, 
     * ordinal and by-value.
     */
    SORTED_SET
  };

  /**
   * Sole Constructor.
   *
   * @lucene.experimental
   */
  public FieldInfo(String name, boolean indexed, int number, boolean storeTermVector, 
            boolean omitNorms, boolean storePayloads, IndexOptions indexOptions, DocValuesType docValues, DocValuesType normsType, Map<String,String> attributes) {
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
      this.indexOptions = null;
      this.normType = null;
    }
    this.attributes = attributes;
    assert checkConsistency();
  }

  private boolean checkConsistency() {
    if (!indexed) {
      assert !storeTermVector;
      assert !storePayloads;
      assert !omitNorms;
      assert normType == null;
      assert indexOptions == null;
    } else {
      assert indexOptions != null;
      if (omitNorms) {
        assert normType == null;
      }
      // Cannot store payloads unless positions are indexed:
      assert indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0 || !this.storePayloads;
    }

    return true;
  }

  void update(IndexableFieldType ft) {
    update(ft.indexed(), false, ft.omitNorms(), false, ft.indexOptions());
  }

  // should only be called by FieldInfos#addOrUpdate
  void update(boolean indexed, boolean storeTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions) {
    //System.out.println("FI.update field=" + name + " indexed=" + indexed + " omitNorms=" + omitNorms + " this.omitNorms=" + this.omitNorms);
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
        if (this.indexOptions == null) {
          this.indexOptions = indexOptions;
        } else {
          // downgrade
          this.indexOptions = this.indexOptions.compareTo(indexOptions) < 0 ? this.indexOptions : indexOptions;
        }
        if (this.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
          // cannot store payloads if we don't store positions:
          this.storePayloads = false;
        }
      }
    }
    assert checkConsistency();
  }

  void setDocValuesType(DocValuesType type) {
    if (docValueType != null && docValueType != type) {
      throw new IllegalArgumentException("cannot change DocValues type from " + docValueType + " to " + type + " for field \"" + name + "\"");
    }
    docValueType = type;
    assert checkConsistency();
  }
  
  /** Returns IndexOptions for the field, or null if the field is not indexed */
  public IndexOptions getIndexOptions() {
    return indexOptions;
  }
  
  /**
   * Returns true if this field has any docValues.
   */
  public boolean hasDocValues() {
    return docValueType != null;
  }

  /**
   * Returns {@link DocValuesType} of the docValues. this may be null if the field has no docvalues.
   */
  public DocValuesType getDocValuesType() {
    return docValueType;
  }
  
  /**
   * Returns {@link DocValuesType} of the norm. this may be null if the field has no norms.
   */
  public DocValuesType getNormType() {
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

  void setNormValueType(DocValuesType type) {
    if (normType != null && normType != type) {
      throw new IllegalArgumentException("cannot change Norm type from " + normType + " to " + type + " for field \"" + name + "\"");
    }
    normType = type;
    assert checkConsistency();
  }
  
  /**
   * Returns true if norms are explicitly omitted for this field
   */
  public boolean omitsNorms() {
    return omitNorms;
  }
  
  /**
   * Returns true if this field actually has any norms.
   */
  public boolean hasNorms() {
    return normType != null;
  }
  
  /**
   * Returns true if this field is indexed.
   */
  public boolean isIndexed() {
    return indexed;
  }
  
  /**
   * Returns true if any payloads exist for this field.
   */
  public boolean hasPayloads() {
    return storePayloads;
  }
  
  /**
   * Returns true if any term vectors exist for this field.
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
   * Returns internal codec attributes map. May be null if no mappings exist.
   */
  public Map<String,String> attributes() {
    return attributes;
  }
}
