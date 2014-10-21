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

  private DocValuesType docValueType;

  // True if any document indexed term vectors
  private boolean storeTermVector;

  private boolean omitNorms; // omit norms associated with indexed fields  

  private IndexOptions indexOptions;
  private boolean storePayloads; // whether this field stores payloads together with term positions

  private Map<String,String> attributes;

  private long dvGen;

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
  }
  
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
     * A per-document byte[].  Values may be larger than
     * 32766 bytes, but different codecs may enforce their own limits.
     */
    BINARY,
    /** 
     * A pre-sorted byte[]. Fields with this type only store distinct byte values 
     * and store an additional offset pointer per document to dereference the shared 
     * byte[]. The stored byte[] is presorted and allows access via document id, 
     * ordinal and by-value.  Values must be <= 32766 bytes.
     */
    SORTED,
    /** 
     * A pre-sorted Number[]. Fields with this type store numeric values in sorted
     * order according to {@link Long#compare(long, long)}.
     */
    SORTED_NUMERIC,
    /** 
     * A pre-sorted Set&lt;byte[]&gt;. Fields with this type only store distinct byte values 
     * and store additional offset pointers per document to dereference the shared 
     * byte[]s. The stored byte[] is presorted and allows access via document id, 
     * ordinal and by-value.  Values must be <= 32766 bytes.
     */
    SORTED_SET
  }

  /**
   * Sole constructor.
   *
   * @lucene.experimental
   */
  public FieldInfo(String name, int number, boolean storeTermVector, boolean omitNorms, 
      boolean storePayloads, IndexOptions indexOptions, DocValuesType docValues,
      long dvGen, Map<String,String> attributes) {
    this.name = name;
    this.number = number;
    this.docValueType = docValues;
    if (indexOptions != null) {
      this.storeTermVector = storeTermVector;
      this.storePayloads = storePayloads;
      this.omitNorms = omitNorms;
      this.indexOptions = indexOptions;
    } else { // for non-indexed fields, leave defaults
      this.storeTermVector = false;
      this.storePayloads = false;
      this.omitNorms = false;
      this.indexOptions = null;
    }
    this.dvGen = dvGen;
    this.attributes = attributes;
    assert checkConsistency();
  }

  /** 
   * Performs internal consistency checks.
   * Always returns true (or throws IllegalStateException) 
   */
  public boolean checkConsistency() {
    if (indexOptions != null) {
      // Cannot store payloads unless positions are indexed:
      if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0 && storePayloads) {
        throw new IllegalStateException("indexed field '" + name + "' cannot have payloads without positions");
      }
    } else {
      if (storeTermVector) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot store term vectors");
      }
      if (storePayloads) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot store payloads");
      }
      if (omitNorms) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot omit norms");
      }
      if (indexOptions != null) {
        throw new IllegalStateException("non-indexed field '" + name + "' cannot have index options");
      }
    }
    
    if (dvGen != -1 && docValueType == null) {
      throw new IllegalStateException("field '" + name + "' cannot have a docvalues update generation without having docvalues");
    }

    return true;
  }

  void update(IndexableFieldType ft) {
    update(false, ft.omitNorms(), false, ft.indexOptions());
  }

  // should only be called by FieldInfos#addOrUpdate
  void update(boolean storeTermVector, boolean omitNorms, boolean storePayloads, IndexOptions indexOptions) {
    //System.out.println("FI.update field=" + name + " indexed=" + indexed + " omitNorms=" + omitNorms + " this.omitNorms=" + this.omitNorms);
    if (this.indexOptions != indexOptions) {
      if (this.indexOptions == null) {
        this.indexOptions = indexOptions;
      } else if (indexOptions != null) {
        // downgrade
        this.indexOptions = this.indexOptions.compareTo(indexOptions) < 0 ? this.indexOptions : indexOptions;
      }
    }

    if (this.indexOptions != null) { // if updated field data is not for indexing, leave the updates out
      this.storeTermVector |= storeTermVector;                // once vector, always vector
      this.storePayloads |= storePayloads;

      // Awkward: only drop norms if incoming update is indexed:
      if (indexOptions != null && this.omitNorms != omitNorms) {
        this.omitNorms = true;                // if one require omitNorms at least once, it remains off for life
      }
    }
    if (this.indexOptions == null || this.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
      // cannot store payloads if we don't store positions:
      this.storePayloads = false;
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
  
  /** Sets the docValues generation of this field. */
  void setDocValuesGen(long dvGen) {
    this.dvGen = dvGen;
    assert checkConsistency();
  }
  
  /**
   * Returns the docValues generation of this field, or -1 if no docValues
   * updates exist for it.
   */
  public long getDocValuesGen() {
    return dvGen;
  }
  
  void setStoreTermVectors() {
    storeTermVector = true;
    assert checkConsistency();
  }
  
  void setStorePayloads() {
    if (indexOptions != null && indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
      storePayloads = true;
    }
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
    return isIndexed() && omitNorms == false;
  }
  
  /**
   * Returns true if this field is indexed (has non-null {@link #getIndexOptions}).
   */
  public boolean isIndexed() {
    return indexOptions != null;
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
      attributes = new HashMap<>();
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
