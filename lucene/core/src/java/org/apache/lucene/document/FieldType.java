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
package org.apache.lucene.document;


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer; // javadocs
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.PointValues;

/**
 * Describes the properties of a field.
 */
public class FieldType implements IndexableFieldType  {

  private boolean stored;
  private boolean tokenized = true;
  private boolean storeTermVectors;
  private boolean storeTermVectorOffsets;
  private boolean storeTermVectorPositions;
  private boolean storeTermVectorPayloads;
  private boolean omitNorms;
  private IndexOptions indexOptions = IndexOptions.NONE;
  private boolean frozen;
  private DocValuesType docValuesType = DocValuesType.NONE;
  private int dimensionCount;
  private int indexDimensionCount;
  private int dimensionNumBytes;
  private Map<String, String> attributes;

  /**
   * Create a new mutable FieldType with all of the properties from <code>ref</code>
   */
  public FieldType(IndexableFieldType ref) {
    this.stored = ref.stored();
    this.tokenized = ref.tokenized();
    this.storeTermVectors = ref.storeTermVectors();
    this.storeTermVectorOffsets = ref.storeTermVectorOffsets();
    this.storeTermVectorPositions = ref.storeTermVectorPositions();
    this.storeTermVectorPayloads = ref.storeTermVectorPayloads();
    this.omitNorms = ref.omitNorms();
    this.indexOptions = ref.indexOptions();
    this.docValuesType = ref.docValuesType();
    this.dimensionCount = ref.pointDimensionCount();
    this.indexDimensionCount = ref.pointIndexDimensionCount();
    this.dimensionNumBytes = ref.pointNumBytes();
    if (ref.getAttributes() != null) {
      this.attributes = new HashMap<>(ref.getAttributes());
    }
    // Do not copy frozen!
  }
  
  /**
   * Create a new FieldType with default properties.
   */
  public FieldType() {
  }

  /**
   * Throws an exception if this FieldType is frozen. Subclasses should
   * call this within setters for additional state.
   */
  protected void checkIfFrozen() {
    if (frozen) {
      throw new IllegalStateException("this FieldType is already frozen and cannot be changed");
    }
  }

  /**
   * Prevents future changes. Note, it is recommended that this is called once
   * the FieldTypes's properties have been set, to prevent unintentional state
   * changes.
   */
  public void freeze() {
    this.frozen = true;
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>false</code>.
   * @see #setStored(boolean)
   */
  @Override
  public boolean stored() {
    return this.stored;
  }
  
  /**
   * Set to <code>true</code> to store this field.
   * @param value true if this field should be stored.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #stored()
   */
  public void setStored(boolean value) {
    checkIfFrozen();
    this.stored = value;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>true</code>.
   * @see #setTokenized(boolean)
   */
  public boolean tokenized() {
    return this.tokenized;
  }
  
  /**
   * Set to <code>true</code> to tokenize this field's contents via the 
   * configured {@link Analyzer}.
   * @param value true if this field should be tokenized.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #tokenized()
   */
  public void setTokenized(boolean value) {
    checkIfFrozen();
    this.tokenized = value;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>false</code>. 
   * @see #setStoreTermVectors(boolean)
   */
  @Override
  public boolean storeTermVectors() {
    return this.storeTermVectors;
  }
  
  /**
   * Set to <code>true</code> if this field's indexed form should be also stored 
   * into term vectors.
   * @param value true if this field should store term vectors.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #storeTermVectors()
   */
  public void setStoreTermVectors(boolean value) {
    checkIfFrozen();
    this.storeTermVectors = value;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>false</code>.
   * @see #setStoreTermVectorOffsets(boolean)
   */
  @Override
  public boolean storeTermVectorOffsets() {
    return this.storeTermVectorOffsets;
  }
  
  /**
   * Set to <code>true</code> to also store token character offsets into the term
   * vector for this field.
   * @param value true if this field should store term vector offsets.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #storeTermVectorOffsets()
   */
  public void setStoreTermVectorOffsets(boolean value) {
    checkIfFrozen();
    this.storeTermVectorOffsets = value;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>false</code>.
   * @see #setStoreTermVectorPositions(boolean)
   */
  @Override
  public boolean storeTermVectorPositions() {
    return this.storeTermVectorPositions;
  }
  
  /**
   * Set to <code>true</code> to also store token positions into the term
   * vector for this field.
   * @param value true if this field should store term vector positions.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #storeTermVectorPositions()
   */
  public void setStoreTermVectorPositions(boolean value) {
    checkIfFrozen();
    this.storeTermVectorPositions = value;
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>false</code>.
   * @see #setStoreTermVectorPayloads(boolean) 
   */
  @Override
  public boolean storeTermVectorPayloads() {
    return this.storeTermVectorPayloads;
  }
  
  /**
   * Set to <code>true</code> to also store token payloads into the term
   * vector for this field.
   * @param value true if this field should store term vector payloads.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #storeTermVectorPayloads()
   */
  public void setStoreTermVectorPayloads(boolean value) {
    checkIfFrozen();
    this.storeTermVectorPayloads = value;
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>false</code>.
   * @see #setOmitNorms(boolean)
   */
  @Override
  public boolean omitNorms() {
    return this.omitNorms;
  }
  
  /**
   * Set to <code>true</code> to omit normalization values for the field.
   * @param value true if this field should omit norms.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #omitNorms()
   */
  public void setOmitNorms(boolean value) {
    checkIfFrozen();
    this.omitNorms = value;
  }

  /**
   * {@inheritDoc}
   * <p>
   * The default is {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS}.
   * @see #setIndexOptions(IndexOptions)
   */
  @Override
  public IndexOptions indexOptions() {
    return this.indexOptions;
  }
  
  /**
   * Sets the indexing options for the field:
   * @param value indexing options
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #indexOptions()
   */
  public void setIndexOptions(IndexOptions value) {
    checkIfFrozen();
    if (value == null) {
      throw new NullPointerException("IndexOptions must not be null");
    }
    this.indexOptions = value;
  }

  /**
   * Enables points indexing.
   */
  public void setDimensions(int dimensionCount, int dimensionNumBytes) {
    this.setDimensions(dimensionCount, dimensionCount, dimensionNumBytes);
  }

  /**
   * Enables points indexing with selectable dimension indexing.
   */
  public void setDimensions(int dimensionCount, int indexDimensionCount, int dimensionNumBytes) {
    if (dimensionCount < 0) {
      throw new IllegalArgumentException("dimensionCount must be >= 0; got " + dimensionCount);
    }
    if (dimensionCount > PointValues.MAX_DIMENSIONS) {
      throw new IllegalArgumentException("dimensionCount must be <= " + PointValues.MAX_DIMENSIONS + "; got " + dimensionCount);
    }
    if (indexDimensionCount < 0) {
      throw new IllegalArgumentException("indexDimensionCount must be >= 0; got " + indexDimensionCount);
    }
    if (indexDimensionCount > dimensionCount) {
      throw new IllegalArgumentException("indexDimensionCount must be <= dimensionCount: " + dimensionCount + "; got " + indexDimensionCount);
    }
    if (indexDimensionCount > PointValues.MAX_INDEX_DIMENSIONS) {
      throw new IllegalArgumentException("indexDimensionCount must be <= " + PointValues.MAX_INDEX_DIMENSIONS + "; got " + indexDimensionCount);
    }
    if (dimensionNumBytes < 0) {
      throw new IllegalArgumentException("dimensionNumBytes must be >= 0; got " + dimensionNumBytes);
    }
    if (dimensionNumBytes > PointValues.MAX_NUM_BYTES) {
      throw new IllegalArgumentException("dimensionNumBytes must be <= " + PointValues.MAX_NUM_BYTES + "; got " + dimensionNumBytes);
    }
    if (dimensionCount == 0) {
      if (indexDimensionCount != 0) {
        throw new IllegalArgumentException("when dimensionCount is 0, indexDimensionCount must be 0; got " + indexDimensionCount);
      }
      if (dimensionNumBytes != 0) {
        throw new IllegalArgumentException("when dimensionCount is 0, dimensionNumBytes must be 0; got " + dimensionNumBytes);
      }
    } else if (indexDimensionCount == 0) {
      throw new IllegalArgumentException("when dimensionCount is > 0, indexDimensionCount must be > 0; got " + indexDimensionCount);
    } else if (dimensionNumBytes == 0) {
      if (dimensionCount != 0) {
        throw new IllegalArgumentException("when dimensionNumBytes is 0, dimensionCount must be 0; got " + dimensionCount);
      }
    }

    this.dimensionCount = dimensionCount;
    this.indexDimensionCount = indexDimensionCount;
    this.dimensionNumBytes = dimensionNumBytes;
  }

  @Override
  public int pointDimensionCount() {
    return dimensionCount;
  }

  @Override
  public int pointIndexDimensionCount() {
    return indexDimensionCount;
  }

  @Override
  public int pointNumBytes() {
    return dimensionNumBytes;
  }

  /**
   * Puts an attribute value.
   * <p>
   * This is a key-value mapping for the field that the codec can use
   * to store additional metadata.
   * <p>
   * If a value already exists for the field, it will be replaced with
   * the new value. This method is not thread-safe, user must not add attributes
   * while other threads are indexing documents with this field type.
   *
   * @lucene.experimental
   */
  public String putAttribute(String key, String value) {
    checkIfFrozen();
    if (attributes == null) {
      attributes = new HashMap<>();
    }
    return attributes.put(key, value);
  }

  @Override
  public Map<String, String> getAttributes() {
    return attributes;
  }

  /** Prints a Field for human consumption. */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (stored()) {
      result.append("stored");
    }
    if (indexOptions != IndexOptions.NONE) {
      if (result.length() > 0)
        result.append(",");
      result.append("indexed");
      if (tokenized()) {
        result.append(",tokenized");
      }
      if (storeTermVectors()) {
        result.append(",termVector");
      }
      if (storeTermVectorOffsets()) {
        result.append(",termVectorOffsets");
      }
      if (storeTermVectorPositions()) {
        result.append(",termVectorPosition");
      }
      if (storeTermVectorPayloads()) {
        result.append(",termVectorPayloads");
      }
      if (omitNorms()) {
        result.append(",omitNorms");
      }
      if (indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        result.append(",indexOptions=");
        result.append(indexOptions);
      }
    }
    if (dimensionCount != 0) {
      if (result.length() > 0) {
        result.append(",");
      }
      result.append("pointDimensionCount=");
      result.append(dimensionCount);
      result.append(",pointIndexDimensionCount=");
      result.append(indexDimensionCount);
      result.append(",pointNumBytes=");
      result.append(dimensionNumBytes);
    }
    if (docValuesType != DocValuesType.NONE) {
      if (result.length() > 0) {
        result.append(",");
      }
      result.append("docValuesType=");
      result.append(docValuesType);
    }
    
    return result.toString();
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>null</code> (no docValues) 
   * @see #setDocValuesType(DocValuesType)
   */
  @Override
  public DocValuesType docValuesType() {
    return docValuesType;
  }

  /**
   * Sets the field's DocValuesType
   * @param type DocValues type, or null if no DocValues should be stored.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #docValuesType()
   */
  public void setDocValuesType(DocValuesType type) {
    checkIfFrozen();
    if (type == null) {
      throw new NullPointerException("DocValuesType must not be null");
    }
    docValuesType = type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + dimensionCount;
    result = prime * result + indexDimensionCount;
    result = prime * result + dimensionNumBytes;
    result = prime * result + ((docValuesType == null) ? 0 : docValuesType.hashCode());
    result = prime * result + indexOptions.hashCode();
    result = prime * result + (omitNorms ? 1231 : 1237);
    result = prime * result + (storeTermVectorOffsets ? 1231 : 1237);
    result = prime * result + (storeTermVectorPayloads ? 1231 : 1237);
    result = prime * result + (storeTermVectorPositions ? 1231 : 1237);
    result = prime * result + (storeTermVectors ? 1231 : 1237);
    result = prime * result + (stored ? 1231 : 1237);
    result = prime * result + (tokenized ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    FieldType other = (FieldType) obj;
    if (dimensionCount != other.dimensionCount) return false;
    if (indexDimensionCount != other.indexDimensionCount) return false;
    if (dimensionNumBytes != other.dimensionNumBytes) return false;
    if (docValuesType != other.docValuesType) return false;
    if (indexOptions != other.indexOptions) return false;
    if (omitNorms != other.omitNorms) return false;
    if (storeTermVectorOffsets != other.storeTermVectorOffsets) return false;
    if (storeTermVectorPayloads != other.storeTermVectorPayloads) return false;
    if (storeTermVectorPositions != other.storeTermVectorPositions) return false;
    if (storeTermVectors != other.storeTermVectors) return false;
    if (stored != other.stored) return false;
    if (tokenized != other.tokenized) return false;
    return true;
  }
}
