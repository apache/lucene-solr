package org.apache.lucene.document;

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

import org.apache.lucene.analysis.Analyzer; // javadocs
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.NumericRangeQuery; // javadocs
import org.apache.lucene.util.NumericUtils;

/**
 * Describes the properties of a field.
 */
public class FieldType implements IndexableFieldType {

  /** Data type of the numeric value
   * @since 3.2
   */
  public static enum NumericType {
    /** 32-bit integer numeric type */
    INT, 
    /** 64-bit long numeric type */
    LONG, 
    /** 32-bit float numeric type */
    FLOAT, 
    /** 64-bit double numeric type */
    DOUBLE
  }

  private boolean indexed;
  private boolean stored;
  private boolean tokenized = true;
  private boolean storeTermVectors;
  private boolean storeTermVectorOffsets;
  private boolean storeTermVectorPositions;
  private boolean storeTermVectorPayloads;
  private boolean omitNorms;
  private IndexOptions indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
  private NumericType numericType;
  private boolean frozen;
  private int numericPrecisionStep = NumericUtils.PRECISION_STEP_DEFAULT;
  private DocValuesType docValueType;

  /**
   * Create a new mutable FieldType with all of the properties from <code>ref</code>
   */
  public FieldType(FieldType ref) {
    this.indexed = ref.indexed();
    this.stored = ref.stored();
    this.tokenized = ref.tokenized();
    this.storeTermVectors = ref.storeTermVectors();
    this.storeTermVectorOffsets = ref.storeTermVectorOffsets();
    this.storeTermVectorPositions = ref.storeTermVectorPositions();
    this.storeTermVectorPayloads = ref.storeTermVectorPayloads();
    this.omitNorms = ref.omitNorms();
    this.indexOptions = ref.indexOptions();
    this.docValueType = ref.docValueType();
    this.numericType = ref.numericType();
    // Do not copy frozen!
  }
  
  /**
   * Create a new FieldType with default properties.
   */
  public FieldType() {
  }

  private void checkIfFrozen() {
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
   * @see #setIndexed(boolean)
   */
  @Override
  public boolean indexed() {
    return this.indexed;
  }
  
  /**
   * Set to <code>true</code> to index (invert) this field.
   * @param value true if this field should be indexed.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #indexed()
   */
  public void setIndexed(boolean value) {
    checkIfFrozen();
    this.indexed = value;
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
  @Override
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
   * @see #setIndexOptions(org.apache.lucene.index.FieldInfo.IndexOptions)
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
    this.indexOptions = value;
  }

  /**
   * Specifies the field's numeric type.
   * @param type numeric type, or null if the field has no numeric type.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #numericType()
   */
  public void setNumericType(NumericType type) {
    checkIfFrozen();
    numericType = type;
  }

  /** 
   * NumericType: if non-null then the field's value will be indexed
   * numerically so that {@link NumericRangeQuery} can be used at 
   * search time. 
   * <p>
   * The default is <code>null</code> (no numeric type) 
   * @see #setNumericType(NumericType)
   */
  public NumericType numericType() {
    return numericType;
  }

  /**
   * Sets the numeric precision step for the field.
   * @param precisionStep numeric precision step for the field
   * @throws IllegalArgumentException if precisionStep is less than 1. 
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #numericPrecisionStep()
   */
  public void setNumericPrecisionStep(int precisionStep) {
    checkIfFrozen();
    if (precisionStep < 1) {
      throw new IllegalArgumentException("precisionStep must be >= 1 (got " + precisionStep + ")");
    }
    this.numericPrecisionStep = precisionStep;
  }

  /** 
   * Precision step for numeric field. 
   * <p>
   * This has no effect if {@link #numericType()} returns null.
   * <p>
   * The default is {@link NumericUtils#PRECISION_STEP_DEFAULT}
   * @see #setNumericPrecisionStep(int)
   */
  public int numericPrecisionStep() {
    return numericPrecisionStep;
  }

  /** Prints a Field for human consumption. */
  @Override
  public final String toString() {
    StringBuilder result = new StringBuilder();
    if (stored()) {
      result.append("stored");
    }
    if (indexed()) {
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
        if (storeTermVectorPayloads()) {
          result.append(",termVectorPayloads");
        }
      }
      if (omitNorms()) {
        result.append(",omitNorms");
      }
      if (indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        result.append(",indexOptions=");
        result.append(indexOptions);
      }
      if (numericType != null) {
        result.append(",numericType=");
        result.append(numericType);
        result.append(",numericPrecisionStep=");
        result.append(numericPrecisionStep);
      }
    }
    if (docValueType != null) {
      if (result.length() > 0)
        result.append(",");
      result.append("docValueType=");
      result.append(docValueType);
    }
    
    return result.toString();
  }
  
  /**
   * {@inheritDoc}
   * <p>
   * The default is <code>null</code> (no docValues) 
   * @see #setDocValueType(org.apache.lucene.index.FieldInfo.DocValuesType)
   */
  @Override
  public DocValuesType docValueType() {
    return docValueType;
  }

  /**
   * Set's the field's DocValuesType
   * @param type DocValues type, or null if no DocValues should be stored.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #docValueType()
   */
  public void setDocValueType(DocValuesType type) {
    checkIfFrozen();
    docValueType = type;
  }
}
