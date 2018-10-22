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
package org.apache.solr.legacy;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

/**
 * FieldType extension with support for legacy numerics
 * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
 */
@Deprecated
public final class LegacyFieldType extends FieldType {
  private LegacyNumericType numericType;
  private int numericPrecisionStep = LegacyNumericUtils.PRECISION_STEP_DEFAULT;

  /**
   * Create a new mutable LegacyFieldType with all of the properties from <code>ref</code>
   */
  public LegacyFieldType(LegacyFieldType ref) {
    super(ref);
    this.numericType = ref.numericType;
    this.numericPrecisionStep = ref.numericPrecisionStep;
  }
  
  /**
   * Create a new FieldType with default properties.
   */
  public LegacyFieldType() {
  }
  
  /**
   * Specifies the field's numeric type.
   * @param type numeric type, or null if the field has no numeric type.
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #numericType()
   *
   * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
   */
  @Deprecated
  public void setNumericType(LegacyNumericType type) {
    checkIfFrozen();
    numericType = type;
  }
  
  /** 
   * LegacyNumericType: if non-null then the field's value will be indexed
   * numerically so that {@link org.apache.solr.legacy.LegacyNumericRangeQuery} can be used at
   * search time. 
   * <p>
   * The default is <code>null</code> (no numeric type) 
   * @see #setNumericType(LegacyNumericType)
   *
   * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
   */
  @Deprecated
  public LegacyNumericType numericType() {
    return numericType;
  }
  
  /**
   * Sets the numeric precision step for the field.
   * @param precisionStep numeric precision step for the field
   * @throws IllegalArgumentException if precisionStep is less than 1. 
   * @throws IllegalStateException if this FieldType is frozen against
   *         future modifications.
   * @see #numericPrecisionStep()
   *
   * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
   */
  @Deprecated
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
   * The default is {@link org.apache.solr.legacy.LegacyNumericUtils#PRECISION_STEP_DEFAULT}
   * @see #setNumericPrecisionStep(int)
   *
   * @deprecated Please switch to {@link org.apache.lucene.index.PointValues} instead
   */
  @Deprecated
  public int numericPrecisionStep() {
    return numericPrecisionStep;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + numericPrecisionStep;
    result = prime * result + ((numericType == null) ? 0 : numericType.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) return false;
    LegacyFieldType other = (LegacyFieldType) obj;
    if (numericPrecisionStep != other.numericPrecisionStep) return false;
    if (numericType != other.numericType) return false;
    return true;
  }

  /** Prints a Field for human consumption. */
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(super.toString());
    if (indexOptions() != IndexOptions.NONE) {
      if (result.length() > 0) {
        result.append(",");
      }
      if (numericType != null) {
        result.append(",numericType=");
        result.append(numericType);
        result.append(",numericPrecisionStep=");
        result.append(numericPrecisionStep);
      }
    }
    return result.toString();
  }
}
