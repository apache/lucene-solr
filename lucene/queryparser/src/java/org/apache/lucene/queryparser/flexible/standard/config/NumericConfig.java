package org.apache.lucene.queryparser.flexible.standard.config;

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

import java.text.NumberFormat;
import java.util.Objects;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.LegacyNumericType;

/**
 * This class holds the configuration used to parse numeric queries and create
 * {@link org.apache.lucene.search.LegacyNumericRangeQuery}s.
 * 
 * @see org.apache.lucene.search.LegacyNumericRangeQuery
 * @see NumberFormat
 */
public class NumericConfig {
  
  private int precisionStep;
  
  private NumberFormat format;
  
  private FieldType.LegacyNumericType type;
  
  /**
   * Constructs a {@link NumericConfig} object.
   * 
   * @param precisionStep
   *          the precision used to index the numeric values
   * @param format
   *          the {@link NumberFormat} used to parse a {@link String} to
   *          {@link Number}
   * @param type
   *          the numeric type used to index the numeric values
   * 
   * @see NumericConfig#setPrecisionStep(int)
   * @see NumericConfig#setNumberFormat(NumberFormat)
   * @see #setType(org.apache.lucene.document.FieldType.LegacyNumericType)
   */
  public NumericConfig(int precisionStep, NumberFormat format,
      LegacyNumericType type) {
    setPrecisionStep(precisionStep);
    setNumberFormat(format);
    setType(type);
    
  }
  
  /**
   * Returns the precision used to index the numeric values
   * 
   * @return the precision used to index the numeric values
   * 
   * @see org.apache.lucene.search.LegacyNumericRangeQuery#getPrecisionStep()
   */
  public int getPrecisionStep() {
    return precisionStep;
  }
  
  /**
   * Sets the precision used to index the numeric values
   * 
   * @param precisionStep
   *          the precision used to index the numeric values
   * 
   * @see org.apache.lucene.search.LegacyNumericRangeQuery#getPrecisionStep()
   */
  public void setPrecisionStep(int precisionStep) {
    this.precisionStep = precisionStep;
  }
  
  /**
   * Returns the {@link NumberFormat} used to parse a {@link String} to
   * {@link Number}
   * 
   * @return the {@link NumberFormat} used to parse a {@link String} to
   *         {@link Number}
   */
  public NumberFormat getNumberFormat() {
    return format;
  }
  
  /**
   * Returns the numeric type used to index the numeric values
   * 
   * @return the numeric type used to index the numeric values
   */
  public LegacyNumericType getType() {
    return type;
  }
  
  /**
   * Sets the numeric type used to index the numeric values
   * 
   * @param type the numeric type used to index the numeric values
   */
  public void setType(LegacyNumericType type) {
    
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null!");
    }
    
    this.type = type;
    
  }
  
  /**
   * Sets the {@link NumberFormat} used to parse a {@link String} to
   * {@link Number}
   * 
   * @param format
   *          the {@link NumberFormat} used to parse a {@link String} to
   *          {@link Number}, cannot be <code>null</code>
   */
  public void setNumberFormat(NumberFormat format) {
    
    if (format == null) {
      throw new IllegalArgumentException("format cannot be null!");
    }
    
    this.format = format;
    
  }
  
  @Override
  public boolean equals(Object obj) {
    
    if (obj == this) return true;
    
    if (obj instanceof NumericConfig) {
      NumericConfig other = (NumericConfig) obj;
      
      if (this.precisionStep == other.precisionStep
          && this.type == other.type
          && (this.format == other.format || (this.format.equals(other.format)))) {
        return true;
      }
      
    }
    
    return false;
    
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(precisionStep, type, format);
  }
  
}
