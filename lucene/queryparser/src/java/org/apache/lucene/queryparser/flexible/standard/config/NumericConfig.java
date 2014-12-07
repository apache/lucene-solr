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

import org.apache.lucene.document.FieldTypes;

/**
 * This class holds the configuration used to parse numeric queries and create
 * {@link NumericRangeQuery}s.
 * 
 * @see NumericRangeQuery
 * @see NumberFormat
 */
public class NumericConfig {
  
  private NumberFormat format;
  
  private final FieldTypes fieldTypes;
  
  /**
   * Constructs a {@link NumericConfig} object.
   * 
   * @param format
   *          the {@link NumberFormat} used to parse a {@link String} to
   *          {@link Number}
   * @param type
   *          the numeric type used to index the numeric values
   * 
   * @see NumericConfig#setNumberFormat(NumberFormat)
   */
  public NumericConfig(NumberFormat format, FieldTypes fieldTypes) {
    setNumberFormat(format);
    this.fieldTypes = fieldTypes;
  }

  public FieldTypes getFieldTypes() {
    return fieldTypes;
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
      
      if (this.format == other.format || (this.format.equals(other.format))) {
        return true;
      }
      
    }
    
    return false;
    
  }
  
}
