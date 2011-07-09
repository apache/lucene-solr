package org.apache.lucene.queryParser.standard.config;

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

import java.text.NumberFormat;

import org.apache.lucene.document.NumericField;

public class NumericConfig {
  
  private int precisionStep;
  
  private NumberFormat format;
  
  private NumericField.DataType type;
 
  public NumericConfig(int precisionStep, NumberFormat format, NumericField.DataType type) {
    setPrecisionStep(precisionStep);
    setNumberFormat(format);
    setType(type);
    
  }
  
  public int getPrecisionStep() {
    return precisionStep;
  }
  
  public void setPrecisionStep(int precisionStep) {
    this.precisionStep = precisionStep;
  }
  
  public NumberFormat getNumberFormat() {
    return format;
  }
  
  public NumericField.DataType getType() {
    return type;
  }

  public void setType(NumericField.DataType type) {
    
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null!");
    }
    
    this.type = type;
    
  }

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
          && this.format == other.format) {
        return true;
      }
      
    }
    
    return false;
    
  }
  
}
