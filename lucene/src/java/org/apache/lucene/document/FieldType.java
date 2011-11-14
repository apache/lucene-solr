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

import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;

public class FieldType implements IndexableFieldType {

  private boolean indexed;
  private boolean stored;
  private boolean tokenized;
  private boolean storeTermVectors;
  private boolean storeTermVectorOffsets;
  private boolean storeTermVectorPositions;
  private boolean omitNorms;
  private IndexOptions indexOptions = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
  private boolean frozen;

  public FieldType(IndexableFieldType ref) {
    this.indexed = ref.indexed();
    this.stored = ref.stored();
    this.tokenized = ref.tokenized();
    this.storeTermVectors = ref.storeTermVectors();
    this.storeTermVectorOffsets = ref.storeTermVectorOffsets();
    this.storeTermVectorPositions = ref.storeTermVectorPositions();
    this.omitNorms = ref.omitNorms();
    this.indexOptions = ref.indexOptions();
    // Do not copy frozen!
  }
  
  public FieldType() {
  }

  private void checkIfFrozen() {
    if (frozen) {
      throw new IllegalStateException();
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
  
  public boolean indexed() {
    return this.indexed;
  }
  
  public void setIndexed(boolean value) {
    checkIfFrozen();
    this.indexed = value;
  }

  public boolean stored() {
    return this.stored;
  }
  
  public void setStored(boolean value) {
    checkIfFrozen();
    this.stored = value;
  }

  public boolean tokenized() {
    return this.tokenized;
  }
  
  public void setTokenized(boolean value) {
    checkIfFrozen();
    this.tokenized = value;
  }

  public boolean storeTermVectors() {
    return this.storeTermVectors;
  }
  
  public void setStoreTermVectors(boolean value) {
    checkIfFrozen();
    this.storeTermVectors = value;
  }

  public boolean storeTermVectorOffsets() {
    return this.storeTermVectorOffsets;
  }
  
  public void setStoreTermVectorOffsets(boolean value) {
    checkIfFrozen();
    this.storeTermVectorOffsets = value;
  }

  public boolean storeTermVectorPositions() {
    return this.storeTermVectorPositions;
  }
  
  public void setStoreTermVectorPositions(boolean value) {
    checkIfFrozen();
    this.storeTermVectorPositions = value;
  }
  
  public boolean omitNorms() {
    return this.omitNorms;
  }
  
  public void setOmitNorms(boolean value) {
    checkIfFrozen();
    this.omitNorms = value;
  }

  public IndexOptions indexOptions() {
    return this.indexOptions;
  }
  
  public void setIndexOptions(IndexOptions value) {
    checkIfFrozen();
    this.indexOptions = value;
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
        if (result.length() > 0)
          result.append(",");
        result.append("tokenized");
      }
      if (storeTermVectors()) {
        if (result.length() > 0)
          result.append(",");
        result.append("termVector");
      }
      if (storeTermVectorOffsets()) {
        if (result.length() > 0)
          result.append(",");
        result.append("termVectorOffsets");
      }
      if (storeTermVectorPositions()) {
        if (result.length() > 0)
          result.append(",");
        result.append("termVectorPosition");
      }
      if (omitNorms()) {
        result.append(",omitNorms");
      }
      if (indexOptions != IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) {
        result.append(",indexOptions=");
        result.append(indexOptions);
      }
    }
    
    return result.toString();
  }
}
