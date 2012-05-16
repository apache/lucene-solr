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

import java.util.Iterator;

import org.apache.lucene.index.FieldInfo.IndexOptions;

/** 
 * Collection of {@link FieldInfo}s (accessible by number or by name).
 *  @lucene.experimental
 */
public abstract class FieldInfos implements Cloneable,Iterable<FieldInfo> {
 
  /**
   * Returns a deep clone of this FieldInfos instance.
   */
  @Override
  public abstract FieldInfos clone();

  /**
   * Return the fieldinfo object referenced by the field name
   * @return the FieldInfo object or null when the given fieldName
   * doesn't exist.
   */  
  public abstract FieldInfo fieldInfo(String fieldName);

  /**
   * Return the fieldinfo object referenced by the fieldNumber.
   * @param fieldNumber
   * @return the FieldInfo object or null when the given fieldNumber
   * doesn't exist.
   */  
  public abstract FieldInfo fieldInfo(int fieldNumber);

  /**
   * Returns an iterator over all the fieldinfo objects present,
   * ordered by ascending field number
   */
  // TODO: what happens if in fact a different order is used?
  public abstract Iterator<FieldInfo> iterator();

  /**
   * @return number of fields
   */
  public abstract int size();

  /** Returns true if any fields have positions */
  public boolean hasProx() {
    for (FieldInfo fi : this) {
      if (fi.isIndexed() && fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
        return true;
      }
    }
    return false;
  }
  
  /** Returns true if any fields have freqs */
  public boolean hasFreq() {
    for (FieldInfo fi : this) {
      if (fi.isIndexed() && fi.getIndexOptions() != IndexOptions.DOCS_ONLY) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * @return true if at least one field has any vectors
   */
  public boolean hasVectors() {
    for (FieldInfo fi : this) {
      if (fi.hasVectors()) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * @return true if at least one field has any norms
   */
  public boolean hasNorms() {
    for (FieldInfo fi : this) {
      if (fi.hasNorms()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if at least one field has docValues
   */
  public boolean hasDocValues() {
    for (FieldInfo fi : this) {
      if (fi.hasDocValues()) { 
        return true;
      }
    }
    return false;
  }
}
