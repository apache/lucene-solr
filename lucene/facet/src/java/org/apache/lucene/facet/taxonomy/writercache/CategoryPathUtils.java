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
package org.apache.lucene.facet.taxonomy.writercache;

import org.apache.lucene.facet.taxonomy.FacetLabel;

/** Utilities for use of {@link FacetLabel} by {@link CompactLabelToOrdinal}. */
class CategoryPathUtils {
  
  /** Serializes the given {@link FacetLabel} to the {@link CharBlockArray}. */
  public static void serialize(FacetLabel cp, CharBlockArray charBlockArray) {
    charBlockArray.append((char) cp.length);
    if (cp.length == 0) {
      return;
    }
    for (int i = 0; i < cp.length; i++) {
      charBlockArray.append((char) cp.components[i].length());
      charBlockArray.append(cp.components[i]);
    }
  }

  /**
   * Calculates a hash function of a path that was serialized with
   * {@link #serialize(FacetLabel, CharBlockArray)}.
   */
  public static int hashCodeOfSerialized(CharBlockArray charBlockArray, int offset) {
    int length = charBlockArray.charAt(offset++);
    if (length == 0) {
      return 0;
    }
    
    int hash = length;
    for (int i = 0; i < length; i++) {
      int len = charBlockArray.charAt(offset++);
      hash = hash * 31 + charBlockArray.subSequence(offset, offset + len).hashCode();
      offset += len;
    }
    return hash;
  }

  /**
   * Check whether the {@link FacetLabel} is equal to the one serialized in
   * {@link CharBlockArray}.
   */
  public static boolean equalsToSerialized(FacetLabel cp, CharBlockArray charBlockArray, int offset) {
    int n = charBlockArray.charAt(offset++);
    if (cp.length != n) {
      return false;
    }
    if (cp.length == 0) {
      return true;
    }
    
    for (int i = 0; i < cp.length; i++) {
      int len = charBlockArray.charAt(offset++);
      if (len != cp.components[i].length()) {
        return false;
      }
      if (!cp.components[i].equals(charBlockArray.subSequence(offset, offset + len))) {
        return false;
      }
      offset += len;
    }
    return true;
  }

}
