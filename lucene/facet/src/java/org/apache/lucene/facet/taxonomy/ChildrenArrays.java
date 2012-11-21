package org.apache.lucene.facet.taxonomy;

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

/**
 * Equivalent representations of the taxonomy's parent info, 
 * used internally for efficient computation of facet results: 
 * "youngest child" and "oldest sibling"   
 */
public class ChildrenArrays {
  
  private final int[] youngestChild, olderSibling;
  
  public ChildrenArrays(int[] parents) {
    this(parents, null);
  }
  
  public ChildrenArrays(int[] parents, ChildrenArrays copyFrom) {
    youngestChild = new int[parents.length];
    olderSibling = new int[parents.length];
    int first = 0;
    if (copyFrom != null) {
      System.arraycopy(copyFrom.getYoungestChildArray(), 0, youngestChild, 0, copyFrom.getYoungestChildArray().length);
      System.arraycopy(copyFrom.getOlderSiblingArray(), 0, olderSibling, 0, copyFrom.getOlderSiblingArray().length);
      first = copyFrom.getOlderSiblingArray().length;
    }
    computeArrays(parents, first);
  }
  
  private void computeArrays(int[] parents, int first) {
    // reset the youngest child of all ordinals. while this should be done only
    // for the leaves, we don't know up front which are the leaves, so we reset
    // all of them.
    for (int i = first; i < parents.length; i++) {
      youngestChild[i] = TaxonomyReader.INVALID_ORDINAL;
    }
    
    // the root category has no parent, and therefore no siblings
    if (first == 0) {
      first = 1;
      olderSibling[0] = TaxonomyReader.INVALID_ORDINAL;
    }
    
    for (int i = first; i < parents.length; i++) {
      // note that parents[i] is always < i, so the right-hand-side of
      // the following line is already set when we get here
      olderSibling[i] = youngestChild[parents[i]];
      youngestChild[parents[i]] = i;
    }
  }
  
  /**
   * Returns an {@code int[]} the size of the taxonomy listing for each category
   * the ordinal of its immediate older sibling (the sibling in the taxonomy
   * tree with the highest ordinal below that of the given ordinal). The value
   * for a category with no older sibling is {@link TaxonomyReader#INVALID_ORDINAL}.
   */
  public int[] getOlderSiblingArray() {
    return olderSibling;
  }
  
  /**
   * Returns an {@code int[]} the size of the taxonomy listing the ordinal of
   * the youngest (highest numbered) child category of each category in the
   * taxonomy. The value for a leaf category (a category without children) is
   * {@link TaxonomyReader#INVALID_ORDINAL}.
   */
  public int[] getYoungestChildArray() {
    return youngestChild;
  }

}
