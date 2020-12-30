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
package org.apache.lucene.facet.taxonomy;

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.util.Arrays;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache; // javadocs
import org.apache.lucene.facet.taxonomy.writercache.NameHashIntCacheLRU; // javadocs

/**
 * Holds a sequence of string components, specifying the hierarchical name of a category.
 *
 * @lucene.internal
 */
public class FacetLabel implements Comparable<FacetLabel> {

  /*
   * copied from DocumentWriterPerThread -- if a FacetLabel is resolved to a
   * drill-down term which is encoded to a larger term than that length, it is
   * silently dropped! Therefore we limit the number of characters to MAX/4 to
   * be on the safe side.
   */
  /** The maximum number of characters a {@link FacetLabel} can have. */
  public static final int MAX_CATEGORY_PATH_LENGTH = (BYTE_BLOCK_SIZE - 2) / 4;

  /**
   * The components of this {@link FacetLabel}. Note that this array may be shared with other {@link
   * FacetLabel} instances, e.g. as a result of {@link #subpath(int)}, therefore you should traverse
   * the array up to {@link #length} for this path's components.
   */
  public final String[] components;

  /** The number of components of this {@link FacetLabel}. */
  public final int length;

  // Used by subpath
  private FacetLabel(final FacetLabel copyFrom, final int prefixLen) {
    // while the code which calls this method is safe, at some point a test
    // tripped on AIOOBE in toString, but we failed to reproduce. adding the
    // assert as a safety check.
    assert prefixLen >= 0 && prefixLen <= copyFrom.components.length
        : "prefixLen cannot be negative nor larger than the given components' length: prefixLen="
            + prefixLen
            + " components.length="
            + copyFrom.components.length;
    this.components = copyFrom.components;
    length = prefixLen;
  }

  /** Construct from the given path components. */
  public FacetLabel(final String... components) {
    this.components = components;
    length = components.length;
    checkComponents();
  }

  /** Construct from the dimension plus the given path components. */
  public FacetLabel(String dim, String[] path) {
    components = new String[1 + path.length];
    components[0] = dim;
    System.arraycopy(path, 0, components, 1, path.length);
    length = components.length;
    checkComponents();
  }

  private void checkComponents() {
    long len = 0;
    for (String comp : components) {
      if (comp == null || comp.isEmpty()) {
        throw new IllegalArgumentException(
            "empty or null components not allowed: " + Arrays.toString(components));
      }
      len += comp.length();
    }
    len += components.length - 1; // add separators
    if (len > MAX_CATEGORY_PATH_LENGTH) {
      throw new IllegalArgumentException(
          "category path exceeds maximum allowed path length: max="
              + MAX_CATEGORY_PATH_LENGTH
              + " len="
              + len
              + " path="
              + Arrays.toString(components).substring(0, 30)
              + "...");
    }
  }

  /** Compares this path with another {@link FacetLabel} for lexicographic order. */
  @Override
  public int compareTo(FacetLabel other) {
    final int len = length < other.length ? length : other.length;
    for (int i = 0, j = 0; i < len; i++, j++) {
      int cmp = components[i].compareTo(other.components[j]);
      if (cmp < 0) {
        return -1; // this is 'before'
      }
      if (cmp > 0) {
        return 1; // this is 'after'
      }
    }

    // one is a prefix of the other
    return length - other.length;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FacetLabel)) {
      return false;
    }

    FacetLabel other = (FacetLabel) obj;
    if (length != other.length) {
      return false; // not same length, cannot be equal
    }

    // CategoryPaths are more likely to differ at the last components, so start
    // from last-first
    for (int i = length - 1; i >= 0; i--) {
      if (!components[i].equals(other.components[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    if (length == 0) {
      return 0;
    }

    int hash = length;
    for (int i = 0; i < length; i++) {
      hash = hash * 31 + components[i].hashCode();
    }
    return hash;
  }

  /**
   * Calculate a 64-bit hash function for this path. This is necessary for {@link
   * NameHashIntCacheLRU} (the default cache impl for {@link LruTaxonomyWriterCache}) to reduce the
   * chance of "silent but deadly" collisions.
   */
  public long longHashCode() {
    if (length == 0) {
      return 0;
    }

    long hash = length;
    for (int i = 0; i < length; i++) {
      hash = hash * 65599 + components[i].hashCode();
    }
    return hash;
  }

  /** Returns a sub-path of this path up to {@code length} components. */
  public FacetLabel subpath(final int length) {
    if (length >= this.length || length < 0) {
      return this;
    } else {
      return new FacetLabel(this, length);
    }
  }

  /** Returns a string representation of the path. */
  @Override
  public String toString() {
    if (length == 0) {
      return "FacetLabel: []";
    }
    String[] parts = new String[length];
    System.arraycopy(components, 0, parts, 0, length);
    return "FacetLabel: " + Arrays.toString(parts);
  }
}
