package org.apache.lucene.spatial.util;

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

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * A builder of {@link DocIdSet}s that supports random access.
 * @lucene.internal
 */
public final class BitDocIdSetBuilder {

  private final int maxDoc;
  private final int threshold;
  private SparseFixedBitSet sparseSet;
  private FixedBitSet denseSet;

  // we cache an upper bound of the cost of this builder so that we don't have
  // to re-compute approximateCardinality on the sparse set every time
  private long costUpperBound;

  /** Create a new empty instance. */
  public BitDocIdSetBuilder(int maxDoc) {
    this.maxDoc = maxDoc;
    threshold = maxDoc >>> 10;
  }

  // pkg-private for testing
  boolean dense() {
    return denseSet != null;
  }

  /**
   * Is this builder definitely empty?  If so, {@link #build()} will return null.  This is usually the same as
   * simply being empty but if this builder was constructed with the {@code full} option or if an iterator was passed
   * that iterated over no documents, then we're not sure.
   */
  public boolean isDefinitelyEmpty() {
    return sparseSet == null && denseSet == null;
  }

  /**
   * Add the content of the provided {@link DocIdSetIterator} to this builder.
   */
  public void or(DocIdSetIterator it) throws IOException {
    if (denseSet != null) {
      // already upgraded
      denseSet.or(it);
      return;
    }

    final long itCost = it.cost();
    costUpperBound += itCost;
    if (costUpperBound >= threshold) {
      costUpperBound = (sparseSet == null ? 0 : sparseSet.approximateCardinality()) + itCost;

      if (costUpperBound >= threshold) {
        // upgrade
        denseSet = new FixedBitSet(maxDoc);
        denseSet.or(it);
        if (sparseSet != null) {
          denseSet.or(new BitSetIterator(sparseSet, 0L));
        }
        return;
      }
    }

    // we are still sparse
    if (sparseSet == null) {
      sparseSet = new SparseFixedBitSet(maxDoc);
    }
    sparseSet.or(it);
  }

  /**
   * Build a {@link DocIdSet} that contains all doc ids that have been added.
   * This method may return <tt>null</tt> if no documents were addded to this
   * builder.
   * NOTE: this is a destructive operation, the builder should not be used
   * anymore after this method has been called.
   */
  public BitDocIdSet build() {
    final BitDocIdSet result;
    if (denseSet != null) {
      result = new BitDocIdSet(denseSet);
    } else if (sparseSet != null) {
      result = new BitDocIdSet(sparseSet);
    } else {
      result = null;
    }
    denseSet = null;
    sparseSet = null;
    costUpperBound = 0;
    return result;
  }

}
