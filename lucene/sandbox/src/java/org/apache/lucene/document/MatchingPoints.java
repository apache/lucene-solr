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
package org.apache.lucene.document;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Accumulates matching hits for points.
 * <p>
 * Add matches with ({@link #add(int)}) and call {@link #iterator()} for
 * an iterator over the results. 
 * <p>
 * <b>NOTE:</b> it is required that you implement the optional {@code grow()}
 * method in your IntersectVisitor, this is used for cost computation.
 * <p>
 * This implementation currently optimizes bitset structure (sparse vs dense)
 * and {@link DocIdSetIterator#cost()} (cardinality) based on index statistics.
 * This API may change as point values evolves.
 * 
 * @lucene.experimental
 */
final class MatchingPoints {
  /** bitset we collect into */
  private final BitSet bits;
  /** number of documents containing a value for the points field */
  private final int docCount;
  /** number of values indexed for the points field */
  private final long numPoints;
  /** number of documents in the index segment */
  private final int maxDoc;
  /** counter of hits seen */
  private long counter;

  /**
   * Creates a new accumulator.
   * @param reader reader to collect point matches from
   * @param field field name.
   */
  public MatchingPoints(LeafReader reader, String field) {
    maxDoc = reader.maxDoc();
    PointValues values = reader.getPointValues();
    if (values == null) {
      throw new IllegalStateException("the query is missing null checks");
    }
    docCount = values.getDocCount(field);
    numPoints = values.size(field);
    // heuristic: if the field is really sparse, use a sparse impl
    if (docCount >= 0 && docCount * 100L < maxDoc) {
      bits = new SparseFixedBitSet(maxDoc);
    } else {
      bits = new FixedBitSet(maxDoc);
    }
  }

  /**
   * Record a matching docid.
   * <p>
   * NOTE: doc IDs do not need to be provided in any order.
   */
  public void add(int doc) {
    bits.set(doc);
  }

  /**
   * Grows cardinality counter by the given amount.
   */
  public void grow(int amount) {
    counter += amount;
  }
  
  /**
   * Returns an iterator over the recorded matches.
   */
  public DocIdSetIterator iterator() {
    // ensure caller implements the grow() api
    assert counter > 0 || bits.cardinality() == 0 : "the IntersectVisitor is missing grow()";

    // if single-valued (docCount == numPoints), then we know 1 point == 1 doc
    // otherwise we approximate based on field stats
    return new BitSetIterator(bits, (long) (counter * (docCount / (double) numPoints)));
  }
}
