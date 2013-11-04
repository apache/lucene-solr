package org.apache.lucene.index.sorter;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;

/**
 * Helper class to sort readers that contain blocks of documents.
 */
public abstract class BlockJoinSorter extends Sorter {

  protected final Filter parentsFilter;

  /** Sole constructor. */
  public BlockJoinSorter(Filter parentsFilter) {
    this.parentsFilter = parentsFilter;
  }

  /** Return a {@link Sorter.DocComparator} instance that will be called on
   *  parent doc IDs. */
  protected abstract DocComparator getParentComparator(AtomicReader reader);

  /** Return a {@link Sorter.DocComparator} instance that will be called on
   *  children of the same parent. By default, children of the same parent are
   *  not reordered. */
  protected DocComparator getChildComparator(AtomicReader reader) {
    return INDEX_ORDER_COMPARATOR;
  }

  @Override
  public final DocMap sort(AtomicReader reader) throws IOException {
    final DocIdSet parents = parentsFilter.getDocIdSet(reader.getContext(), null);
    if (parents == null) {
      throw new IllegalStateException("AtomicReader " + reader + " contains no parents!");
    }
    if (!(parents instanceof FixedBitSet)) {
      throw new IllegalStateException("parentFilter must return FixedBitSet; got " + parents);
    }
    final FixedBitSet parentBits = (FixedBitSet) parents;
    final DocComparator parentComparator = getParentComparator(reader);
    final DocComparator childComparator = getChildComparator(reader);
    final DocComparator comparator = new DocComparator() {

      @Override
      public int compare(int docID1, int docID2) {
        final int parent1 = parentBits.nextSetBit(docID1);
        final int parent2 = parentBits.nextSetBit(docID2);
        if (parent1 == parent2) { // both are in the same block
          if (docID1 == parent1 || docID2 == parent2) {
            // keep parents at the end of blocks
            return docID1 - docID2;
          } else {
            return childComparator.compare(docID1, docID2);
          }
        } else {
          int cmp = parentComparator.compare(parent1, parent2);
          if (cmp == 0) {
            cmp = parent1 - parent2;
          }
          return cmp;
        }
      }

    };
    return sort(reader.maxDoc(), comparator);
  }

}
