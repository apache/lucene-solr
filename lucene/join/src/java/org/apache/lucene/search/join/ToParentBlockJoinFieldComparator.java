package org.apache.lucene.search.join;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SimpleFieldComparator;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;

/**
 * A field comparator that allows parent documents to be sorted by fields
 * from the nested / child documents.
 *
 * @lucene.experimental
 */
public abstract class ToParentBlockJoinFieldComparator extends SimpleFieldComparator<Object> implements LeafFieldComparator { // repeat LeafFieldComparator for javadocs

  private final BitDocIdSetFilter parentFilter;
  private final BitDocIdSetFilter childFilter;
  final int spareSlot;

  FieldComparator<Object> wrappedComparator;
  LeafFieldComparator wrappedLeafComparator;
  BitSet parentDocuments;
  BitSet childDocuments;

  ToParentBlockJoinFieldComparator(FieldComparator<Object> wrappedComparator, BitDocIdSetFilter parentFilter, BitDocIdSetFilter childFilter, int spareSlot) {
    this.wrappedComparator = wrappedComparator;
    this.parentFilter = parentFilter;
    this.childFilter = childFilter;
    this.spareSlot = spareSlot;
  }

  @Override
  public int compare(int slot1, int slot2) {
    return wrappedComparator.compare(slot1, slot2);
  }

  @Override
  public void setBottom(int slot) {
    wrappedLeafComparator.setBottom(slot);
  }

  @Override
  public void setTopValue(Object value) {
    wrappedComparator.setTopValue(value);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    BitDocIdSet children = childFilter.getDocIdSet(context);
    if (children == null) {
      childDocuments = null;
    } else {
      childDocuments = children.bits();
    }
    BitDocIdSet parents = parentFilter.getDocIdSet(context);
    if (parents == null) {
      parentDocuments = null;
    } else {
      parentDocuments = parents.bits();
    }
    wrappedLeafComparator = wrappedComparator.getLeafComparator(context);
  }

  @Override
  public Object value(int slot) {
    return wrappedComparator.value(slot);
  }

  /**
   * Concrete implementation of {@link ToParentBlockJoinSortField} to sorts the parent docs with the lowest values
   * in the child / nested docs first.
   */
  public static final class Lowest extends ToParentBlockJoinFieldComparator implements LeafFieldComparator {

    /**
     * Create ToParentBlockJoinFieldComparator.Lowest
     *
     * @param wrappedComparator The {@link LeafFieldComparator} on the child / nested level.
     * @param parentFilter Filter that identifies the parent documents.
     * @param childFilter Filter that defines which child / nested documents participates in sorting.
     * @param spareSlot The extra slot inside the wrapped comparator that is used to compare which nested document
     *                  inside the parent document scope is most competitive.
     */
    public Lowest(FieldComparator<Object> wrappedComparator, BitDocIdSetFilter parentFilter, BitDocIdSetFilter childFilter, int spareSlot) {
      super(wrappedComparator, parentFilter, childFilter, spareSlot);
    }

    @Override
    public int compareBottom(int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      // We need to copy the lowest value from all child docs into slot.
      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return 0;
      }

      // We only need to emit a single cmp value for any matching child doc
      int cmp = wrappedLeafComparator.compareBottom(childDoc);
      if (cmp > 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return cmp;
        }
        int cmp1 = wrappedLeafComparator.compareBottom(childDoc);
        if (cmp1 > 0) {
          return cmp1;
        } else {
          if (cmp1 == 0) {
            cmp = 0;
          }
        }
      }
    }

    @Override
    public void copy(int slot, int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return;
      }

      // We need to copy the lowest value from all child docs into slot.
      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      wrappedLeafComparator.copy(spareSlot, childDoc);
      wrappedLeafComparator.copy(slot, childDoc);

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return;
        }
        wrappedLeafComparator.copy(spareSlot, childDoc);
        if (wrappedComparator.compare(spareSlot, slot) < 0) {
          wrappedLeafComparator.copy(slot, childDoc);
        }
      }
    }

    @Override
    public int compareTop(int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      // We need to copy the lowest value from all nested docs into slot.
      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return 0;
      }

      // We only need to emit a single cmp value for any matching child doc
      int cmp = wrappedLeafComparator.compareBottom(childDoc);
      if (cmp > 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return cmp;
        }
        int cmp1 = wrappedLeafComparator.compareTop(childDoc);
        if (cmp1 > 0) {
          return cmp1;
        } else {
          if (cmp1 == 0) {
            cmp = 0;
          }
        }
      }
    }

  }

  /**
   * Concrete implementation of {@link ToParentBlockJoinSortField} to sorts the parent docs with the highest values
   * in the child / nested docs first.
   */
  public static final class Highest extends ToParentBlockJoinFieldComparator implements LeafFieldComparator {

    /**
     * Create ToParentBlockJoinFieldComparator.Highest
     *
     * @param wrappedComparator The {@link LeafFieldComparator} on the child / nested level.
     * @param parentFilter Filter that identifies the parent documents.
     * @param childFilter Filter that defines which child / nested documents participates in sorting.
     * @param spareSlot The extra slot inside the wrapped comparator that is used to compare which nested document
     *                  inside the parent document scope is most competitive.
     */
    public Highest(FieldComparator<Object> wrappedComparator, BitDocIdSetFilter parentFilter, BitDocIdSetFilter childFilter, int spareSlot) {
      super(wrappedComparator, parentFilter, childFilter, spareSlot);
    }

    @Override
    public int compareBottom(int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return 0;
      }

      int cmp = wrappedLeafComparator.compareBottom(childDoc);
      if (cmp < 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return cmp;
        }
        int cmp1 = wrappedLeafComparator.compareBottom(childDoc);
        if (cmp1 < 0) {
          return cmp1;
        } else {
          if (cmp1 == 0) {
            cmp = 0;
          }
        }
      }
    }

    @Override
    public void copy(int slot, int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return;
      }

      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return;
      }
      wrappedLeafComparator.copy(spareSlot, childDoc);
      wrappedLeafComparator.copy(slot, childDoc);

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return;
        }
        wrappedLeafComparator.copy(spareSlot, childDoc);
        if (wrappedComparator.compare(spareSlot, slot) > 0) {
          wrappedLeafComparator.copy(slot, childDoc);
        }
      }
    }

    @Override
    public int compareTop(int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return 0;
      }

      int cmp = wrappedLeafComparator.compareBottom(childDoc);
      if (cmp < 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == DocIdSetIterator.NO_MORE_DOCS) {
          return cmp;
        }
        int cmp1 = wrappedLeafComparator.compareTop(childDoc);
        if (cmp1 < 0) {
          return cmp1;
        } else {
          if (cmp1 == 0) {
            cmp = 0;
          }
        }
      }
    }

  }

}
