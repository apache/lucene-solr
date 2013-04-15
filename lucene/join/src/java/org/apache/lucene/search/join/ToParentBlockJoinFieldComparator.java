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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

/**
 * A field comparator that allows parent documents to be sorted by fields
 * from the nested / child documents.
 *
 * @lucene.experimental
 */
public abstract class ToParentBlockJoinFieldComparator extends FieldComparator<Object> {

  private final Filter parentFilter;
  private final Filter childFilter;
  final int spareSlot;

  FieldComparator<Object> wrappedComparator;
  FixedBitSet parentDocuments;
  FixedBitSet childDocuments;

  ToParentBlockJoinFieldComparator(FieldComparator<Object> wrappedComparator, Filter parentFilter, Filter childFilter, int spareSlot) {
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
    wrappedComparator.setBottom(slot);
  }

  @Override
  public FieldComparator<Object> setNextReader(AtomicReaderContext context) throws IOException {
    DocIdSet innerDocuments = childFilter.getDocIdSet(context, null);
    if (isEmpty(innerDocuments)) {
      this.childDocuments = null;
    } else if (innerDocuments instanceof FixedBitSet) {
      this.childDocuments = (FixedBitSet) innerDocuments;
    } else {
      DocIdSetIterator iterator = innerDocuments.iterator();
      if (iterator != null) {
        this.childDocuments = toFixedBitSet(iterator, context.reader().maxDoc());
      } else {
        childDocuments = null;
      }
    }
    DocIdSet rootDocuments = parentFilter.getDocIdSet(context, null);
    if (isEmpty(rootDocuments)) {
      this.parentDocuments = null;
    } else if (rootDocuments instanceof FixedBitSet) {
      this.parentDocuments = (FixedBitSet) rootDocuments;
    } else {
      DocIdSetIterator iterator = rootDocuments.iterator();
      if (iterator != null) {
        this.parentDocuments = toFixedBitSet(iterator, context.reader().maxDoc());
      } else {
        this.parentDocuments = null;
      }
    }

    wrappedComparator = wrappedComparator.setNextReader(context);
    return this;
  }

  private static boolean isEmpty(DocIdSet set) {
    return set == null || set == DocIdSet.EMPTY_DOCIDSET;
  }

  private static FixedBitSet toFixedBitSet(DocIdSetIterator iterator, int numBits) throws IOException {
    FixedBitSet set = new FixedBitSet(numBits);
    int doc;
    while ((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      set.set(doc);
    }
    return set;
  }

  @Override
  public Object value(int slot) {
    return wrappedComparator.value(slot);
  }

  /**
   * Concrete implementation of {@link ToParentBlockJoinSortField} to sorts the parent docs with the lowest values
   * in the child / nested docs first.
   */
  public static final class Lowest extends ToParentBlockJoinFieldComparator {

    /**
     * Create ToParentBlockJoinFieldComparator.Lowest
     *
     * @param wrappedComparator The {@link FieldComparator} on the child / nested level.
     * @param parentFilter Filter (must produce FixedBitSet per-segment) that identifies the parent documents.
     * @param childFilter Filter that defines which child / nested documents participates in sorting.
     * @param spareSlot The extra slot inside the wrapped comparator that is used to compare which nested document
     *                  inside the parent document scope is most competitive.
     */
    public Lowest(FieldComparator<Object> wrappedComparator, Filter parentFilter, Filter childFilter, int spareSlot) {
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
      if (childDoc >= parentDoc || childDoc == -1) {
        return 0;
      }

      // We only need to emit a single cmp value for any matching child doc
      int cmp = wrappedComparator.compareBottom(childDoc);
      if (cmp > 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == -1) {
          return cmp;
        }
        int cmp1 = wrappedComparator.compareBottom(childDoc);
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
      if (childDoc >= parentDoc || childDoc == -1) {
        return;
      }
      wrappedComparator.copy(spareSlot, childDoc);
      wrappedComparator.copy(slot, childDoc);

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == -1) {
          return;
        }
        wrappedComparator.copy(spareSlot, childDoc);
        if (wrappedComparator.compare(spareSlot, slot) < 0) {
          wrappedComparator.copy(slot, childDoc);
        }
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareDocToValue(int parentDoc, Object value) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      // We need to copy the lowest value from all nested docs into slot.
      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == -1) {
        return 0;
      }

      // We only need to emit a single cmp value for any matching child doc
      int cmp = wrappedComparator.compareBottom(childDoc);
      if (cmp > 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == -1) {
          return cmp;
        }
        int cmp1 = wrappedComparator.compareDocToValue(childDoc, value);
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
  public static final class Highest extends ToParentBlockJoinFieldComparator {

    /**
     * Create ToParentBlockJoinFieldComparator.Highest
     *
     * @param wrappedComparator The {@link FieldComparator} on the child / nested level.
     * @param parentFilter Filter (must produce FixedBitSet per-segment) that identifies the parent documents.
     * @param childFilter Filter that defines which child / nested documents participates in sorting.
     * @param spareSlot The extra slot inside the wrapped comparator that is used to compare which nested document
     *                  inside the parent document scope is most competitive.
     */
    public Highest(FieldComparator<Object> wrappedComparator, Filter parentFilter, Filter childFilter, int spareSlot) {
      super(wrappedComparator, parentFilter, childFilter, spareSlot);
    }

    @Override
    public int compareBottom(int parentDoc) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == -1) {
        return 0;
      }

      int cmp = wrappedComparator.compareBottom(childDoc);
      if (cmp < 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == -1) {
          return cmp;
        }
        int cmp1 = wrappedComparator.compareBottom(childDoc);
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
      if (childDoc >= parentDoc || childDoc == -1) {
        return;
      }
      wrappedComparator.copy(spareSlot, childDoc);
      wrappedComparator.copy(slot, childDoc);

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == -1) {
          return;
        }
        wrappedComparator.copy(spareSlot, childDoc);
        if (wrappedComparator.compare(spareSlot, slot) > 0) {
          wrappedComparator.copy(slot, childDoc);
        }
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareDocToValue(int parentDoc, Object value) throws IOException {
      if (parentDoc == 0 || parentDocuments == null || childDocuments == null) {
        return 0;
      }

      int prevParentDoc = parentDocuments.prevSetBit(parentDoc - 1);
      int childDoc = childDocuments.nextSetBit(prevParentDoc + 1);
      if (childDoc >= parentDoc || childDoc == -1) {
        return 0;
      }

      int cmp = wrappedComparator.compareBottom(childDoc);
      if (cmp < 0) {
        return cmp;
      }

      while (true) {
        childDoc = childDocuments.nextSetBit(childDoc + 1);
        if (childDoc >= parentDoc || childDoc == -1) {
          return cmp;
        }
        int cmp1 = wrappedComparator.compareDocToValue(childDoc, value);
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
