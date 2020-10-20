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

package org.apache.lucene.search.comparators;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;

/**
 * Comparator that sorts by field's natural Term sort order using ordinals.
 * This is functionally equivalent to
 * {@link org.apache.lucene.search.comparators.TermValComparator},
 * but it first resolves the string to their relative ordinal positions
 * (using the index returned by
 * {@link org.apache.lucene.index.LeafReader#getSortedDocValues(String)}),
 * and does most comparisons using the ordinals.
 * For medium to large results, this comparator will be much faster
 * than {@link org.apache.lucene.search.comparators.TermValComparator}.
 * For very small result sets it may be slower.
 *
 * The comparator provides an iterator that can efficiently skip
 * documents when search sort is done according to the index sort.
 */
public class TermOrdValComparator extends FieldComparator<BytesRef> {
  private final String field;
  private final boolean reverse;
  private final int[] ords; // ordinals for each slot
  private final BytesRef[] values; // values for each slot
  private final BytesRefBuilder[] tempBRs;
  /* Which reader last copied a value into the slot. When
     we compare two slots, we just compare-by-ord if the
     readerGen is the same; else we must compare the
     values (slower).*/
  private final int[] readers;
  private int currentReader = -1; // index of the current reader we are on
  private final int missingSortCmp; // -1 – if missing values are sorted first, 1 – if sorted last
  private final int missingOrd; // which ordinal to use for a missing value

  private BytesRef topValue;
  private boolean topSameReader;
  private int topOrd;

  private BytesRef bottomValue;
  boolean bottomSameReader; // true if current bottom slot matches the current reader
  int bottomSlot = -1; // bottom slot, or -1 if queue isn't full yet
  int bottomOrd; // bottom ord (same as ords[bottomSlot] once bottomSlot is set), cached for faster comparison

  protected boolean hitsThresholdReached;

  public TermOrdValComparator(int numHits, String field, boolean sortMissingLast, boolean reverse) {
    this.field = field;
    this.reverse = reverse;
    this.ords = new int[numHits];
    this.values = new BytesRef[numHits];
    tempBRs = new BytesRefBuilder[numHits];
    readers = new int[numHits];
    if (sortMissingLast) {
      missingSortCmp = 1;
      missingOrd = Integer.MAX_VALUE;
    } else {
      missingSortCmp = -1;
      missingOrd = -1;
    }
  }

  @Override
  public int compare(int slot1, int slot2) {
    if (readers[slot1] == readers[slot2]) {
      return ords[slot1] - ords[slot2];
    }
    final BytesRef val1 = values[slot1];
    final BytesRef val2 = values[slot2];
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }

  @Override
  public void setTopValue(BytesRef value) {
    // null is accepted, this means the last doc of the prior search was missing this value
    topValue = value;
  }

  @Override
  public BytesRef value(int slot) {
    return values[slot];
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new TermOrdValLeafComparator(context);
  }

  @Override
  public int compareValues(BytesRef val1, BytesRef val2) {
    if (val1 == null) {
      if (val2 == null) {
        return 0;
      }
      return missingSortCmp;
    } else if (val2 == null) {
      return -missingSortCmp;
    }
    return val1.compareTo(val2);
  }

  /**
   * Leaf comparator for {@link TermOrdValComparator} that provides skipping functionality when index is sorted
   */
  public class TermOrdValLeafComparator implements LeafFieldComparator {
    private final SortedDocValues termsIndex;
    private boolean indexSort = false; // true if a query sort is a part of the index sort
    private DocIdSetIterator competitiveIterator;
    private boolean collectedAllCompetitiveHits = false;
    private boolean iteratorUpdated = false;

    public TermOrdValLeafComparator(LeafReaderContext context) throws IOException {
      termsIndex = getSortedDocValues(context, field);
      currentReader++;
      if (topValue != null) {
        // Recompute topOrd/SameReader
        int ord = termsIndex.lookupTerm(topValue);
        if (ord >= 0) {
          topSameReader = true;
          topOrd = ord;
        } else {
          topSameReader = false;
          topOrd = -ord-2;
        }
      } else {
        topOrd = missingOrd;
        topSameReader = true;
      }
      if (bottomSlot != -1) {
        // Recompute bottomOrd/SameReader
        setBottom(bottomSlot);
      }
      this.competitiveIterator = DocIdSetIterator.all(context.reader().maxDoc());
    }

    protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
      return DocValues.getSorted(context.reader(), field);
    }

    @Override
    public void setBottom(final int slot) throws IOException {
      bottomSlot = slot;
      bottomValue = values[bottomSlot];
      if (currentReader == readers[bottomSlot]) {
        bottomOrd = ords[bottomSlot];
        bottomSameReader = true;
      } else {
        if (bottomValue == null) {
          // missingOrd is null for all segments
          assert ords[bottomSlot] == missingOrd;
          bottomOrd = missingOrd;
          bottomSameReader = true;
          readers[bottomSlot] = currentReader;
        } else {
          final int ord = termsIndex.lookupTerm(bottomValue);
          if (ord < 0) {
            bottomOrd = -ord - 2;
            bottomSameReader = false;
          } else {
            bottomOrd = ord;
            // exact value match
            bottomSameReader = true;
            readers[bottomSlot] = currentReader;
            ords[bottomSlot] = bottomOrd;
          }
        }
      }
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      assert bottomSlot != -1;
      int docOrd = getOrdForDoc(doc);
      if (docOrd == -1) {
        docOrd = missingOrd;
      }
      int result;
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        result = bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in setBottom):
        result = 1;
      } else {
        result = -1;
      }
      // for the index sort case, if we encounter a first doc that is non-competitive,
      // and the hits threshold is reached, we can update the iterator to skip the rest of docs
      if (indexSort && (reverse ? result >= 0 : result <= 0)) {
        collectedAllCompetitiveHits = true;
        if (hitsThresholdReached && iteratorUpdated == false) {
          competitiveIterator = DocIdSetIterator.empty();
          iteratorUpdated = true;
        }
      }
      return result;
    }

    @Override
    public int compareTop(int doc) throws IOException {
      int ord = getOrdForDoc(doc);
      if (ord == -1) {
        ord = missingOrd;
      }
      if (topSameReader) {
        // ord is precisely comparable, even in the equal case
        return topOrd - ord;
      } else if (ord <= topOrd) {
        // the equals case always means doc is < value (because we set lastOrd to the lower bound)
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      int ord = getOrdForDoc(doc);
      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (tempBRs[slot] == null) {
          tempBRs[slot] = new BytesRefBuilder();
        }
        tempBRs[slot].copyBytes(termsIndex.lookupOrd(ord));
        values[slot] = tempBRs[slot].get();
      }
      ords[slot] = ord;
      readers[slot] = currentReader;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {}

    @Override
    public void usesIndexSort() {
      indexSort = true;
    }

    @Override
    public void setHitsThresholdReached() {
      hitsThresholdReached = true;
      // for the index sort case, if we collected collected all competitive hits
      // we can update the iterator to skip the rest of docs
      if (indexSort && collectedAllCompetitiveHits && iteratorUpdated == false) {
        competitiveIterator = DocIdSetIterator.empty();
        iteratorUpdated = true;
      }
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      if (indexSort == false) return null;
      return new DocIdSetIterator() {
        private int docID = -1;

        @Override
        public int nextDoc() throws IOException {
          return advance(docID + 1);
        }

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public long cost() {
          return competitiveIterator.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return docID = competitiveIterator.advance(target);
        }
      };
    }

    private int getOrdForDoc(int doc) throws IOException {
      if (termsIndex.advanceExact(doc)) {
        return termsIndex.ordValue();
      } else {
        return -1;
      }
    }
  }
}
