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

package org.apache.solr.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.packed.Direct16;
import org.apache.lucene.util.packed.Direct32;
import org.apache.lucene.util.packed.Direct8;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;


public class MissingStringLastComparatorSource extends FieldComparatorSource {
  private final BytesRef missingValueProxy;

  public MissingStringLastComparatorSource() {
    this(UnicodeUtil.BIG_TERM);
  }

  /** Creates a {@link FieldComparatorSource} that sorts null last in a normal ascending sort.
   * <tt>missingValueProxy</tt> as the value to return from FieldComparator.value()
   *
   * @param missingValueProxy   The value returned when sortValue() is called for a document missing the sort field.
   * This value is *not* normally used for sorting.
   */
  public MissingStringLastComparatorSource(BytesRef missingValueProxy) {
    this.missingValueProxy=missingValueProxy;
  }

  @Override
  public FieldComparator newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
    return new TermOrdValComparator_SML(numHits, fieldname, sortPos, reversed, missingValueProxy);
  }

}

// Copied from Lucene's TermOrdValComparator and modified since the Lucene version couldn't
// be extended.
class TermOrdValComparator_SML extends FieldComparator<Comparable> {
  private static final int NULL_ORD = Integer.MAX_VALUE;

  private final int[] ords;
  private final BytesRef[] values;
  private final int[] readerGen;

  private FieldCache.DocTermsIndex termsIndex;
  private final String field;

  private final BytesRef NULL_VAL;
  private PerSegmentComparator current;

  public TermOrdValComparator_SML(int numHits, String field, int sortPos, boolean reversed, BytesRef nullVal) {
    ords = new int[numHits];
    values = new BytesRef[numHits];
    readerGen = new int[numHits];
    this.field = field;
    this.NULL_VAL = nullVal;
  }

  @Override
  public int compare(int slot1, int slot2) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBottom(int slot) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compareBottom(int doc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copy(int slot, int doc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesRef value(int slot) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int compareValues(Comparable first, Comparable second) {
    if (first == null) {
      if (second == null) {
        return 0;
      } else {
        return 1;
      }
    } else if (second == null) {
      return -1;
    } else {
      return first.compareTo(second);
    }
  }

  @Override
  public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
    return TermOrdValComparator_SML.createComparator(context.reader, this);
  }

  // Base class for specialized (per bit width of the
  // ords) per-segment comparator.  NOTE: this is messy;
  // we do this only because hotspot can't reliably inline
  // the underlying array access when looking up doc->ord
  private static abstract class PerSegmentComparator extends FieldComparator<BytesRef> {
    protected TermOrdValComparator_SML parent;
    protected final int[] ords;
    protected final BytesRef[] values;
    protected final int[] readerGen;

    protected int currentReaderGen = -1;
    protected FieldCache.DocTermsIndex termsIndex;

    protected int bottomSlot = -1;
    protected int bottomOrd;
    protected boolean bottomSameReader = false;
    protected BytesRef bottomValue;
    protected final BytesRef tempBR = new BytesRef();


    public PerSegmentComparator(TermOrdValComparator_SML parent) {
      this.parent = parent;
      PerSegmentComparator previous = parent.current;
      if (previous != null) {
        currentReaderGen = previous.currentReaderGen;
        bottomSlot = previous.bottomSlot;
        bottomOrd = previous.bottomOrd;
        bottomValue = previous.bottomValue;
      }
      ords = parent.ords;
      values = parent.values;
      readerGen = parent.readerGen;
      termsIndex = parent.termsIndex;
      currentReaderGen++;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      return TermOrdValComparator_SML.createComparator(context.reader, parent);
    }

    @Override
    public int compare(int slot1, int slot2) {
      if (readerGen[slot1] == readerGen[slot2]) {
        return ords[slot1] - ords[slot2];
      }

      final BytesRef val1 = values[slot1];
      final BytesRef val2 = values[slot2];
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return 1;
      } else if (val2 == null) {
        return -1;
      }
      return val1.compareTo(val2);
    }

    @Override
    public void setBottom(final int bottom) {
      bottomSlot = bottom;

      bottomValue = values[bottomSlot];
      if (currentReaderGen == readerGen[bottomSlot]) {
        bottomOrd = ords[bottomSlot];
        bottomSameReader = true;
      } else {
        if (bottomValue == null) {
          // 0 ord is null for all segments
          assert ords[bottomSlot] == NULL_ORD;
          bottomOrd = NULL_ORD;
          bottomSameReader = true;
          readerGen[bottomSlot] = currentReaderGen;
        } else {
          final int index = binarySearch(tempBR, termsIndex, bottomValue);
          if (index < 0) {
            bottomOrd = -index - 2;
            bottomSameReader = false;
          } else {
            bottomOrd = index;
            // exact value match
            bottomSameReader = true;
            readerGen[bottomSlot] = currentReaderGen;
            ords[bottomSlot] = bottomOrd;
          }
        }
      }
    }

    @Override
    public BytesRef value(int slot) {
      return values==null ? parent.NULL_VAL : values[slot];
    }
  }

  // Used per-segment when bit width of doc->ord is 8:
  private static final class ByteOrdComparator extends PerSegmentComparator {
    private final byte[] readerOrds;

    public ByteOrdComparator(byte[] readerOrds, TermOrdValComparator_SML parent) {
      super(parent);
      this.readerOrds = readerOrds;
    }

    @Override
    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int order = readerOrds[doc]&0xFF;
      if (order == 0) order = NULL_ORD;
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - order;
      } else {
        // ord is only approx comparable: if they are not
        // equal, we can use that; if they are equal, we
        // must fallback to compare by value
        final int cmp = bottomOrd - order;
        if (cmp != 0) {
          return cmp;
        }

        // take care of the case where both vals are null
        if (order == NULL_ORD) return 0;

        // and at this point we know that neither value is null, so safe to compare
        termsIndex.lookup(order, tempBR);
        return bottomValue.compareTo(tempBR);
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = readerOrds[doc]&0xFF;
      if (ord == 0) {
        ords[slot] = NULL_ORD;
        values[slot] = null;
      } else {
        ords[slot] = ord;
        assert ord > 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.lookup(ord, values[slot]);
      }
      readerGen[slot] = currentReaderGen;
    }
  }

  // Used per-segment when bit width of doc->ord is 16:
  private static final class ShortOrdComparator extends PerSegmentComparator {
    private final short[] readerOrds;

    public ShortOrdComparator(short[] readerOrds, TermOrdValComparator_SML parent) {
      super(parent);
      this.readerOrds = readerOrds;
    }

    @Override
    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int order = readerOrds[doc]&0xFFFF;
      if (order == 0) order = NULL_ORD;
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - order;
      } else {
        // ord is only approx comparable: if they are not
        // equal, we can use that; if they are equal, we
        // must fallback to compare by value
        final int cmp = bottomOrd - order;
        if (cmp != 0) {
          return cmp;
        }

        // take care of the case where both vals are null
        if (order == NULL_ORD) return 0;

        // and at this point we know that neither value is null, so safe to compare
        termsIndex.lookup(order, tempBR);
        return bottomValue.compareTo(tempBR);
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = readerOrds[doc]&0xFFFF;
      if (ord == 0) {
        ords[slot] = NULL_ORD;
        values[slot] = null;
      } else {
        ords[slot] = ord;
        assert ord > 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.lookup(ord, values[slot]);
      }
      readerGen[slot] = currentReaderGen;
    }
  }

  // Used per-segment when bit width of doc->ord is 32:
  private static final class IntOrdComparator extends PerSegmentComparator {
    private final int[] readerOrds;

    public IntOrdComparator(int[] readerOrds, TermOrdValComparator_SML parent) {
      super(parent);
      this.readerOrds = readerOrds;
    }

    @Override
    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int order = readerOrds[doc];
      if (order == 0) order = NULL_ORD;
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - order;
      } else {
        // ord is only approx comparable: if they are not
        // equal, we can use that; if they are equal, we
        // must fallback to compare by value
        final int cmp = bottomOrd - order;
        if (cmp != 0) {
          return cmp;
        }

        // take care of the case where both vals are null
        if (order == NULL_ORD) return 0;

        // and at this point we know that neither value is null, so safe to compare
        termsIndex.lookup(order, tempBR);
        return bottomValue.compareTo(tempBR);
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = readerOrds[doc];
      if (ord == 0) {
        ords[slot] = NULL_ORD;
        values[slot] = null;
      } else {
        ords[slot] = ord;
        assert ord > 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.lookup(ord, values[slot]);
      }
      readerGen[slot] = currentReaderGen;
    }
  }

  // Used per-segment when bit width is not a native array
  // size (8, 16, 32):
  private static final class AnyOrdComparator extends PerSegmentComparator {
    private final PackedInts.Reader readerOrds;

    public AnyOrdComparator(PackedInts.Reader readerOrds, TermOrdValComparator_SML parent) {
      super(parent);
      this.readerOrds = readerOrds;
    }

    @Override
    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int order = (int) readerOrds.get(doc);
      if (order == 0) order = NULL_ORD;
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - order;
      } else {
        // ord is only approx comparable: if they are not
        // equal, we can use that; if they are equal, we
        // must fallback to compare by value
        final int cmp = bottomOrd - order;
        if (cmp != 0) {
          return cmp;
        }

        // take care of the case where both vals are null
        if (order == NULL_ORD) return 0;

        // and at this point we know that neither value is null, so safe to compare
        termsIndex.lookup(order, tempBR);
        return bottomValue.compareTo(tempBR);
      }

    }

    @Override
    public void copy(int slot, int doc) {
      int ord = (int) readerOrds.get(doc);
      if (ord == 0) {
        ords[slot] = NULL_ORD;
        values[slot] = null;
      } else {
        ords[slot] = ord;
        assert ord > 0;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.lookup(ord, values[slot]);
      }
      readerGen[slot] = currentReaderGen;
    }
  }

  public static FieldComparator createComparator(IndexReader reader, TermOrdValComparator_SML parent) throws IOException {
    parent.termsIndex = FieldCache.DEFAULT.getTermsIndex(reader, parent.field);
    final PackedInts.Reader docToOrd = parent.termsIndex.getDocToOrd();
    PerSegmentComparator perSegComp;

    if (docToOrd instanceof Direct8) {
      perSegComp = new ByteOrdComparator(((Direct8) docToOrd).getArray(), parent);
    } else if (docToOrd instanceof Direct16) {
      perSegComp = new ShortOrdComparator(((Direct16) docToOrd).getArray(), parent);
    } else if (docToOrd instanceof Direct32) {
      perSegComp = new IntOrdComparator(((Direct32) docToOrd).getArray(), parent);
    } else {
      perSegComp = new AnyOrdComparator(docToOrd, parent);
    }

    if (perSegComp.bottomSlot != -1) {
      perSegComp.setBottom(perSegComp.bottomSlot);
    }

    parent.current = perSegComp;
    return perSegComp;
  }
}
