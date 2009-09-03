package org.apache.lucene.search;

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

import java.io.IOException;
import java.text.Collator;
import java.util.Locale;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache.DoubleParser;
import org.apache.lucene.search.FieldCache.LongParser;
import org.apache.lucene.search.FieldCache.ByteParser;
import org.apache.lucene.search.FieldCache.FloatParser;
import org.apache.lucene.search.FieldCache.IntParser;
import org.apache.lucene.search.FieldCache.ShortParser;
import org.apache.lucene.search.FieldCache.StringIndex;

/**
 * Expert: a FieldComparator compares hits so as to determine their
 * sort order when collecting the top results with {@link
 * TopFieldCollector}.  The concrete public FieldComparator
 * classes here correspond to the SortField types.
 *
 * <p>This API is designed to achieve high performance
 * sorting, by exposing a tight interaction with {@link
 * FieldValueHitQueue} as it visits hits.  Whenever a hit is
 * competitive, it's enrolled into a virtual slot, which is
 * an int ranging from 0 to numHits-1.  The {@link
 * FieldComparator} is made aware of segment transitions
 * during searching in case any internal state it's tracking
 * needs to be recomputed during these transitions.</p>
 *
 * <p>A comparator must define these functions:</p>
 *
 * <ul>
 *
 *  <li> {@link #compare} Compare a hit at 'slot a'
 *       with hit 'slot b'.
 *
 *  <li> {@link #setBottom} This method is called by
 *       {@link FieldValueHitQueue} to notify the
 *       FieldComparator of the current weakest ("bottom")
 *       slot.  Note that this slot may not hold the weakest
 *       value according to your comparator, in cases where
 *       your comparator is not the primary one (ie, is only
 *       used to break ties from the comparators before it).
 *
 *  <li> {@link #compareBottom} Compare a new hit (docID)
 *       against the "weakest" (bottom) entry in the queue.
 *
 *  <li> {@link #copy} Installs a new hit into the
 *       priority queue.  The {@link FieldValueHitQueue}
 *       calls this method when a new hit is competitive.
 *
 *  <li> {@link #setNextReader} Invoked
 *       when the search is switching to the next segment.
 *       You may need to update internal state of the
 *       comparator, for example retrieving new values from
 *       the {@link FieldCache}.
 *
 *  <li> {@link #value} Return the sort value stored in
 *       the specified slot.  This is only called at the end
 *       of the search, in order to populate {@link
 *       FieldDoc#fields} when returning the top results.
 * </ul>
 *
 * <b>NOTE:</b> This API is experimental and might change in
 * incompatible ways in the next release.
 */
public abstract class FieldComparator {

  /** Parses field's values as byte (using {@link
   *  FieldCache#getBytes} and sorts by ascending value */
  public static final class ByteComparator extends FieldComparator {
    private final byte[] values;
    private byte[] currentReaderValues;
    private final String field;
    private ByteParser parser;
    private byte bottom;

    ByteComparator(int numHits, String field, FieldCache.Parser parser) {
      values = new byte[numHits];
      this.field = field;
      this.parser = (ByteParser) parser;
    }

    public int compare(int slot1, int slot2) {
      return values[slot1] - values[slot2];
    }

    public int compareBottom(int doc) {
      return bottom - currentReaderValues[doc];
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getBytes(reader, field, parser);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return new Byte(values[slot]);
    }
  }

  /** Sorts by ascending docID */
  public static final class DocComparator extends FieldComparator {
    private final int[] docIDs;
    private int docBase;
    private int bottom;

    DocComparator(int numHits) {
      docIDs = new int[numHits];
    }

    public int compare(int slot1, int slot2) {
      // No overflow risk because docIDs are non-negative
      return docIDs[slot1] - docIDs[slot2];
    }

    public int compareBottom(int doc) {
      // No overflow risk because docIDs are non-negative
      return bottom - (docBase + doc);
    }

    public void copy(int slot, int doc) {
      docIDs[slot] = docBase + doc;
    }

    public void setNextReader(IndexReader reader, int docBase) {
      // TODO: can we "map" our docIDs to the current
      // reader? saves having to then subtract on every
      // compare call
      this.docBase = docBase;
    }
    
    public void setBottom(final int bottom) {
      this.bottom = docIDs[bottom];
    }

    public Comparable value(int slot) {
      return new Integer(docIDs[slot]);
    }
  }

  /** Parses field's values as double (using {@link
   *  FieldCache#getDoubles} and sorts by ascending value */
  public static final class DoubleComparator extends FieldComparator {
    private final double[] values;
    private double[] currentReaderValues;
    private final String field;
    private DoubleParser parser;
    private double bottom;

    DoubleComparator(int numHits, String field, FieldCache.Parser parser) {
      values = new double[numHits];
      this.field = field;
      this.parser = (DoubleParser) parser;
    }

    public int compare(int slot1, int slot2) {
      final double v1 = values[slot1];
      final double v2 = values[slot2];
      if (v1 > v2) {
        return 1;
      } else if (v1 < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public int compareBottom(int doc) {
      final double v2 = currentReaderValues[doc];
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getDoubles(reader, field, parser);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return new Double(values[slot]);
    }
  }

  /** Parses field's values as float (using {@link
   *  FieldCache#getFloats} and sorts by ascending value */
  public static final class FloatComparator extends FieldComparator {
    private final float[] values;
    private float[] currentReaderValues;
    private final String field;
    private FloatParser parser;
    private float bottom;

    FloatComparator(int numHits, String field, FieldCache.Parser parser) {
      values = new float[numHits];
      this.field = field;
      this.parser = (FloatParser) parser;
    }

    public int compare(int slot1, int slot2) {
      // TODO: are there sneaky non-branch ways to compute
      // sign of float?
      final float v1 = values[slot1];
      final float v2 = values[slot2];
      if (v1 > v2) {
        return 1;
      } else if (v1 < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public int compareBottom(int doc) {
      // TODO: are there sneaky non-branch ways to compute
      // sign of float?
      final float v2 = currentReaderValues[doc];
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getFloats(reader, field, parser);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return new Float(values[slot]);
    }
  }

  /** Parses field's values as int (using {@link
   *  FieldCache#getInts} and sorts by ascending value */
  public static final class IntComparator extends FieldComparator {
    private final int[] values;
    private int[] currentReaderValues;
    private final String field;
    private IntParser parser;
    private int bottom;                           // Value of bottom of queue

    IntComparator(int numHits, String field, FieldCache.Parser parser) {
      values = new int[numHits];
      this.field = field;
      this.parser = (IntParser) parser;
    }

    public int compare(int slot1, int slot2) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      // Cannot return values[slot1] - values[slot2] because that
      // may overflow
      final int v1 = values[slot1];
      final int v2 = values[slot2];
      if (v1 > v2) {
        return 1;
      } else if (v1 < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public int compareBottom(int doc) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      // Cannot return bottom - values[slot2] because that
      // may overflow
      final int v2 = currentReaderValues[doc];
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getInts(reader, field, parser);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return new Integer(values[slot]);
    }
  }

  /** Parses field's values as long (using {@link
   *  FieldCache#getLongs} and sorts by ascending value */
  public static final class LongComparator extends FieldComparator {
    private final long[] values;
    private long[] currentReaderValues;
    private final String field;
    private LongParser parser;
    private long bottom;

    LongComparator(int numHits, String field, FieldCache.Parser parser) {
      values = new long[numHits];
      this.field = field;
      this.parser = (LongParser) parser;
    }

    public int compare(int slot1, int slot2) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      final long v1 = values[slot1];
      final long v2 = values[slot2];
      if (v1 > v2) {
        return 1;
      } else if (v1 < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public int compareBottom(int doc) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      final long v2 = currentReaderValues[doc];
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getLongs(reader, field, parser);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return new Long(values[slot]);
    }
  }

  /** Sorts by descending relevance.  NOTE: if you are
   *  sorting only by descending relevance and then
   *  secondarily by ascending docID, performance is faster
   *  using {@link TopScoreDocCollector} directly (which {@link
   *  IndexSearcher#search} uses when no {@link Sort} is
   *  specified). */
  public static final class RelevanceComparator extends FieldComparator {
    private final float[] scores;
    private float bottom;
    private Scorer scorer;
    
    RelevanceComparator(int numHits) {
      scores = new float[numHits];
    }

    public int compare(int slot1, int slot2) {
      final float score1 = scores[slot1];
      final float score2 = scores[slot2];
      return score1 > score2 ? -1 : (score1 < score2 ? 1 : 0);
    }

    public int compareBottom(int doc) throws IOException {
      float score = scorer.score();
      return bottom > score ? -1 : (bottom < score ? 1 : 0);
    }

    public void copy(int slot, int doc) throws IOException {
      scores[slot] = scorer.score();
    }

    public void setNextReader(IndexReader reader, int docBase) {
    }
    
    public void setBottom(final int bottom) {
      this.bottom = scores[bottom];
    }

    public void setScorer(Scorer scorer) {
      // wrap with a ScoreCachingWrappingScorer so that successive calls to
      // score() will not incur score computation over and over again.
      this.scorer = new ScoreCachingWrappingScorer(scorer);
    }
    
    public Comparable value(int slot) {
      return new Float(scores[slot]);
    }
  }

  /** Parses field's values as short (using {@link
   *  FieldCache#getShorts} and sorts by ascending value */
  public static final class ShortComparator extends FieldComparator {
    private final short[] values;
    private short[] currentReaderValues;
    private final String field;
    private ShortParser parser;
    private short bottom;

    ShortComparator(int numHits, String field, FieldCache.Parser parser) {
      values = new short[numHits];
      this.field = field;
      this.parser = (ShortParser) parser;
    }

    public int compare(int slot1, int slot2) {
      return values[slot1] - values[slot2];
    }

    public int compareBottom(int doc) {
      return bottom - currentReaderValues[doc];
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getShorts(reader, field, parser);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return new Short(values[slot]);
    }
  }

  /** Sorts by a field's value using the Collator for a
   *  given Locale.*/
  public static final class StringComparatorLocale extends FieldComparator {

    private final String[] values;
    private String[] currentReaderValues;
    private final String field;
    final Collator collator;
    private String bottom;

    StringComparatorLocale(int numHits, String field, Locale locale) {
      values = new String[numHits];
      this.field = field;
      collator = Collator.getInstance(locale);
    }

    public int compare(int slot1, int slot2) {
      final String val1 = values[slot1];
      final String val2 = values[slot2];
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return -1;
      } else if (val2 == null) {
        return 1;
      }
      return collator.compare(val1, val2);
    }

    public int compareBottom(int doc) {
      final String val2 = currentReaderValues[doc];
      if (bottom == null) {
        if (val2 == null) {
          return 0;
        }
        return -1;
      } else if (val2 == null) {
        return 1;
      }
      return collator.compare(bottom, val2);
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getStrings(reader, field);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return values[slot];
    }
  }

  /** Sorts by field's natural String sort order, using
   *  ordinals.  This is functionally equivalent to {@link
   *  StringValComparator}, but it first resolves the string
   *  to their relative ordinal positions (using the index
   *  returned by {@link FieldCache#getStringIndex}), and
   *  does most comparisons using the ordinals.  For medium
   *  to large results, this comparator will be much faster
   *  than {@link StringValComparator}.  For very small
   *  result sets it may be slower. */
  public static final class StringOrdValComparator extends FieldComparator {

    private final int[] ords;
    private final String[] values;
    private final int[] readerGen;

    private int currentReaderGen = -1;
    private String[] lookup;
    private int[] order;
    private final String field;

    private int bottomSlot = -1;
    private int bottomOrd;
    private String bottomValue;
    private final boolean reversed;
    private final int sortPos;

    public StringOrdValComparator(int numHits, String field, int sortPos, boolean reversed) {
      ords = new int[numHits];
      values = new String[numHits];
      readerGen = new int[numHits];
      this.sortPos = sortPos;
      this.reversed = reversed;
      this.field = field;
    }

    public int compare(int slot1, int slot2) {
      if (readerGen[slot1] == readerGen[slot2]) {
        int cmp = ords[slot1] - ords[slot2];
        if (cmp != 0) {
          return cmp;
        }
      }

      final String val1 = values[slot1];
      final String val2 = values[slot2];
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return -1;
      } else if (val2 == null) {
        return 1;
      }
      return val1.compareTo(val2);
    }

    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int order = this.order[doc];
      final int cmp = bottomOrd - order;
      if (cmp != 0) {
        return cmp;
      }

      final String val2 = lookup[order];
      if (bottomValue == null) {
        if (val2 == null) {
          return 0;
        }
        // bottom wins
        return -1;
      } else if (val2 == null) {
        // doc wins
        return 1;
      }
      return bottomValue.compareTo(val2);
    }

    private void convert(int slot) {
      readerGen[slot] = currentReaderGen;
      int index = 0;
      String value = values[slot];
      if (value == null) {
        ords[slot] = 0;
        return;
      }

      if (sortPos == 0 && bottomSlot != -1 && bottomSlot != slot) {
        // Since we are the primary sort, the entries in the
        // queue are bounded by bottomOrd:
        assert bottomOrd < lookup.length;
        if (reversed) {
          index = binarySearch(lookup, value, bottomOrd, lookup.length-1);
        } else {
          index = binarySearch(lookup, value, 0, bottomOrd);
        }
      } else {
        // Full binary search
        index = binarySearch(lookup, value);
      }

      if (index < 0) {
        index = -index - 2;
      }
      ords[slot] = index;
    }

    public void copy(int slot, int doc) {
      final int ord = order[doc];
      ords[slot] = ord;
      assert ord >= 0;
      values[slot] = lookup[ord];
      readerGen[slot] = currentReaderGen;
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      StringIndex currentReaderValues = FieldCache.DEFAULT.getStringIndex(reader, field);
      currentReaderGen++;
      order = currentReaderValues.order;
      lookup = currentReaderValues.lookup;
      assert lookup.length > 0;
      if (bottomSlot != -1) {
        convert(bottomSlot);
        bottomOrd = ords[bottomSlot];
      }
    }
    
    public void setBottom(final int bottom) {
      bottomSlot = bottom;
      if (readerGen[bottom] != currentReaderGen) {
        convert(bottomSlot);
      }
      bottomOrd = ords[bottom];
      assert bottomOrd >= 0;
      assert bottomOrd < lookup.length;
      bottomValue = values[bottom];
    }

    public Comparable value(int slot) {
      return values[slot];
    }

    public String[] getValues() {
      return values;
    }

    public int getBottomSlot() {
      return bottomSlot;
    }

    public String getField() {
      return field;
    }
  }

  /** Sorts by field's natural String sort order.  All
   *  comparisons are done using String.compareTo, which is
   *  slow for medium to large result sets but possibly
   *  very fast for very small results sets. */
  public static final class StringValComparator extends FieldComparator {

    private String[] values;
    private String[] currentReaderValues;
    private final String field;
    private String bottom;

    StringValComparator(int numHits, String field) {
      values = new String[numHits];
      this.field = field;
    }

    public int compare(int slot1, int slot2) {
      final String val1 = values[slot1];
      final String val2 = values[slot2];
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return -1;
      } else if (val2 == null) {
        return 1;
      }

      return val1.compareTo(val2);
    }

    public int compareBottom(int doc) {
      final String val2 = currentReaderValues[doc];
      if (bottom == null) {
        if (val2 == null) {
          return 0;
        }
        return -1;
      } else if (val2 == null) {
        return 1;
      }
      return bottom.compareTo(val2);
    }

    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues[doc];
    }

    public void setNextReader(IndexReader reader, int docBase) throws IOException {
      currentReaderValues = FieldCache.DEFAULT.getStrings(reader, field);
    }
    
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    public Comparable value(int slot) {
      return values[slot];
    }
  }

  final protected static int binarySearch(String[] a, String key) {
    return binarySearch(a, key, 0, a.length-1);
  }

  final protected static int binarySearch(String[] a, String key, int low, int high) {

    while (low <= high) {
      int mid = (low + high) >>> 1;
      String midVal = a[mid];
      int cmp;
      if (midVal != null) {
        cmp = midVal.compareTo(key);
      } else {
        cmp = -1;
      }

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid;
    }
    return -(low + 1);
  }

  /**
   * Compare hit at slot1 with hit at slot2.
   * 
   * @param slot1 first slot to compare
   * @param slot2 second slot to compare
   * @return any N < 0 if slot2's value is sorted after
   * slot1, any N > 0 if the slot2's value is sorted before
   * slot1 and 0 if they are equal
   */
  public abstract int compare(int slot1, int slot2);

  /**
   * Set the bottom slot, ie the "weakest" (sorted last)
   * entry in the queue.  When {@link #compareBottom} is
   * called, you should compare against this slot.  This
   * will always be called before {@link #compareBottom}.
   * 
   * @param slot the currently weakest (sorted last) slot in the queue
   */
  public abstract void setBottom(final int slot);

  /**
   * Compare the bottom of the queue with doc.  This will
   * only invoked after setBottom has been called.  This
   * should return the same result as {@link
   * #compare(int,int)}} as if bottom were slot1 and the new
   * document were slot 2.
   *    
   * <p>For a search that hits many results, this method
   * will be the hotspot (invoked by far the most
   * frequently).</p>
   * 
   * @param doc that was hit
   * @return any N < 0 if the doc's value is sorted after
   * the bottom entry (not competitive), any N > 0 if the
   * doc's value is sorted before the bottom entry and 0 if
   * they are equal.
   */
  public abstract int compareBottom(int doc) throws IOException;

  /**
   * This method is called when a new hit is competitive.
   * You should copy any state associated with this document
   * that will be required for future comparisons, into the
   * specified slot.
   * 
   * @param slot which slot to copy the hit to
   * @param doc docID relative to current reader
   */
  public abstract void copy(int slot, int doc) throws IOException;

  /**
   * Set a new Reader. All doc correspond to the current Reader.
   * 
   * @param reader current reader
   * @param docBase docBase of this reader 
   * @throws IOException
   * @throws IOException
   */
  public abstract void setNextReader(IndexReader reader, int docBase) throws IOException;

  /** Sets the Scorer to use in case a document's score is
   *  needed.
   * 
   * @param scorer Scorer instance that you should use to
   * obtain the current hit's score, if necessary. */
  public void setScorer(Scorer scorer) {
    // Empty implementation since most comparators don't need the score. This
    // can be overridden by those that need it.
  }
  
  /**
   * Return the actual value in the slot.
   *
   * @param slot the value
   * @return value in this slot upgraded to Comparable
   */
  public abstract Comparable value(int slot);
}
