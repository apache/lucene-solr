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

import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.index.values.IndexDocValues;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.search.FieldCache.DocTermsIndex;
import org.apache.lucene.search.cache.ByteValuesCreator;
import org.apache.lucene.search.cache.CachedArray;
import org.apache.lucene.search.cache.CachedArrayCreator;
import org.apache.lucene.search.cache.DoubleValuesCreator;
import org.apache.lucene.search.cache.FloatValuesCreator;
import org.apache.lucene.search.cache.IntValuesCreator;
import org.apache.lucene.search.cache.LongValuesCreator;
import org.apache.lucene.search.cache.ShortValuesCreator;
import org.apache.lucene.search.cache.CachedArray.ByteValues;
import org.apache.lucene.search.cache.CachedArray.DoubleValues;
import org.apache.lucene.search.cache.CachedArray.FloatValues;
import org.apache.lucene.search.cache.CachedArray.IntValues;
import org.apache.lucene.search.cache.CachedArray.LongValues;
import org.apache.lucene.search.cache.CachedArray.ShortValues;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.Direct16;
import org.apache.lucene.util.packed.Direct32;
import org.apache.lucene.util.packed.Direct8;
import org.apache.lucene.util.packed.PackedInts;

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
 *  <li> {@link #setNextReader(IndexReader.AtomicReaderContext)} Invoked
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
 * @lucene.experimental
 */
public abstract class FieldComparator<T> {

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
   * Set a new {@link AtomicReaderContext}. All subsequent docIDs are relative to
   * the current reader (you must add docBase if you need to
   * map it to a top-level docID).
   * 
   * @param context current reader context
   * @return the comparator to use for this segment; most
   *   comparators can just return "this" to reuse the same
   *   comparator across segments
   * @throws IOException
   */
  public abstract FieldComparator setNextReader(AtomicReaderContext context) throws IOException;

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
   * @return value in this slot
   */
  public abstract T value(int slot);

  /** Returns -1 if first is less than second.  Default
   *  impl to assume the type implements Comparable and
   *  invoke .compareTo; be sure to override this method if
   *  your FieldComparator's type isn't a Comparable or
   *  if your values may sometimes be null */
  @SuppressWarnings("unchecked")
  public int compareValues(T first, T second) {
    return ((Comparable<T>) first).compareTo(second);
  }

  public static abstract class NumericComparator<T extends CachedArray, U extends Number> extends FieldComparator<U> {
    protected final CachedArrayCreator<T> creator;
    protected T cached;
    protected final boolean checkMissing;
    protected Bits valid;
    
    public NumericComparator( CachedArrayCreator<T> c, boolean checkMissing ) {
      this.creator = c;
      this.checkMissing = checkMissing;
    }

    protected FieldComparator setup(T cached) {
      this.cached = cached;
      if (checkMissing)
        valid = cached.valid;
      return this;
    }
  }

  /** Parses field's values as byte (using {@link
   *  FieldCache#getBytes} and sorts by ascending value */
  public static final class ByteComparator extends NumericComparator<ByteValues,Byte> {
    private byte[] docValues;
    private final byte[] values;
    private final byte missingValue;
    private byte bottom;

    ByteComparator(int numHits, ByteValuesCreator creator, Byte missingValue ) {
      super( creator, missingValue!=null );
      values = new byte[numHits];
      this.missingValue = checkMissing
         ? missingValue.byteValue() : 0;
    }

    @Override
    public int compare(int slot1, int slot2) {
      return values[slot1] - values[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      byte v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      return bottom - v2;
    }

    @Override
    public void copy(int slot, int doc) {
      byte v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      values[slot] = v2;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      setup(FieldCache.DEFAULT.getBytes(context.reader, creator.field, creator));
      docValues = cached.values;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Byte value(int slot) {
      return Byte.valueOf(values[slot]);
    }
  }

  
  /** Parses field's values as double (using {@link
   *  FieldCache#getDoubles} and sorts by ascending value */
  public static final class DoubleComparator extends NumericComparator<DoubleValues,Double> {
    private double[] docValues;
    private final double[] values;
    private final double missingValue;
    private double bottom;

    DoubleComparator(int numHits, DoubleValuesCreator creator, Double missingValue ) {
      super( creator, missingValue != null );
      values = new double[numHits];
      this.missingValue = checkMissing
        ? missingValue.doubleValue() : 0;
    }

    @Override
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

    @Override
    public int compareBottom(int doc) {
      double v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      double v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      values[slot] = v2;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      setup(FieldCache.DEFAULT.getDoubles(context.reader, creator.field, creator));
      docValues = cached.values;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Double value(int slot) {
      return Double.valueOf(values[slot]);
    }
  }

  /** Uses float index values to sort by ascending value */
  public static final class FloatDocValuesComparator extends FieldComparator<Double> {
    private final double[] values;
    private Source currentReaderValues;
    private final String field;
    private double bottom;

    FloatDocValuesComparator(int numHits, String field) {
      values = new double[numHits];
      this.field = field;
    }

    @Override
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

    @Override
    public int compareBottom(int doc) {
      final double v2 = currentReaderValues.getFloat(doc);
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues.getFloat(doc); 
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      final IndexDocValues docValues = context.reader.docValues(field);
      if (docValues != null) {
        currentReaderValues = docValues.getSource(); 
      }
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Double value(int slot) {
      return Double.valueOf(values[slot]);
    }
  }

  /** Parses field's values as float (using {@link
   *  FieldCache#getFloats} and sorts by ascending value */
  public static final class FloatComparator extends NumericComparator<FloatValues,Float> {
    private float[] docValues;
    private final float[] values;
    private final float missingValue;
    private float bottom;

    FloatComparator(int numHits, FloatValuesCreator creator, Float missingValue ) {
      super( creator, missingValue != null );
      values = new float[numHits];
      this.missingValue = checkMissing
        ? missingValue.floatValue() : 0;
    }
    
    @Override
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

    @Override
    public int compareBottom(int doc) {
      // TODO: are there sneaky non-branch ways to compute sign of float?
      float v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      float v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      values[slot] = v2;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      setup(FieldCache.DEFAULT.getFloats(context.reader, creator.field, creator));
      docValues = cached.values;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Float value(int slot) {
      return Float.valueOf(values[slot]);
    }
  }

  /** Parses field's values as short (using {@link
   *  FieldCache#getShorts} and sorts by ascending value */
  public static final class ShortComparator extends NumericComparator<ShortValues,Short> {
    private short[] docValues;
    private final short[] values;
    private short bottom;
    private final short missingValue;

    ShortComparator(int numHits, ShortValuesCreator creator, Short missingValue ) {
      super( creator, missingValue != null );
      values = new short[numHits];
      this.missingValue = checkMissing
        ? missingValue.shortValue() : 0;
    }

    @Override
    public int compare(int slot1, int slot2) {
      return values[slot1] - values[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      short v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      return bottom - v2;
    }

    @Override
    public void copy(int slot, int doc) {
      short v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      values[slot] = v2;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      setup( FieldCache.DEFAULT.getShorts(context.reader, creator.field, creator));
      docValues = cached.values;
      return this;
    }

    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Short value(int slot) {
      return Short.valueOf(values[slot]);
    }
  }

  /** Parses field's values as int (using {@link
   *  FieldCache#getInts} and sorts by ascending value */
  public static final class IntComparator extends NumericComparator<IntValues,Integer> {
    private int[] docValues;
    private final int[] values;
    private int bottom;                           // Value of bottom of queue
    final int missingValue;
    
    IntComparator(int numHits, IntValuesCreator creator, Integer missingValue ) {
      super( creator, missingValue != null );
      values = new int[numHits];
      this.missingValue = checkMissing
        ? missingValue.intValue() : 0;
    }
        
    @Override
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

    @Override
    public int compareBottom(int doc) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      // Cannot return bottom - values[slot2] because that
      // may overflow
      int v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      values[slot] = v2;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      setup(FieldCache.DEFAULT.getInts(context.reader, creator.field, creator));
      docValues = cached.values;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Integer value(int slot) {
      return Integer.valueOf(values[slot]);
    }
  }

  /** Loads int index values and sorts by ascending value. */
  public static final class IntDocValuesComparator extends FieldComparator<Long> {
    private final long[] values;
    private Source currentReaderValues;
    private final String field;
    private long bottom;

    IntDocValuesComparator(int numHits, String field) {
      values = new long[numHits];
      this.field = field;
    }

    @Override
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

    @Override
    public int compareBottom(int doc) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      final long v2 = currentReaderValues.getInt(doc);
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      values[slot] = currentReaderValues.getInt(doc); 
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      IndexDocValues docValues = context.reader.docValues(field);
      if (docValues != null) {
        currentReaderValues = docValues.getSource();
      }
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Long value(int slot) {
      return Long.valueOf(values[slot]);
    }
  }

  /** Parses field's values as long (using {@link
   *  FieldCache#getLongs} and sorts by ascending value */
  public static final class LongComparator extends NumericComparator<LongValues,Long> {
    private long[] docValues;
    private final long[] values;
    private long bottom;
    private final long missingValue;

    LongComparator(int numHits, LongValuesCreator creator, Long missingValue ) {
      super( creator, missingValue != null );
      values = new long[numHits];
      this.missingValue = checkMissing
        ? missingValue.longValue() : 0;
    }
    
    @Override
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

    @Override
    public int compareBottom(int doc) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      long v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      
      if (bottom > v2) {
        return 1;
      } else if (bottom < v2) {
        return -1;
      } else {
        return 0;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      long v2 = docValues[doc];
      if (valid != null && v2==0 && !valid.get(doc))
        v2 = missingValue;

      values[slot] = v2;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      setup(FieldCache.DEFAULT.getLongs(context.reader, creator.field, creator));
      docValues = cached.values;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public Long value(int slot) {
      return Long.valueOf(values[slot]);
    }
  }

  /** Sorts by descending relevance.  NOTE: if you are
   *  sorting only by descending relevance and then
   *  secondarily by ascending docID, performance is faster
   *  using {@link TopScoreDocCollector} directly (which {@link
   *  IndexSearcher#search} uses when no {@link Sort} is
   *  specified). */
  public static final class RelevanceComparator extends FieldComparator<Float> {
    private final float[] scores;
    private float bottom;
    private Scorer scorer;
    
    RelevanceComparator(int numHits) {
      scores = new float[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      final float score1 = scores[slot1];
      final float score2 = scores[slot2];
      return score1 > score2 ? -1 : (score1 < score2 ? 1 : 0);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      float score = scorer.score();
      return bottom > score ? -1 : (bottom < score ? 1 : 0);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      scores[slot] = scorer.score();
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) {
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = scores[bottom];
    }

    @Override
    public void setScorer(Scorer scorer) {
      // wrap with a ScoreCachingWrappingScorer so that successive calls to
      // score() will not incur score computation over and
      // over again.
      if (!(scorer instanceof ScoreCachingWrappingScorer)) {
        this.scorer = new ScoreCachingWrappingScorer(scorer);
      } else {
        this.scorer = scorer;
      }
    }
    
    @Override
    public Float value(int slot) {
      return Float.valueOf(scores[slot]);
    }

    // Override because we sort reverse of natural Float order:
    @Override
    public int compareValues(Float first, Float second) {
      // Reversed intentionally because relevance by default
      // sorts descending:
      return second.compareTo(first);
    }
  }

  /** Sorts by ascending docID */
  public static final class DocComparator extends FieldComparator<Integer> {
    private final int[] docIDs;
    private int docBase;
    private int bottom;

    DocComparator(int numHits) {
      docIDs = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      // No overflow risk because docIDs are non-negative
      return docIDs[slot1] - docIDs[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      // No overflow risk because docIDs are non-negative
      return bottom - (docBase + doc);
    }

    @Override
    public void copy(int slot, int doc) {
      docIDs[slot] = docBase + doc;
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) {
      // TODO: can we "map" our docIDs to the current
      // reader? saves having to then subtract on every
      // compare call
      this.docBase = context.docBase;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = docIDs[bottom];
    }

    @Override
    public Integer value(int slot) {
      return Integer.valueOf(docIDs[slot]);
    }
  }
  
  /** Sorts by field's natural Term sort order, using
   *  ordinals.  This is functionally equivalent to {@link
   *  TermValComparator}, but it first resolves the string
   *  to their relative ordinal positions (using the index
   *  returned by {@link FieldCache#getTermsIndex}), and
   *  does most comparisons using the ordinals.  For medium
   *  to large results, this comparator will be much faster
   *  than {@link TermValComparator}.  For very small
   *  result sets it may be slower. */
  public static final class TermOrdValComparator extends FieldComparator<BytesRef> {
    /** @lucene.internal */
    final int[] ords;
    /** @lucene.internal */
    final BytesRef[] values;
    /** @lucene.internal */
    final int[] readerGen;

    /** @lucene.internal */
    int currentReaderGen = -1;
    private DocTermsIndex termsIndex;
    private final String field;

    /** @lucene.internal */
    int bottomSlot = -1;
    /** @lucene.internal */
    int bottomOrd;
    /** @lucene.internal */
    boolean bottomSameReader;
    /** @lucene.internal */
    BytesRef bottomValue;
    /** @lucene.internal */
    final BytesRef tempBR = new BytesRef();

    public TermOrdValComparator(int numHits, String field, int sortPos, boolean reversed) {
      ords = new int[numHits];
      values = new BytesRef[numHits];
      readerGen = new int[numHits];
      this.field = field;
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
        return -1;
      } else if (val2 == null) {
        return 1;
      }
      return val1.compareTo(val2);
    }

    @Override
    public int compareBottom(int doc) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copy(int slot, int doc) {
      throw new UnsupportedOperationException();
    }

    /** Base class for specialized (per bit width of the
     * ords) per-segment comparator.  NOTE: this is messy;
     * we do this only because hotspot can't reliably inline
     * the underlying array access when looking up doc->ord
     * @lucene.internal
     */
    abstract class PerSegmentComparator extends FieldComparator<BytesRef> {
      
      @Override
      public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
        return TermOrdValComparator.this.setNextReader(context);
      }

      @Override
      public int compare(int slot1, int slot2) {
        return TermOrdValComparator.this.compare(slot1, slot2);
      }

      @Override
      public void setBottom(final int bottom) {
        TermOrdValComparator.this.setBottom(bottom);
      }

      @Override
      public BytesRef value(int slot) {
        return TermOrdValComparator.this.value(slot);
      }

      @Override
      public int compareValues(BytesRef val1, BytesRef val2) {
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
    }

    // Used per-segment when bit width of doc->ord is 8:
    private final class ByteOrdComparator extends PerSegmentComparator {
      private final byte[] readerOrds;
      private final DocTermsIndex termsIndex;
      private final int docBase;

      public ByteOrdComparator(byte[] readerOrds, DocTermsIndex termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - (readerOrds[doc]&0xFF);
        } else {
          // ord is only approx comparable: if they are not
          // equal, we can use that; if they are equal, we
          // must fallback to compare by value
          final int order = readerOrds[doc]&0xFF;
          final int cmp = bottomOrd - order;
          if (cmp != 0) {
            return cmp;
          }

          if (bottomValue == null) {
            if (order == 0) {
              // unset
              return 0;
            }
            // bottom wins
            return -1;
          } else if (order == 0) {
            // doc wins
            return 1;
          }
          termsIndex.lookup(order, tempBR);
          return bottomValue.compareTo(tempBR);
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = readerOrds[doc]&0xFF;
        ords[slot] = ord;
        if (ord == 0) {
          values[slot] = null;
        } else {
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
    private final class ShortOrdComparator extends PerSegmentComparator {
      private final short[] readerOrds;
      private final DocTermsIndex termsIndex;
      private final int docBase;

      public ShortOrdComparator(short[] readerOrds, DocTermsIndex termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - (readerOrds[doc]&0xFFFF);
        } else {
          // ord is only approx comparable: if they are not
          // equal, we can use that; if they are equal, we
          // must fallback to compare by value
          final int order = readerOrds[doc]&0xFFFF;
          final int cmp = bottomOrd - order;
          if (cmp != 0) {
            return cmp;
          }

          if (bottomValue == null) {
            if (order == 0) {
              // unset
              return 0;
            }
            // bottom wins
            return -1;
          } else if (order == 0) {
            // doc wins
            return 1;
          }
          termsIndex.lookup(order, tempBR);
          return bottomValue.compareTo(tempBR);
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = readerOrds[doc]&0xFFFF;
        ords[slot] = ord;
        if (ord == 0) {
          values[slot] = null;
        } else {
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
    private final class IntOrdComparator extends PerSegmentComparator {
      private final int[] readerOrds;
      private final DocTermsIndex termsIndex;
      private final int docBase;

      public IntOrdComparator(int[] readerOrds, DocTermsIndex termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - readerOrds[doc];
        } else {
          // ord is only approx comparable: if they are not
          // equal, we can use that; if they are equal, we
          // must fallback to compare by value
          final int order = readerOrds[doc];
          final int cmp = bottomOrd - order;
          if (cmp != 0) {
            return cmp;
          }

          if (bottomValue == null) {
            if (order == 0) {
              // unset
              return 0;
            }
            // bottom wins
            return -1;
          } else if (order == 0) {
            // doc wins
            return 1;
          }
          termsIndex.lookup(order, tempBR);
          return bottomValue.compareTo(tempBR);
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = readerOrds[doc];
        ords[slot] = ord;
        if (ord == 0) {
          values[slot] = null;
        } else {
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
    private final class AnyOrdComparator extends PerSegmentComparator {
      private final PackedInts.Reader readerOrds;
      private final DocTermsIndex termsIndex;
      private final int docBase;

      public AnyOrdComparator(PackedInts.Reader readerOrds, DocTermsIndex termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - (int) readerOrds.get(doc);
        } else {
          // ord is only approx comparable: if they are not
          // equal, we can use that; if they are equal, we
          // must fallback to compare by value
          final int order = (int) readerOrds.get(doc);
          final int cmp = bottomOrd - order;
          if (cmp != 0) {
            return cmp;
          }

          if (bottomValue == null) {
            if (order == 0) {
              // unset
              return 0;
            }
            // bottom wins
            return -1;
          } else if (order == 0) {
            // doc wins
            return 1;
          }
          termsIndex.lookup(order, tempBR);
          return bottomValue.compareTo(tempBR);
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = (int) readerOrds.get(doc);
        ords[slot] = ord;
        if (ord == 0) {
          values[slot] = null;
        } else {
          assert ord > 0;
          if (values[slot] == null) {
            values[slot] = new BytesRef();
          }
          termsIndex.lookup(ord, values[slot]);
        }
        readerGen[slot] = currentReaderGen;
      }
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      final int docBase = context.docBase;
      termsIndex = FieldCache.DEFAULT.getTermsIndex(context.reader, field);
      final PackedInts.Reader docToOrd = termsIndex.getDocToOrd();
      FieldComparator perSegComp;
      if (docToOrd instanceof Direct8) {
        perSegComp = new ByteOrdComparator(((Direct8) docToOrd).getArray(), termsIndex, docBase);
      } else if (docToOrd instanceof Direct16) {
        perSegComp = new ShortOrdComparator(((Direct16) docToOrd).getArray(), termsIndex, docBase);
      } else if (docToOrd instanceof Direct32) {
        perSegComp = new IntOrdComparator(((Direct32) docToOrd).getArray(), termsIndex, docBase);
      } else {
        perSegComp = new AnyOrdComparator(docToOrd, termsIndex, docBase);
      }

      currentReaderGen++;
      if (bottomSlot != -1) {
        perSegComp.setBottom(bottomSlot);
      }

      return perSegComp;
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
          assert ords[bottomSlot] == 0;
          bottomOrd = 0;
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
      return values[slot];
    }
  }

  /** Sorts by field's natural Term sort order.  All
   *  comparisons are done using BytesRef.compareTo, which is
   *  slow for medium to large result sets but possibly
   *  very fast for very small results sets. */
  public static final class TermValComparator extends FieldComparator<BytesRef> {

    private BytesRef[] values;
    private DocTerms docTerms;
    private final String field;
    private BytesRef bottom;
    private final BytesRef tempBR = new BytesRef();

    TermValComparator(int numHits, String field) {
      values = new BytesRef[numHits];
      this.field = field;
    }

    @Override
    public int compare(int slot1, int slot2) {
      final BytesRef val1 = values[slot1];
      final BytesRef val2 = values[slot2];
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

    @Override
    public int compareBottom(int doc) {
      BytesRef val2 = docTerms.getTerm(doc, tempBR);
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

    @Override
    public void copy(int slot, int doc) {
      if (values[slot] == null) {
        values[slot] = new BytesRef();
      }
      docTerms.getTerm(doc, values[slot]);
    }

    @Override
    public FieldComparator setNextReader(AtomicReaderContext context) throws IOException {
      docTerms = FieldCache.DEFAULT.getTerms(context.reader, field);
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public BytesRef value(int slot) {
      return values[slot];
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
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
  }

  final protected static int binarySearch(BytesRef br, DocTermsIndex a, BytesRef key) {
    return binarySearch(br, a, key, 1, a.numOrd()-1);
  }

  final protected static int binarySearch(BytesRef br, DocTermsIndex a, BytesRef key, int low, int high) {

    while (low <= high) {
      int mid = (low + high) >>> 1;
      BytesRef midVal = a.lookup(mid, br);
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
}
