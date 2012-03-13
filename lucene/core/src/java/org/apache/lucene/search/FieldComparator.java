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
import java.util.Comparator;

import org.apache.lucene.index.AtomicReader; // javadocs
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.FieldCache.ByteParser;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.search.FieldCache.DocTermsIndex;
import org.apache.lucene.search.FieldCache.DoubleParser;
import org.apache.lucene.search.FieldCache.FloatParser;
import org.apache.lucene.search.FieldCache.IntParser;
import org.apache.lucene.search.FieldCache.LongParser;
import org.apache.lucene.search.FieldCache.ShortParser;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
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
 *  <li> {@link #setNextReader(AtomicReaderContext)} Invoked
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
  public abstract FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException;

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
    if (first == null) {
      if (second == null) {
        return 0;
      } else {
        return -1;
      }
    } else if (second == null) {
      return 1;
    } else {
      return ((Comparable<T>) first).compareTo(second);
    }
  }

  public static abstract class NumericComparator<T extends Number> extends FieldComparator<T> {
    protected final T missingValue;
    protected final String field;
    protected Bits docsWithField;
    
    public NumericComparator(String field, T missingValue) {
      this.field = field;
      this.missingValue = missingValue;
    }

    @Override
    public FieldComparator<T> setNextReader(AtomicReaderContext context) throws IOException {
      if (missingValue != null) {
        docsWithField = FieldCache.DEFAULT.getDocsWithField(context.reader(), field);
        // optimization to remove unneeded checks on the bit interface:
        if (docsWithField instanceof Bits.MatchAllBits) {
          docsWithField = null;
        }
      } else {
        docsWithField = null;
      }
      return this;
    }
  }

  /** Parses field's values as byte (using {@link
   *  FieldCache#getBytes} and sorts by ascending value */
  public static final class ByteComparator extends NumericComparator<Byte> {
    private final byte[] values;
    private final ByteParser parser;
    private byte[] currentReaderValues;
    private byte bottom;

    ByteComparator(int numHits, String field, FieldCache.Parser parser, Byte missingValue) {
      super(field, missingValue);
      values = new byte[numHits];
      this.parser = (ByteParser) parser;
    }

    @Override
    public int compare(int slot1, int slot2) {
      return values[slot1] - values[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      byte v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return bottom - v2;
    }

    @Override
    public void copy(int slot, int doc) {
      byte v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }
      values[slot] = v2;
    }

    @Override
    public FieldComparator<Byte> setNextReader(AtomicReaderContext context) throws IOException {
      // NOTE: must do this before calling super otherwise
      // we compute the docsWithField Bits twice!
      currentReaderValues = FieldCache.DEFAULT.getBytes(context.reader(), field, parser, missingValue != null);
      return super.setNextReader(context);
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
  public static final class DoubleComparator extends NumericComparator<Double> {
    private final double[] values;
    private final DoubleParser parser;
    private double[] currentReaderValues;
    private double bottom;

    DoubleComparator(int numHits, String field, FieldCache.Parser parser, Double missingValue) {
      super(field, missingValue);
      values = new double[numHits];
      this.parser = (DoubleParser) parser;
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
      double v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

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
      double v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Double> setNextReader(AtomicReaderContext context) throws IOException {
      // NOTE: must do this before calling super otherwise
      // we compute the docsWithField Bits twice!
      currentReaderValues = FieldCache.DEFAULT.getDoubles(context.reader(), field, parser, missingValue != null);
      return super.setNextReader(context);
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
    private final String field;
    private DocValues.Source currentReaderValues;
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
    public FieldComparator<Double> setNextReader(AtomicReaderContext context) throws IOException {
      final DocValues docValues = context.reader().docValues(field);
      if (docValues != null) {
        currentReaderValues = docValues.getSource(); 
      } else {
        currentReaderValues = DocValues.getDefaultSource(DocValues.Type.FLOAT_64);
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
  public static final class FloatComparator extends NumericComparator<Float> {
    private final float[] values;
    private final FloatParser parser;
    private float[] currentReaderValues;
    private float bottom;

    FloatComparator(int numHits, String field, FieldCache.Parser parser, Float missingValue) {
      super(field, missingValue);
      values = new float[numHits];
      this.parser = (FloatParser) parser;
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
      float v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }
      
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
      float v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Float> setNextReader(AtomicReaderContext context) throws IOException {
      // NOTE: must do this before calling super otherwise
      // we compute the docsWithField Bits twice!
      currentReaderValues = FieldCache.DEFAULT.getFloats(context.reader(), field, parser, missingValue != null);
      return super.setNextReader(context);
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
  public static final class ShortComparator extends NumericComparator<Short> {
    private final short[] values;
    private final ShortParser parser;
    private short[] currentReaderValues;
    private short bottom;

    ShortComparator(int numHits, String field, FieldCache.Parser parser, Short missingValue) {
      super(field, missingValue);
      values = new short[numHits];
      this.parser = (ShortParser) parser;
    }

    @Override
    public int compare(int slot1, int slot2) {
      return values[slot1] - values[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      short v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return bottom - v2;
    }

    @Override
    public void copy(int slot, int doc) {
      short v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Short> setNextReader(AtomicReaderContext context) throws IOException {
      // NOTE: must do this before calling super otherwise
      // we compute the docsWithField Bits twice!
      currentReaderValues = FieldCache.DEFAULT.getShorts(context.reader(), field, parser, missingValue != null);
      return super.setNextReader(context);
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
  public static final class IntComparator extends NumericComparator<Integer> {
    private final int[] values;
    private final IntParser parser;
    private int[] currentReaderValues;
    private int bottom;                           // Value of bottom of queue

    IntComparator(int numHits, String field, FieldCache.Parser parser, Integer missingValue) {
      super(field, missingValue);
      values = new int[numHits];
      this.parser = (IntParser) parser;
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
      int v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

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
      int v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Integer> setNextReader(AtomicReaderContext context) throws IOException {
      // NOTE: must do this before calling super otherwise
      // we compute the docsWithField Bits twice!
      currentReaderValues = FieldCache.DEFAULT.getInts(context.reader(), field, parser, missingValue != null);
      return super.setNextReader(context);
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
    private DocValues.Source currentReaderValues;
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
    public FieldComparator<Long> setNextReader(AtomicReaderContext context) throws IOException {
      DocValues docValues = context.reader().docValues(field);
      if (docValues != null) {
        currentReaderValues = docValues.getSource();
      } else {
        currentReaderValues = DocValues.getDefaultSource(DocValues.Type.FIXED_INTS_64);
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
  public static final class LongComparator extends NumericComparator<Long> {
    private final long[] values;
    private final LongParser parser;
    private long[] currentReaderValues;
    private long bottom;

    LongComparator(int numHits, String field, FieldCache.Parser parser, Long missingValue) {
      super(field, missingValue);
      values = new long[numHits];
      this.parser = (LongParser) parser;
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
      long v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

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
      long v2 = currentReaderValues[doc];
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }

    @Override
    public FieldComparator<Long> setNextReader(AtomicReaderContext context) throws IOException {
      // NOTE: must do this before calling super otherwise
      // we compute the docsWithField Bits twice!
      currentReaderValues = FieldCache.DEFAULT.getLongs(context.reader(), field, parser, missingValue != null);
      return super.setNextReader(context);
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
    public FieldComparator<Float> setNextReader(AtomicReaderContext context) {
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
    public FieldComparator<Integer> setNextReader(AtomicReaderContext context) {
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
    /* Ords for each slot.
       @lucene.internal */
    final int[] ords;

    /* Values for each slot.
       @lucene.internal */
    final BytesRef[] values;

    /* Which reader last copied a value into the slot. When
       we compare two slots, we just compare-by-ord if the
       readerGen is the same; else we must compare the
       values (slower).
       @lucene.internal */
    final int[] readerGen;

    /* Gen of current reader we are on.
       @lucene.internal */
    int currentReaderGen = -1;

    /* Current reader's doc ord/values.
       @lucene.internal */
    DocTermsIndex termsIndex;

    private final String field;

    /* Bottom slot, or -1 if queue isn't full yet
       @lucene.internal */
    int bottomSlot = -1;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot
       is set).  Cached for faster compares.
       @lucene.internal */
    int bottomOrd;

    /* True if current bottom slot matches the current
       reader.
       @lucene.internal */
    boolean bottomSameReader;

    /* Bottom value (same as values[bottomSlot] once
       bottomSlot is set).  Cached for faster compares.
      @lucene.internal */
    BytesRef bottomValue;

    final BytesRef tempBR = new BytesRef();

    public TermOrdValComparator(int numHits, String field) {
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
      public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
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
        final int docOrd = (readerOrds[doc]&0xFF);
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
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
        final int docOrd = (readerOrds[doc]&0xFFFF);
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
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
        final int docOrd = readerOrds[doc];
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
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
        final int docOrd = (int) readerOrds.get(doc);
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
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
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
      final int docBase = context.docBase;
      termsIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), field);
      final PackedInts.Reader docToOrd = termsIndex.getDocToOrd();
      FieldComparator<BytesRef> perSegComp = null;
      if (docToOrd.hasArray()) {
        final Object arr = docToOrd.getArray();
        if (arr instanceof byte[]) {
          perSegComp = new ByteOrdComparator((byte[]) arr, termsIndex, docBase);
        } else if (arr instanceof short[]) {
          perSegComp = new ShortOrdComparator((short[]) arr, termsIndex, docBase);
        } else if (arr instanceof int[]) {
          perSegComp = new IntOrdComparator((int[]) arr, termsIndex, docBase);
        }
        // Don't specialize the long[] case since it's not
        // possible, ie, worse case is MAX_INT-1 docs with
        // every one having a unique value.
      }
      if (perSegComp == null) {
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

  /** Sorts by field's natural Term sort order, using
   *  ordinals; this is just like {@link
   *  TermOrdValComparator} except it uses DocValues to
   *  retrieve the sort ords saved during indexing. */
  public static final class TermOrdValDocValuesComparator extends FieldComparator<BytesRef> {
    /* Ords for each slot.
       @lucene.internal */
    final int[] ords;

    /* Values for each slot.
       @lucene.internal */
    final BytesRef[] values;

    /* Which reader last copied a value into the slot. When
       we compare two slots, we just compare-by-ord if the
       readerGen is the same; else we must compare the
       values (slower).
       @lucene.internal */
    final int[] readerGen;

    /* Gen of current reader we are on.
       @lucene.internal */
    int currentReaderGen = -1;

    /* Current reader's doc ord/values.
       @lucene.internal */
    DocValues.SortedSource termsIndex;

    /* Comparator for comparing by value.
       @lucene.internal */
    Comparator<BytesRef> comp;

    private final String field;

    /* Bottom slot, or -1 if queue isn't full yet
       @lucene.internal */
    int bottomSlot = -1;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot
       is set).  Cached for faster compares.
       @lucene.internal */
    int bottomOrd;

    /* True if current bottom slot matches the current
       reader.
       @lucene.internal */
    boolean bottomSameReader;

    /* Bottom value (same as values[bottomSlot] once
       bottomSlot is set).  Cached for faster compares.
      @lucene.internal */
    BytesRef bottomValue;

    /** @lucene.internal */
    final BytesRef tempBR = new BytesRef();

    public TermOrdValDocValuesComparator(int numHits, String field) {
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
      return comp.compare(val1, val2);
    }

    @Override
    public int compareBottom(int doc) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void copy(int slot, int doc) {
      throw new UnsupportedOperationException();
    }

    // TODO: would be nice to share these specialized impls
    // w/ TermOrdValComparator

    /** Base class for specialized (per bit width of the
     * ords) per-segment comparator.  NOTE: this is messy;
     * we do this only because hotspot can't reliably inline
     * the underlying array access when looking up doc->ord
     * @lucene.internal
     */
    abstract class PerSegmentComparator extends FieldComparator<BytesRef> {
      
      @Override
      public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        return TermOrdValDocValuesComparator.this.setNextReader(context);
      }

      @Override
      public int compare(int slot1, int slot2) {
        return TermOrdValDocValuesComparator.this.compare(slot1, slot2);
      }

      @Override
      public void setBottom(final int bottom) {
        TermOrdValDocValuesComparator.this.setBottom(bottom);
      }

      @Override
      public BytesRef value(int slot) {
        return TermOrdValDocValuesComparator.this.value(slot);
      }

      @Override
      public int compareValues(BytesRef val1, BytesRef val2) {
        assert val1 != null;
        assert val2 != null;
        return comp.compare(val1, val2);
      }
    }

    // Used per-segment when bit width of doc->ord is 8:
    private final class ByteOrdComparator extends PerSegmentComparator {
      private final byte[] readerOrds;
      private final DocValues.SortedSource termsIndex;
      private final int docBase;

      public ByteOrdComparator(byte[] readerOrds, DocValues.SortedSource termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        final int docOrd = readerOrds[doc]&0xFF;
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = readerOrds[doc]&0xFF;
        ords[slot] = ord;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.getByOrd(ord, values[slot]);
        readerGen[slot] = currentReaderGen;
      }
    }

    // Used per-segment when bit width of doc->ord is 16:
    private final class ShortOrdComparator extends PerSegmentComparator {
      private final short[] readerOrds;
      private final DocValues.SortedSource termsIndex;
      private final int docBase;

      public ShortOrdComparator(short[] readerOrds, DocValues.SortedSource termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        final int docOrd = readerOrds[doc]&0xFFFF;
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = readerOrds[doc]&0xFFFF;
        ords[slot] = ord;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.getByOrd(ord, values[slot]);
        readerGen[slot] = currentReaderGen;
      }
    }

    // Used per-segment when bit width of doc->ord is 32:
    private final class IntOrdComparator extends PerSegmentComparator {
      private final int[] readerOrds;
      private final DocValues.SortedSource termsIndex;
      private final int docBase;

      public IntOrdComparator(int[] readerOrds, DocValues.SortedSource termsIndex, int docBase) {
        this.readerOrds = readerOrds;
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        final int docOrd = readerOrds[doc];
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = readerOrds[doc];
        ords[slot] = ord;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.getByOrd(ord, values[slot]);
        readerGen[slot] = currentReaderGen;
      }
    }

    // Used per-segment when bit width is not a native array
    // size (8, 16, 32):
    private final class AnyPackedDocToOrdComparator extends PerSegmentComparator {
      private final PackedInts.Reader readerOrds;
      private final int docBase;

      public AnyPackedDocToOrdComparator(PackedInts.Reader readerOrds, int docBase) {
        this.readerOrds = readerOrds;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        final int docOrd = (int) readerOrds.get(doc);
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = (int) readerOrds.get(doc);
        ords[slot] = ord;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.getByOrd(ord, values[slot]);
        readerGen[slot] = currentReaderGen;
      }
    }

    // Used per-segment when DV doesn't use packed ints for
    // docToOrds:
    private final class AnyOrdComparator extends PerSegmentComparator {
      private final int docBase;

      public AnyOrdComparator(int docBase) {
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        final int docOrd = termsIndex.ord(doc);
        if (bottomSameReader) {
          // ord is precisely comparable, even in the equal case
          return bottomOrd - docOrd;
        } else if (bottomOrd >= docOrd) {
          // the equals case always means bottom is > doc
          // (because we set bottomOrd to the lower bound in
          // setBottom):
          return 1;
        } else {
          return -1;
        }
      }

      @Override
      public void copy(int slot, int doc) {
        final int ord = termsIndex.ord(doc);
        ords[slot] = ord;
        if (values[slot] == null) {
          values[slot] = new BytesRef();
        }
        termsIndex.getByOrd(ord, values[slot]);
        readerGen[slot] = currentReaderGen;
      }
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
      final int docBase = context.docBase;

      final DocValues dv = context.reader().docValues(field);
      if (dv == null) {
        // This may mean entire segment had no docs with
        // this DV field; use default field value (empty
        // byte[]) in this case:
        termsIndex = DocValues.getDefaultSortedSource(DocValues.Type.BYTES_VAR_SORTED, context.reader().maxDoc());
      } else {
        termsIndex = dv.getSource().asSortedSource();
        if (termsIndex == null) {
          // This means segment has doc values, but they are
          // not able to provide a sorted source; consider
          // this a hard error:
          throw new IllegalStateException("DocValues exist for field \"" + field + "\", but not as a sorted source: type=" + dv.getSource().getType() + " reader=" + context.reader());
        }
      }

      comp = termsIndex.getComparator();

      FieldComparator<BytesRef> perSegComp = null;
      if (termsIndex.hasPackedDocToOrd()) {
        final PackedInts.Reader docToOrd = termsIndex.getDocToOrd();
        if (docToOrd.hasArray()) {
          final Object arr = docToOrd.getArray();
          assert arr != null;
          if (arr instanceof byte[]) {
            // 8 bit packed
            perSegComp = new ByteOrdComparator((byte[]) arr, termsIndex, docBase);
          } else if (arr instanceof short[]) {
            // 16 bit packed
            perSegComp = new ShortOrdComparator((short[]) arr, termsIndex, docBase);
          } else if (arr instanceof int[]) {
            // 32 bit packed
            perSegComp = new IntOrdComparator((int[]) arr, termsIndex, docBase);
          }
        }

        if (perSegComp == null) {
          perSegComp = new AnyPackedDocToOrdComparator(docToOrd, docBase);
        }
      } else {
        if (perSegComp == null) {
          perSegComp = new AnyOrdComparator(docBase);
        }
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
          final int index = termsIndex.getOrdByValue(bottomValue, tempBR);
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
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
      docTerms = FieldCache.DEFAULT.getTerms(context.reader(), field);
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

  /** Sorts by field's natural Term sort order.  All
   *  comparisons are done using BytesRef.compareTo, which is
   *  slow for medium to large result sets but possibly
   *  very fast for very small results sets.  The BytesRef
   *  values are obtained using {@link AtomicReader#docValues}. */
  public static final class TermValDocValuesComparator extends FieldComparator<BytesRef> {

    private BytesRef[] values;
    private DocValues.Source docTerms;
    private final String field;
    private BytesRef bottom;
    private final BytesRef tempBR = new BytesRef();

    TermValDocValuesComparator(int numHits, String field) {
      values = new BytesRef[numHits];
      this.field = field;
    }

    @Override
    public int compare(int slot1, int slot2) {
      assert values[slot1] != null;
      assert values[slot2] != null;
      return values[slot1].compareTo(values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      assert bottom != null;
      return bottom.compareTo(docTerms.getBytes(doc, tempBR));
    }

    @Override
    public void copy(int slot, int doc) {
      if (values[slot] == null) {
        values[slot] = new BytesRef();
      }
      docTerms.getBytes(doc, values[slot]);
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
      final DocValues dv = context.reader().docValues(field);
      if (dv != null) {
        docTerms = dv.getSource();
      } else {
        docTerms = DocValues.getDefaultSource(DocValues.Type.BYTES_VAR_DEREF);
      }
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
      assert val1 != null;
      assert val2 != null;
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
