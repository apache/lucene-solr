package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.FieldCache.ByteParser;
import org.apache.lucene.search.FieldCache.DoubleParser;
import org.apache.lucene.search.FieldCache.FloatParser;
import org.apache.lucene.search.FieldCache.IntParser;
import org.apache.lucene.search.FieldCache.LongParser;
import org.apache.lucene.search.FieldCache.ShortParser;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

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
   * @throws IOException if there is a low-level IO error
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

  /** Returns negative result if the doc's value is less
   *  than the provided value. */
  public abstract int compareDocToValue(int doc, T value) throws IOException;

  /**
   * Base FieldComparator class for numeric types
   */
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
    private FieldCache.Bytes currentReaderValues;
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
      byte v2 = currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return bottom - v2;
    }

    @Override
    public void copy(int slot, int doc) {
      byte v2 = currentReaderValues.get(doc);
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

    @Override
    public int compareDocToValue(int doc, Byte value) {
      byte docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return docValue - value.byteValue();
    }
  }

  /** Parses field's values as double (using {@link
   *  FieldCache#getDoubles} and sorts by ascending value */
  public static final class DoubleComparator extends NumericComparator<Double> {
    private final double[] values;
    private final DoubleParser parser;
    private FieldCache.Doubles currentReaderValues;
    private double bottom;

    DoubleComparator(int numHits, String field, FieldCache.Parser parser, Double missingValue) {
      super(field, missingValue);
      values = new double[numHits];
      this.parser = (DoubleParser) parser;
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Double.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      double v2 = currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return Double.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      double v2 = currentReaderValues.get(doc);
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

    @Override
    public int compareDocToValue(int doc, Double valueObj) {
      final double value = valueObj.doubleValue();
      double docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return Double.compare(docValue, value);
    }
  }

  /** Parses field's values as float (using {@link
   *  FieldCache#getFloats} and sorts by ascending value */
  public static final class FloatComparator extends NumericComparator<Float> {
    private final float[] values;
    private final FloatParser parser;
    private FieldCache.Floats currentReaderValues;
    private float bottom;

    FloatComparator(int numHits, String field, FieldCache.Parser parser, Float missingValue) {
      super(field, missingValue);
      values = new float[numHits];
      this.parser = (FloatParser) parser;
    }
    
    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      // TODO: are there sneaky non-branch ways to compute sign of float?
      float v2 = currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return Float.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      float v2 = currentReaderValues.get(doc);
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

    @Override
    public int compareDocToValue(int doc, Float valueObj) {
      final float value = valueObj.floatValue();
      float docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return Float.compare(docValue, value);
    }
  }

  /** Parses field's values as short (using {@link
   *  FieldCache#getShorts} and sorts by ascending value */
  public static final class ShortComparator extends NumericComparator<Short> {
    private final short[] values;
    private final ShortParser parser;
    private FieldCache.Shorts currentReaderValues;
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
      short v2 = currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return bottom - v2;
    }

    @Override
    public void copy(int slot, int doc) {
      short v2 = currentReaderValues.get(doc);
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

    @Override
    public int compareDocToValue(int doc, Short valueObj) {
      final short value = valueObj.shortValue();
      short docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return docValue - value;
    }
  }

  /** Parses field's values as int (using {@link
   *  FieldCache#getInts} and sorts by ascending value */
  public static final class IntComparator extends NumericComparator<Integer> {
    private final int[] values;
    private final IntParser parser;
    private FieldCache.Ints currentReaderValues;
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
      int v2 = currentReaderValues.get(doc);
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
      int v2 = currentReaderValues.get(doc);
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

    @Override
    public int compareDocToValue(int doc, Integer valueObj) {
      final int value = valueObj.intValue();
      int docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      if (docValue < value) {
        return -1;
      } else if (docValue > value) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  /** Parses field's values as long (using {@link
   *  FieldCache#getLongs} and sorts by ascending value */
  public static final class LongComparator extends NumericComparator<Long> {
    private final long[] values;
    private final LongParser parser;
    private FieldCache.Longs currentReaderValues;
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
      long v2 = currentReaderValues.get(doc);
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
      long v2 = currentReaderValues.get(doc);
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

    @Override
    public int compareDocToValue(int doc, Long valueObj) {
      final long value = valueObj.longValue();
      long docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      if (docValue < value) {
        return -1;
      } else if (docValue > value) {
        return 1;
      } else {
        return 0;
      }
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
      return Float.compare(scores[slot2], scores[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      float score = scorer.score();
      assert !Float.isNaN(score);
      return Float.compare(score, bottom);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      scores[slot] = scorer.score();
      assert !Float.isNaN(scores[slot]);
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

    @Override
    public int compareDocToValue(int doc, Float valueObj) throws IOException {
      final float value = valueObj.floatValue();
      float docValue = scorer.score();
      assert !Float.isNaN(docValue);
      return Float.compare(value, docValue);
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

    @Override
    public int compareDocToValue(int doc, Integer valueObj) {
      final int value = valueObj.intValue();
      int docValue = docBase + doc;
      if (docValue < value) {
        return -1;
      } else if (docValue > value) {
        return 1;
      } else {
        return 0;
      }
    }
  }
  
  /** Sorts by field's natural Term sort order, using
   *  ordinals.  This is functionally equivalent to {@link
   *  org.apache.lucene.search.FieldComparator.TermValComparator}, but it first resolves the string
   *  to their relative ordinal positions (using the index
   *  returned by {@link FieldCache#getTermsIndex}), and
   *  does most comparisons using the ordinals.  For medium
   *  to large results, this comparator will be much faster
   *  than {@link org.apache.lucene.search.FieldComparator.TermValComparator}.  For very small
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
    SortedDocValues termsIndex;

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

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
      int ord = termsIndex.getOrd(doc);
      if (ord == -1) {
        if (value == null) {
          return 0;
        }
        return -1;
      } else if (value == null) {
        return 1;
      }
      termsIndex.lookupOrd(ord, tempBR);
      return tempBR.compareTo(value);
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

      @Override
      public int compareDocToValue(int doc, BytesRef value) {
        return TermOrdValComparator.this.compareDocToValue(doc, value);
      }
    }

    // Used per-segment when docToOrd is null:
    private final class AnyOrdComparator extends PerSegmentComparator {
      private final SortedDocValues termsIndex;
      private final int docBase;

      public AnyOrdComparator(SortedDocValues termsIndex, int docBase) {
        this.termsIndex = termsIndex;
        this.docBase = docBase;
      }

      @Override
      public int compareBottom(int doc) {
        assert bottomSlot != -1;
        final int docOrd = termsIndex.getOrd(doc);
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
        final int ord = termsIndex.getOrd(doc);
        ords[slot] = ord;
        if (ord == -1) {
          values[slot] = null;
        } else {
          assert ord >= 0;
          if (values[slot] == null) {
            values[slot] = new BytesRef();
          }
          termsIndex.lookupOrd(ord, values[slot]);
        }
        readerGen[slot] = currentReaderGen;
      }
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
      final int docBase = context.docBase;
      termsIndex = FieldCache.DEFAULT.getTermsIndex(context.reader(), field);
      FieldComparator<BytesRef> perSegComp = new AnyOrdComparator(termsIndex, docBase);
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
          // -1 ord is null for all segments
          assert ords[bottomSlot] == -1;
          bottomOrd = -1;
          bottomSameReader = true;
          readerGen[bottomSlot] = currentReaderGen;
        } else {
          final int index = termsIndex.lookupTerm(bottomValue);
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
    private BinaryDocValues docTerms;
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
      docTerms.get(doc, tempBR);
      if (bottom.bytes == BinaryDocValues.MISSING) {
        if (tempBR.bytes == BinaryDocValues.MISSING) {
          return 0;
        }
        return -1;
      } else if (tempBR.bytes == BinaryDocValues.MISSING) {
        return 1;
      }
      return bottom.compareTo(tempBR);
    }

    @Override
    public void copy(int slot, int doc) {
      if (values[slot] == null) {
        values[slot] = new BytesRef();
      }
      docTerms.get(doc, values[slot]);
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

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
      docTerms.get(doc, tempBR);
      return tempBR.compareTo(value);
    }
  }
}
