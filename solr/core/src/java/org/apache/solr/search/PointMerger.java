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

package org.apache.solr.search;


import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueDouble;
import org.apache.lucene.util.mutable.MutableValueFloat;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.schema.SchemaField;

/**
 * Merge multiple numeric point fields (segments) together.
 *
 * @lucene.internal
 * @lucene.experimental
 */
public class PointMerger {
  public static int TOTAL_BUFFER_SIZE = 1000000;  // target number of elements to cache across all segments
  public static int MIN_SEG_BUFFER_SIZE = 100;    // minimum buffer size on any segment (to limit unnecessary exception throws)

  public static class ValueIterator {
    PQueue queue;
    MutableValue topVal;

    public ValueIterator(SchemaField field, List<LeafReaderContext> readers) throws IOException {
      this(field, readers, TOTAL_BUFFER_SIZE, MIN_SEG_BUFFER_SIZE);
    }

    public ValueIterator(SchemaField field, List<LeafReaderContext> readers, int totalBufferSize, int minSegBufferSize) throws IOException {
      assert field.getType().isPointField();
      queue = new PQueue(readers.size());
      if (readers.isEmpty()) {
        return;
      }
      long ndocs = readers.get(readers.size()-1).docBase + readers.get(readers.size()-1).reader().maxDoc();
      for (LeafReaderContext ctx : readers) {
        PointValues pv = ctx.reader().getPointValues(field.getName());
        if (pv == null) continue;
        BaseSeg seg = null;
        // int capacity = 2;
        int capacity = (int)((long)totalBufferSize * ctx.reader().maxDoc() / ndocs);
        capacity = Math.max(capacity, minSegBufferSize);

        switch (field.getType().getNumberType()) {
          case INTEGER:
            seg = new IntSeg(pv, capacity);
            break;
          case LONG:
            seg = new LongSeg(pv, capacity);
            break;
          case FLOAT:
            seg = new FloatSeg(pv, capacity);
            break;
          case DOUBLE:
            seg = new DoubleSeg(pv, capacity);
            break;
          case DATE:
            seg = new DateSeg(pv, capacity);
            break;
        }
        int count = seg.setNextValue();
        if (count >= 0) {
          queue.add(seg);
        }
      }
      if (queue.size() > 0) topVal = queue.top().getMutableValue().duplicate();
    }

    // gets the mutable value that is updated after every call to getNextCount().
    // getMutableValue only needs to be called a single time since the instance is reused for every call to getNextCount().
    public MutableValue getMutableValue() {
      return topVal;
    }

    public long getNextCount() throws IOException {
      if (queue.size() == 0) return -1;

      BaseSeg seg = queue.top();
      topVal.copy(seg.getMutableValue());
      long count = 0;

      do {
        count += seg.getCurrentCount();
        int nextCount = seg.setNextValue();
        if (nextCount < 0) {
          queue.pop();
          if (queue.size() == 0) break;
        } else {
          queue.updateTop();
        }
        seg = queue.top();
      } while (seg.getMutableValue().equalsSameType(topVal));

      return count;
    }

  }

  static class PQueue extends PriorityQueue<BaseSeg> {
    public PQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(BaseSeg a, BaseSeg b) {
      return BaseSeg.lessThan(a,b);
    }
  }



  abstract static class BaseSeg implements PointValues.IntersectVisitor {
    final PointValues points;
    final int[] count;
    int pos = -1;  // index of the last valid entry
    int readPos = -1;  // last position read from

    MutableValue currentValue;  // subclass constructor will fill this in
    int currentCount;

    BaseSeg(PointValues points, int capacity) {
      this.points = points;
      this.count = new int[capacity];
    }

    public static boolean lessThan(BaseSeg a, BaseSeg b) {
      return a.currentValue.compareTo(b.currentValue) < 0;
    }

    public MutableValue getMutableValue() {
      return currentValue;
    }

    // returns -1 count if there are no more values
    public int getCurrentCount() {
      return currentCount;
    }

    // sets the next value and returns getCurrentCount()
    public int setNextValue() throws IOException {
      return 0;
    };


    void refill() throws IOException {
      assert readPos >= pos;
      readPos = -1;
      pos = -1;
      try {
        points.intersect(this);
      } catch (BreakException e) {
        // nothing to do
      }
    }

    @Override
    public void visit(int docID) throws IOException {
      throw new UnsupportedOperationException();
    }

  }


  static class IntSeg extends BaseSeg  {
    final int[] values;
    int last = Integer.MIN_VALUE;
    final MutableValueInt mval;

    IntSeg(PointValues points, int capacity) {
      super(points, capacity);
      this.values = new int[capacity];
      this.currentValue = this.mval = new MutableValueInt();
    }

    public int setNextValue() throws IOException {
      if (readPos >= pos) {
        if (last != Integer.MAX_VALUE) {
          ++last;
          refill();
        }
        if (readPos >= pos) {
          last = Integer.MAX_VALUE;
          currentCount = -1;
          return -1;
        }
      }

      ++readPos;
      mval.value = values[readPos];
      currentCount = count[readPos];
      return currentCount;
    }


    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      // TODO: handle filter or deleted documents?
      int v = IntPoint.decodeDimension(packedValue, 0);
      if (v < last) return;

      if (v == last && pos >= 0) {
        count[pos]++;
      } else {
        if (pos+1 < values.length) {
          last = v;
          ++pos;
          values[pos] = v;
          count[pos] = 1;
        } else {
          // a new value we don't have room for
          throw breakException;
        }
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      int v = IntPoint.decodeDimension(maxPackedValue, 0);
      if (v >= last) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      } else {
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
      }
    }
  }

  static class LongSeg extends BaseSeg  {
    final long[] values;
    long last = Long.MIN_VALUE;
    MutableValueLong mval;

    LongSeg(PointValues points, int capacity) {
      super(points, capacity);
      this.values = new long[capacity];
      this.currentValue = this.mval = new MutableValueLong();
    }

    public int setNextValue() throws IOException {
      if (readPos >= pos) {
        if (last != Long.MAX_VALUE) {
          ++last;
          refill();
        }
        if (readPos >= pos) {
          last = Long.MAX_VALUE;
          currentCount = -1;
          return -1;
        }
      }

      ++readPos;
      mval.value = values[readPos];
      currentCount = count[readPos];
      return currentCount;
    }


    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      // TODO: handle filter or deleted documents?
      long v = LongPoint.decodeDimension(packedValue, 0);
      if (v < last) return;

      if (v == last && pos >= 0) {
        count[pos]++;
      } else {
        if (pos+1 < values.length) {
          last = v;
          ++pos;
          values[pos] = v;
          count[pos] = 1;
        } else {
          // a new value we don't have room for
          throw breakException;
        }
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      long v = LongPoint.decodeDimension(maxPackedValue, 0);
      if (v >= last) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      } else {
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
      }
    }
  }

  static class FloatSeg extends BaseSeg  {
    final float[] values;
    float last = -Float.MAX_VALUE;
    final MutableValueFloat mval;

    FloatSeg(PointValues points, int capacity) {
      super(points, capacity);
      this.values = new float[capacity];
      this.currentValue = this.mval = new MutableValueFloat();
    }

    public int setNextValue() throws IOException {
      if (readPos >= pos) {
        if (last != Float.MAX_VALUE) {
          last = Math.nextUp(last);
          refill();
        }
        if (readPos >= pos) {
          last = Float.MAX_VALUE;
          currentCount = -1;
          return -1;
        }
      }

      ++readPos;
      mval.value = values[readPos];
      currentCount = count[readPos];
      return currentCount;
    }


    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      // TODO: handle filter or deleted documents?
      float v = FloatPoint.decodeDimension(packedValue, 0);
      if (v < last) return;

      if (v == last && pos >= 0) {
        count[pos]++;
      } else {
        if (pos+1 < values.length) {
          last = v;
          ++pos;
          values[pos] = v;
          count[pos] = 1;
        } else {
          // a new value we don't have room for
          throw breakException;
        }
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      float v = FloatPoint.decodeDimension(maxPackedValue, 0);
      if (v >= last) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      } else {
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
      }
    }
  }

  static class DoubleSeg extends BaseSeg  {
    final double[] values;
    double last = -Double.MAX_VALUE;
    final MutableValueDouble mval;

    DoubleSeg(PointValues points, int capacity) {
      super(points, capacity);
      this.values = new double[capacity];
      this.currentValue = this.mval = new MutableValueDouble();
    }

    public int setNextValue() throws IOException {
      if (readPos >= pos) {
        if (last != Double.MAX_VALUE) {
          last = Math.nextUp(last);
          refill();
        }
        if (readPos >= pos) {
          last = Double.MAX_VALUE;
          currentCount = -1;
          return -1;
        }
      }

      ++readPos;
      mval.value = values[readPos];
      currentCount = count[readPos];
      return currentCount;
    }


    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      // TODO: handle filter or deleted documents?
      double v = DoublePoint.decodeDimension(packedValue, 0);
      if (v < last) return;

      if (v == last && pos >= 0) {
        count[pos]++;
      } else {
        if (pos+1 < values.length) {
          last = v;
          ++pos;
          values[pos] = v;
          count[pos] = 1;
        } else {
          // a new value we don't have room for
          throw breakException;
        }
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      double v = DoublePoint.decodeDimension(maxPackedValue, 0);
      if (v >= last) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      } else {
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
      }
    }
  }

  static class DateSeg extends LongSeg {
    DateSeg(PointValues points, int capacity) {
      super(points, capacity);
      this.currentValue = this.mval = new MutableValueDate();
    }
  }

  static class BreakException extends RuntimeException {
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }

  static BreakException breakException = new BreakException();

}
