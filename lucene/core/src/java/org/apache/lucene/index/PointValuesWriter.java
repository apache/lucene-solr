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
package org.apache.lucene.index;

import java.io.IOException;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PagedBytes;

/** Buffers up pending byte[][] value(s) per doc, then flushes when segment flushes. */
class PointValuesWriter {
  private final FieldInfo fieldInfo;
  private final PagedBytes bytes;
  private final DataOutput bytesOut;
  private final Counter iwBytesUsed;
  private int[] docIDs;
  private int numPoints;
  private int numDocs;
  private int lastDocID = -1;
  private final int packedBytesLength;

  PointValuesWriter(Counter bytesUsed, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = bytesUsed;
    this.bytes = new PagedBytes(12);
    bytesOut = bytes.getDataOutput();
    docIDs = new int[16];
    iwBytesUsed.addAndGet(16 * Integer.BYTES);
    packedBytesLength = fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes();
  }

  // TODO: if exactly the same value is added to exactly the same doc, should we dedup?
  public void addPackedValue(int docID, BytesRef value) throws IOException {
    if (value == null) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": point value must not be null");
    }
    if (value.length != packedBytesLength) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": this field's value has length=" + value.length + " but should be " + (fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes()));
    }

    if (docIDs.length == numPoints) {
      docIDs = ArrayUtil.grow(docIDs, numPoints+1);
      iwBytesUsed.addAndGet((docIDs.length - numPoints) * Integer.BYTES);
    }
    final long bytesRamBytesUsedBefore = bytes.ramBytesUsed();
    bytesOut.writeBytes(value.bytes, value.offset, value.length);
    iwBytesUsed.addAndGet(bytes.ramBytesUsed() - bytesRamBytesUsedBefore);
    docIDs[numPoints] = docID;
    if (docID != lastDocID) {
      numDocs++;
      lastDocID = docID;
    }

    numPoints++;
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, PointsWriter writer) throws IOException {
    final PagedBytes.Reader bytesReader = bytes.freeze(false);
    PointValues points = new MutablePointValues() {
      final int[] ords = new int[numPoints];
      int[] temp;
      {
        for (int i = 0; i < numPoints; ++i) {
          ords[i] = i;
        }
      }

      @Override
      public void intersect(IntersectVisitor visitor) throws IOException {
        final BytesRef scratch = new BytesRef();
        final byte[] packedValue = new byte[packedBytesLength];
        for(int i=0;i<numPoints;i++) {
          getValue(i, scratch);
          assert scratch.length == packedValue.length;
          System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedBytesLength);
          visitor.visit(getDocID(i), packedValue);
        }
      }

      @Override
      public long estimatePointCount(IntersectVisitor visitor) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMinPackedValue() {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMaxPackedValue() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumDimensions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumIndexDimensions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getBytesPerDimension() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long size() {
        return numPoints;
      }

      @Override
      public int getDocCount() {
        return numDocs;
      }

      @Override
      public void swap(int i, int j) {
        int tmp = ords[i];
        ords[i] = ords[j];
        ords[j] = tmp;
      }

      @Override
      public int getDocID(int i) {
        return docIDs[ords[i]];
      }

      @Override
      public void getValue(int i, BytesRef packedValue) {
        final long offset = (long) packedBytesLength * ords[i];
        bytesReader.fillSlice(packedValue, offset, packedBytesLength);
      }

      @Override
      public byte getByteAt(int i, int k) {
        final long offset = (long) packedBytesLength * ords[i] + k;
        return bytesReader.getByte(offset);
      }

      @Override
      public void save(int i, int j) {
        if (temp == null) {
          temp = new int[ords.length];
        }
        temp[j] = ords[i];
      }

      @Override
      public void restore(int i, int j) {
        if (temp != null) {
          System.arraycopy(temp, i, ords, i, j - i);
        }
      }
    };

    final PointValues values;
    if (sortMap == null) {
      values = points;
    } else {
      values = new MutableSortingPointValues((MutablePointValues) points, sortMap);
    }
    PointsReader reader = new PointsReader() {
      @Override
      public PointValues getValues(String fieldName) {
        if (fieldName.equals(fieldInfo.name) == false) {
          throw new IllegalArgumentException("fieldName must be the same");
        }
        return values;
      }

      @Override
      public void checkIntegrity() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }

      @Override
      public void close() {
      }
    };
    writer.writeField(fieldInfo, reader);
  }

  static final class MutableSortingPointValues extends MutablePointValues {

    private final MutablePointValues in;
    private final Sorter.DocMap docMap;

    public MutableSortingPointValues(final MutablePointValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      in.intersect(new IntersectVisitor() {
        @Override
        public void visit(int docID) throws IOException {
          visitor.visit(docMap.oldToNew(docID));
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
          visitor.visit(docMap.oldToNew(docID), packedValue);
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          return visitor.compare(minPackedValue, maxPackedValue);
        }
      });
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      return in.estimatePointCount(visitor);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDimensions() throws IOException {
      return in.getNumDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      return in.size();
    }

    @Override
    public int getDocCount() {
      return in.getDocCount();
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      in.getValue(i, packedValue);
    }

    @Override
    public byte getByteAt(int i, int k) {
      return in.getByteAt(i, k);
    }

    @Override
    public int getDocID(int i) {
      return docMap.oldToNew(in.getDocID(i));
    }

    @Override
    public void swap(int i, int j) {
      in.swap(i, j);
    }

    @Override
    public void save(int i, int j) {
      in.save(i, j);
    }

    @Override
    public void restore(int i, int j) {
      in.restore(i, j);
    }
  }
}
