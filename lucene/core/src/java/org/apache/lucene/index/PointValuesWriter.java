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

import org.apache.lucene.codecs.MutablePointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/** Buffers up pending byte[][] value(s) per doc, then flushes when segment flushes. */
class PointValuesWriter {
  private final FieldInfo fieldInfo;
  private final ByteBlockPool bytes;
  private final Counter iwBytesUsed;
  private int[] docIDs;
  private int numPoints;
  private int numDocs;
  private int lastDocID = -1;
  private final int packedBytesLength;

  public PointValuesWriter(DocumentsWriterPerThread docWriter, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = docWriter.bytesUsed;
    this.bytes = new ByteBlockPool(docWriter.byteBlockAllocator);
    docIDs = new int[16];
    iwBytesUsed.addAndGet(16 * Integer.BYTES);
    packedBytesLength = fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes();
  }

  // TODO: if exactly the same value is added to exactly the same doc, should we dedup?
  public void addPackedValue(int docID, BytesRef value) {
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
    bytes.append(value);
    docIDs[numPoints] = docID;
    if (docID != lastDocID) {
      numDocs++;
      lastDocID = docID;
    }

    numPoints++;
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, PointsWriter writer) throws IOException {
    MutablePointsReader points = new MutablePointsReader() {
      final int[] ords = new int[numPoints];
      {
        for (int i = 0; i < numPoints; ++i) {
          ords[i] = i;
        }
      }

      @Override
      public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
        if (fieldName.equals(fieldInfo.name) == false) {
          throw new IllegalArgumentException("fieldName must be the same");
        }
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

      @Override
      public long estimatePointCount(String fieldName, IntersectVisitor visitor) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMinPackedValue(String fieldName) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMaxPackedValue(String fieldName) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumDimensions(String fieldName) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getBytesPerDimension(String fieldName) {
        throw new UnsupportedOperationException();
      }

      @Override
      public long size(String fieldName) {
        if (fieldName.equals(fieldInfo.name) == false) {
          throw new IllegalArgumentException("fieldName must be the same");
        }
        return numPoints;
      }

      @Override
      public int getDocCount(String fieldName) {
        if (fieldName.equals(fieldInfo.name) == false) {
          throw new IllegalArgumentException("fieldName must be the same");
        }
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
        packedValue.length = packedBytesLength;
        bytes.setRawBytesRef(packedValue, offset);
      }

      @Override
      public byte getByteAt(int i, int k) {
        final long offset = (long) packedBytesLength * ords[i] + k;
        return bytes.readByte(offset);
      }
    };

    final MutablePointsReader values;
    if (sortMap == null) {
      values = points;
    } else {
      values = new MutableSortingPointReader(points, sortMap);
    }

    writer.writeField(fieldInfo, values);
  }

  static final class MutableSortingPointReader extends MutablePointsReader {

    private final MutablePointsReader in;
    private final Sorter.DocMap docMap;

    public MutableSortingPointReader(final MutablePointsReader in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public void intersect(String field, PointValues.IntersectVisitor visitor) throws IOException {
      in.intersect(field, new PointValues.IntersectVisitor() {
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
    public long estimatePointCount(String fieldName, IntersectVisitor visitor) {
      return Long.MAX_VALUE;
    }

    @Override
    public byte[] getMinPackedValue(String field) throws IOException {
      return in.getMinPackedValue(field);
    }

    @Override
    public byte[] getMaxPackedValue(String field) throws IOException {
      return in.getMaxPackedValue(field);
    }

    @Override
    public int getNumDimensions(String field) throws IOException {
      return in.getNumDimensions(field);
    }

    @Override
    public int getBytesPerDimension(String field) throws IOException {
      return in.getBytesPerDimension(field);
    }

    @Override
    public long size(String field) {
      return in.size(field);
    }

    @Override
    public int getDocCount(String field) {
      return in.getDocCount(field);
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
    public void checkIntegrity() throws IOException {
      in.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public long ramBytesUsed() {
      return in.ramBytesUsed();
    }
  }

}
