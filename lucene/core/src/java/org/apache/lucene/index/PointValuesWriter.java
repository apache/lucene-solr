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

import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.bkd.BKDWriter;

/** Buffers up pending byte[][] value(s) per doc, then flushes when segment flushes. */
class PointValuesWriter {
  private final FieldInfo fieldInfo;
  private final ByteBlockPool bytes;
  private final Counter iwBytesUsed;
  private int[] docIDs;
  private int numPoints;
  private int numDocs;
  private int lastDocID = -1;
  private final byte[] packedValue;
  private final LiveIndexWriterConfig indexWriterConfig;

  public PointValuesWriter(DocumentsWriterPerThread docWriter, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = docWriter.bytesUsed;
    this.bytes = new ByteBlockPool(docWriter.byteBlockAllocator);
    docIDs = new int[16];
    iwBytesUsed.addAndGet(16 * Integer.BYTES);
    packedValue = new byte[fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes()];
    indexWriterConfig = docWriter.indexWriterConfig;
  }

  // TODO: if exactly the same value is added to exactly the same doc, should we dedup?
  public void addPackedValue(int docID, BytesRef value) {
    if (value == null) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": point value must not be null");
    }
    if (value.length != fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes()) {
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

  public void flush(SegmentWriteState state, PointsWriter writer) throws IOException {

    writer.writeField(fieldInfo,
                      new PointsReader() {
                        @Override
                        public void intersect(String fieldName, IntersectVisitor visitor) throws IOException {
                          if (fieldName.equals(fieldInfo.name) == false) {
                            throw new IllegalArgumentException("fieldName must be the same");
                          }
                          for(int i=0;i<numPoints;i++) {
                            bytes.readBytes(packedValue.length * i, packedValue, 0, packedValue.length);
                            visitor.visit(docIDs[i], packedValue);
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
                          return numPoints;
                        }

                        @Override
                        public int getDocCount(String fieldName) {
                          return numDocs;
                        }
                      },
                      Math.max(indexWriterConfig.getRAMBufferSizeMB()/8.0, BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP));
  }
}
