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
package org.apache.lucene.bkdtree;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/* @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
class BKDTreeDocValuesConsumer extends DocValuesConsumer implements Closeable {
  final DocValuesConsumer delegate;
  final int maxPointsInLeafNode;
  final int maxPointsSortInHeap;
  final IndexOutput out;
  final Map<Integer,Long> fieldIndexFPs = new HashMap<>();
  final SegmentWriteState state;

  public BKDTreeDocValuesConsumer(DocValuesConsumer delegate, SegmentWriteState state, int maxPointsInLeafNode, int maxPointsSortInHeap) throws IOException {
    BKDTreeWriter.verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.delegate = delegate;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    this.state = state;
    String datFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BKDTreeDocValuesFormat.DATA_EXTENSION);
    out = state.directory.createOutput(datFileName, state.context);
    CodecUtil.writeIndexHeader(out, BKDTreeDocValuesFormat.DATA_CODEC_NAME, BKDTreeDocValuesFormat.DATA_VERSION_CURRENT,
                               state.segmentInfo.getId(), state.segmentSuffix);
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      CodecUtil.writeFooter(out);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(delegate, out);
      } else {
        IOUtils.closeWhileHandlingException(delegate, out);
      }
    }
    
    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, BKDTreeDocValuesFormat.META_EXTENSION);
    IndexOutput metaOut = state.directory.createOutput(metaFileName, state.context);
    success = false;
    try {
      CodecUtil.writeIndexHeader(metaOut, BKDTreeDocValuesFormat.META_CODEC_NAME, BKDTreeDocValuesFormat.META_VERSION_CURRENT,
                                 state.segmentInfo.getId(), state.segmentSuffix);
      metaOut.writeVInt(fieldIndexFPs.size());
      for(Map.Entry<Integer,Long> ent : fieldIndexFPs.entrySet()) {       
        metaOut.writeVInt(ent.getKey());
        metaOut.writeVLong(ent.getValue());
      }
      CodecUtil.writeFooter(metaOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut);
      } else {
        IOUtils.closeWhileHandlingException(metaOut);
      }
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException {
    delegate.addSortedNumericField(field, docToValueCount, values);
    BKDTreeWriter writer = new BKDTreeWriter(maxPointsInLeafNode, maxPointsSortInHeap);
    Iterator<Number> valueIt = values.iterator();
    Iterator<Number> valueCountIt = docToValueCount.iterator();
    for (int docID=0;docID<state.segmentInfo.maxDoc();docID++) {
      assert valueCountIt.hasNext();
      int count = valueCountIt.next().intValue();
      for(int i=0;i<count;i++) {
        assert valueIt.hasNext();
        long value = valueIt.next().longValue();
        int latEnc = (int) (value >> 32);
        int lonEnc = (int) (value & 0xffffffff);
        writer.add(latEnc, lonEnc, docID);
      }
    }

    long indexStartFP = writer.finish(out);

    fieldIndexFPs.put(field.number, indexStartFP);
  }

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) {
    throw new UnsupportedOperationException();
  }
}
