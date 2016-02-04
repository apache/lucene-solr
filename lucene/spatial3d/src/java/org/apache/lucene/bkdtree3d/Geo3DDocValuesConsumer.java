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
package org.apache.lucene.bkdtree3d;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.geo3d.PlanetModel;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/* @deprecated Use dimensional values in Lucene 6.0 instead */
@Deprecated
class Geo3DDocValuesConsumer extends DocValuesConsumer implements Closeable {
  final DocValuesConsumer delegate;
  final int maxPointsInLeafNode;
  final int maxPointsSortInHeap;
  final IndexOutput out;
  final Map<Integer,Long> fieldIndexFPs = new HashMap<>();
  final SegmentWriteState state;

  public Geo3DDocValuesConsumer(PlanetModel planetModel, DocValuesConsumer delegate, SegmentWriteState state, int maxPointsInLeafNode, int maxPointsSortInHeap) throws IOException {
    BKD3DTreeWriter.verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.delegate = delegate;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    this.state = state;
    String datFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Geo3DDocValuesFormat.DATA_EXTENSION);
    out = state.directory.createOutput(datFileName, state.context);
    CodecUtil.writeIndexHeader(out, Geo3DDocValuesFormat.DATA_CODEC_NAME, Geo3DDocValuesFormat.DATA_VERSION_CURRENT,
                               state.segmentInfo.getId(), state.segmentSuffix);

    // We write the max for this PlanetModel into the index so we know we are decoding correctly at search time, and so we can also do
    // best-effort check that the search time PlanetModel "matches":
    out.writeLong(Double.doubleToLongBits(planetModel.getMaximumMagnitude()));
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
    
    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Geo3DDocValuesFormat.META_EXTENSION);
    IndexOutput metaOut = state.directory.createOutput(metaFileName, state.context);
    success = false;
    try {
      CodecUtil.writeIndexHeader(metaOut, Geo3DDocValuesFormat.META_CODEC_NAME, Geo3DDocValuesFormat.META_VERSION_CURRENT,
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    delegate.addBinaryField(field, values);
    BKD3DTreeWriter writer = new BKD3DTreeWriter(maxPointsInLeafNode, maxPointsSortInHeap);
    Iterator<BytesRef> valuesIt = values.iterator();
    for (int docID=0;docID<state.segmentInfo.maxDoc();docID++) {
      assert valuesIt.hasNext();
      BytesRef value = valuesIt.next();
      // TODO: we should allow multi-valued here, just appended into the BDV
      // 3 ints packed into byte[]
      if (value != null) {
        assert value.length == 12;
        int x = Geo3DDocValuesFormat.readInt(value.bytes, value.offset);
        int y = Geo3DDocValuesFormat.readInt(value.bytes, value.offset+4);
        int z = Geo3DDocValuesFormat.readInt(value.bytes, value.offset+8);
        writer.add(x, y, z, docID);
      }
    }

    long indexStartFP = writer.finish(out);

    fieldIndexFPs.put(field.number, indexStartFP);
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
