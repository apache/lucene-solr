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
package org.apache.lucene.rangetree;

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

class RangeTreeDocValuesConsumer extends DocValuesConsumer implements Closeable {
  final DocValuesConsumer delegate;
  final int maxPointsInLeafNode;
  final int maxPointsSortInHeap;
  final IndexOutput out;
  final Map<Integer,Long> fieldIndexFPs = new HashMap<>();
  final SegmentWriteState state;

  public RangeTreeDocValuesConsumer(DocValuesConsumer delegate, SegmentWriteState state, int maxPointsInLeafNode, int maxPointsSortInHeap) throws IOException {
    RangeTreeWriter.verifyParams(maxPointsInLeafNode, maxPointsSortInHeap);
    this.delegate = delegate;
    this.maxPointsInLeafNode = maxPointsInLeafNode;
    this.maxPointsSortInHeap = maxPointsSortInHeap;
    this.state = state;
    String datFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, RangeTreeDocValuesFormat.DATA_EXTENSION);
    out = state.directory.createOutput(datFileName, state.context);
    CodecUtil.writeIndexHeader(out, RangeTreeDocValuesFormat.DATA_CODEC_NAME, RangeTreeDocValuesFormat.DATA_VERSION_CURRENT,
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
    
    String metaFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, RangeTreeDocValuesFormat.META_EXTENSION);
    IndexOutput metaOut = state.directory.createOutput(metaFileName, state.context);
    success = false;
    try {
      CodecUtil.writeIndexHeader(metaOut, RangeTreeDocValuesFormat.META_CODEC_NAME, RangeTreeDocValuesFormat.META_VERSION_CURRENT,
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
    RangeTreeWriter writer = new RangeTreeWriter(maxPointsInLeafNode, maxPointsSortInHeap);
    Iterator<Number> valueIt = values.iterator();
    Iterator<Number> valueCountIt = docToValueCount.iterator();
    //System.out.println("\nSNF: field=" + field.name);
    for (int docID=0;docID<state.segmentInfo.maxDoc();docID++) {
      assert valueCountIt.hasNext();
      int count = valueCountIt.next().intValue();
      for(int i=0;i<count;i++) {
        assert valueIt.hasNext();
        writer.add(valueIt.next().longValue(), docID);
      }
    }

    long indexStartFP = writer.finish(out);

    fieldIndexFPs.put(field.number, indexStartFP);
  }

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    throw new UnsupportedOperationException("use either SortedNumericDocValuesField or SortedSetDocValuesField");
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) {
    throw new UnsupportedOperationException("use either SortedNumericDocValuesField or SortedSetDocValuesField");
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) {
    throw new UnsupportedOperationException("use either SortedNumericDocValuesField or SortedSetDocValuesField");
  }

  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
    delegate.addSortedSetField(field, values, docToOrdCount, ords);
    RangeTreeWriter writer = new RangeTreeWriter(maxPointsInLeafNode, maxPointsSortInHeap);
    Iterator<Number> docToOrdCountIt = docToOrdCount.iterator();
    Iterator<Number> ordsIt = ords.iterator();
    //System.out.println("\nSSF: field=" + field.name);
    for (int docID=0;docID<state.segmentInfo.maxDoc();docID++) {
      assert docToOrdCountIt.hasNext();
      int count = docToOrdCountIt.next().intValue();
      for(int i=0;i<count;i++) {
        assert ordsIt.hasNext();
        long ord = ordsIt.next().longValue();
        writer.add(ord, docID);
      }
    }

    long indexStartFP = writer.finish(out);

    fieldIndexFPs.put(field.number, indexStartFP);
  }
}
