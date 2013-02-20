package org.apache.lucene.facet.codecs.facet42;

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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/** Writer for {@link Facet42DocValuesFormat}. */
public class Facet42DocValuesConsumer extends DocValuesConsumer {

  final IndexOutput out;
  final int maxDoc;
  final float acceptableOverheadRatio;

  public Facet42DocValuesConsumer(SegmentWriteState state) throws IOException {
    this(state, PackedInts.DEFAULT);
  }
  
  public Facet42DocValuesConsumer(SegmentWriteState state, float acceptableOverheadRatio) throws IOException {  
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    boolean success = false;
    try {
      String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, Facet42DocValuesFormat.EXTENSION);
      out = state.directory.createOutput(fileName, state.context);
      CodecUtil.writeHeader(out, Facet42DocValuesFormat.CODEC, Facet42DocValuesFormat.VERSION_CURRENT);
      maxDoc = state.segmentInfo.getDocCount();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    throw new UnsupportedOperationException("FacetsDocValues can only handle binary fields");
  }

  @Override
  public void addBinaryField(FieldInfo field, final Iterable<BytesRef> values) throws IOException {
    // write the byte[] data
    out.writeVInt(field.number);

    long totBytes = 0;
    for (BytesRef v : values) {
      totBytes += v.length;
    }

    if (totBytes > Integer.MAX_VALUE) {
      throw new IllegalStateException("too many facets in one segment: Facet42DocValues cannot handle more than 2 GB facet data per segment");
    }

    out.writeVInt((int) totBytes);

    for (BytesRef v : values) {
      out.writeBytes(v.bytes, v.offset, v.length);
    }

    PackedInts.Writer w = PackedInts.getWriter(out, maxDoc+1, PackedInts.bitsRequired(totBytes+1), acceptableOverheadRatio);

    int address = 0;
    for(BytesRef v : values) {
      w.add(address);
      address += v.length;
    }
    w.add(address);
    w.finish();
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    throw new UnsupportedOperationException("FacetsDocValues can only handle binary fields");
  }
  
  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
    throw new UnsupportedOperationException("FacetsDocValues can only handle binary fields");
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      out.writeVInt(-1); // write EOF marker
      success = true;
    } finally {
      if (success) {
        IOUtils.close(out);
      } else {
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  
}
