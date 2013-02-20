package org.apache.lucene.codecs.lucene3x;

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

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Writes and Merges Lucene 3.x norms format
 * @lucene.experimental
 */
class PreFlexRWNormsConsumer extends DocValuesConsumer {
  
  /** norms header placeholder */
  private static final byte[] NORMS_HEADER = new byte[]{'N','R','M',-1};
  
  /** Extension of norms file */
  private static final String NORMS_EXTENSION = "nrm";
  
  /** Extension of separate norms file
   * @deprecated Only for reading existing 3.x indexes */
  @Deprecated
  private static final String SEPARATE_NORMS_EXTENSION = "s";

  private final IndexOutput out;
  private int lastFieldNumber = -1; // only for assert
  
  public PreFlexRWNormsConsumer(Directory directory, String segment, IOContext context) throws IOException {
    final String normsFileName = IndexFileNames.segmentFileName(segment, "", NORMS_EXTENSION);
    boolean success = false;
    IndexOutput output = null;
    try {
      output = directory.createOutput(normsFileName, context);
      output.writeBytes(NORMS_HEADER, 0, NORMS_HEADER.length);
      out = output;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(output);
      }
    }
  }

  @Override
  public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
    assert field.number > lastFieldNumber : "writing norms fields out of order" + lastFieldNumber + " -> " + field.number;
    for (Number n : values) {
      if (n.longValue() < Byte.MIN_VALUE || n.longValue() > Byte.MAX_VALUE) {
        throw new UnsupportedOperationException("3.x cannot index norms that won't fit in a byte, got: " + n.longValue());
      }
      out.writeByte(n.byteValue());
    }
    lastFieldNumber = field.number;
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(out);
  }

  @Override
  public void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException {
    throw new AssertionError();
  }

  @Override
  public void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException {
    throw new AssertionError();
  }

  @Override
  public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException {
    throw new AssertionError();
  }
}
