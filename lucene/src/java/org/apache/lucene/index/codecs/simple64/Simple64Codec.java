package org.apache.lucene.index.codecs.simple64;

/**
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
import java.util.Set;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.sep.IntStreamFactory;
import org.apache.lucene.index.codecs.sep.IntIndexInput;
import org.apache.lucene.index.codecs.sep.IntIndexOutput;
import org.apache.lucene.index.codecs.sep.SepPostingsReaderImpl;
import org.apache.lucene.index.codecs.sep.SepPostingsWriterImpl;
import org.apache.lucene.index.codecs.intblock.VariableIntBlockIndexInput;
import org.apache.lucene.index.codecs.intblock.VariableIntBlockIndexOutput;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.index.codecs.TermsIndexWriterBase;
import org.apache.lucene.index.codecs.VariableGapTermsIndexReader;
import org.apache.lucene.index.codecs.VariableGapTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;

/**
 * Simple64
 *
 * @lucene.experimental
 */

public class Simple64Codec extends Codec {
  private final int multiplier;

  public Simple64Codec(int multiplier) {
    name = "Simple64";
    this.multiplier = multiplier;
  }

  @Override
  public String toString() {
    return name;
  }

  // only for testing
  public IntStreamFactory getIntFactory() {
    return new Simple64IntFactory();
  }

  private class Simple64IntFactory extends IntStreamFactory {

    @Override
    public IntIndexInput openInput(Directory dir, final String fileName, int readBufferSize) throws IOException {
      return new VariableIntBlockIndexInput(dir.openInput(fileName, readBufferSize)) {

        @Override
        protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
          return new BlockReader() {
            private final int numBytes = multiplier*8;
            private final byte[] bbuf = new byte[numBytes];
            public int readBlock() throws IOException {
              int count = 0;
              int bufferPosition = 0;
              in.readBytes(bbuf, 0, numBytes);
              for(int i=0;i<multiplier;i++) {
                final int i1 = ((bbuf[bufferPosition++] & 0xFF) << 24) | ((bbuf[bufferPosition++] & 0xFF) << 16)
                | ((bbuf[bufferPosition++] & 0xFF) <<  8) |  (bbuf[bufferPosition++] & 0xFF);
                final int i2 = ((bbuf[bufferPosition++] & 0xFF) << 24) | ((bbuf[bufferPosition++] & 0xFF) << 16)
                | ((bbuf[bufferPosition++] & 0xFF) <<  8) |  (bbuf[bufferPosition++] & 0xFF);
                count += Simple64.decompressSingle(i1, i2, buffer, count);
              }
              return count;
            }
          };
        }
      };
    }

    @Override
    public IntIndexOutput createOutput(Directory dir, String fileName) throws IOException {
      return new VariableIntBlockIndexOutput(dir.createOutput(fileName), 61*multiplier) {
        private final long[] buffer = new long[multiplier];
        private int totWritten;
        private int totConsumed;

        private final long[] result = new long[1];
        private final Simple64 compressor = new Simple64();
        
        @Override
        protected int add(int v) throws IOException {
          final int consumed = compressor.add(v, result);
          if (consumed != 0) {
            totConsumed += consumed;
            buffer[totWritten++] = result[0];
            if (totWritten == multiplier) {
              for(int i=0;i<multiplier;i++) {
                out.writeLong(buffer[i]);
              }
              final int ret = totConsumed;
              totConsumed = 0;
              totWritten = 0;
              return ret;
            } else {
              return 0;
            }
          } else {
            return 0;
          }
        }
      };
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new SepPostingsWriterImpl(state, new Simple64IntFactory());

    boolean success = false;
    TermsIndexWriterBase indexWriter;
    try {
      indexWriter = new VariableGapTermsIndexWriter(state, new VariableGapTermsIndexWriter.EveryNTermSelector(state.termIndexInterval));
      success = true;
    } finally {
      if (!success) {
        postingsWriter.close();
      }
    }

    success = false;
    try {
      FieldsConsumer ret = new BlockTermsWriter(indexWriter, state, postingsWriter, BytesRef.getUTF8SortedAsUnicodeComparator());
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postingsWriter.close();
        } finally {
          indexWriter.close();
        }
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new SepPostingsReaderImpl(state.dir,
                                                                      state.segmentInfo,
                                                                      state.readBufferSize,
                                                                      new Simple64IntFactory(), state.codecId);

    TermsIndexReaderBase indexReader;
    boolean success = false;
    try {
      indexReader = new VariableGapTermsIndexReader(state.dir,
                                                    state.fieldInfos,
                                                    state.segmentInfo.name,
                                                    state.termsIndexDivisor,
                                                    state.codecId);
      success = true;
    } finally {
      if (!success) {
        postingsReader.close();
      }
    }

    success = false;
    try {
      FieldsProducer ret = new BlockTermsReader(indexReader,
                                                       state.dir,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       postingsReader,
                                                       state.readBufferSize,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator(),
                                                       StandardCodec.TERMS_CACHE_SIZE,
                                                       state.codecId);
      success = true;
      return ret;
    } finally {
      if (!success) {
        try {
          postingsReader.close();
        } finally {
          indexReader.close();
        }
      }
    }
  }

  @Override
  public void files(Directory dir, SegmentInfo segmentInfo, String codecId, Set<String> files) {
    SepPostingsReaderImpl.files(segmentInfo, codecId, files);
    BlockTermsReader.files(dir, segmentInfo, codecId, files);
    VariableGapTermsIndexReader.files(dir, segmentInfo, codecId, files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    SepPostingsWriterImpl.getExtensions(extensions);
    BlockTermsReader.getExtensions(extensions);
    VariableGapTermsIndexReader.getIndexExtensions(extensions);
  }
}
