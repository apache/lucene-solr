package org.apache.lucene.index.codecs.mockintblock;

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
import org.apache.lucene.index.codecs.FixedGapTermsIndexReader;
import org.apache.lucene.index.codecs.FixedGapTermsIndexWriter;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.index.codecs.TermsIndexWriterBase;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

/**
 * A silly test codec to verify core support for variable
 * sized int block encoders is working.  The int encoder
 * used here writes baseBlockSize ints at once, if the first
 * int is <= 3, else 2*baseBlockSize.
 */

public class MockVariableIntBlockCodec extends Codec {
  private final int baseBlockSize;

  public MockVariableIntBlockCodec(int baseBlockSize) {
    name = "MockVariableIntBlock";
    this.baseBlockSize = baseBlockSize;
  }

  @Override
  public String toString() {
    return name + "(baseBlockSize="+ baseBlockSize + ")";
  }

  public static class MockIntFactory extends IntStreamFactory {

    private final int baseBlockSize;

    public MockIntFactory(int baseBlockSize) {
      this.baseBlockSize = baseBlockSize;
    }

    @Override
    public IntIndexInput openInput(Directory dir, String fileName, int readBufferSize) throws IOException {
      final IndexInput in = dir.openInput(fileName, readBufferSize);
      final int baseBlockSize = in.readInt();
      return new VariableIntBlockIndexInput(in) {

        @Override
        protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
          return new BlockReader() {
            public void seek(long pos) {}
            public int readBlock() throws IOException {
              buffer[0] = in.readVInt();
              final int count = buffer[0] <= 3 ? baseBlockSize-1 : 2*baseBlockSize-1;
              assert buffer.length >= count: "buffer.length=" + buffer.length + " count=" + count;
              for(int i=0;i<count;i++) {
                buffer[i+1] = in.readVInt();
              }
              return 1+count;
            }
          };
        }
      };
    }

    @Override
    public IntIndexOutput createOutput(Directory dir, String fileName) throws IOException {
      final IndexOutput out = dir.createOutput(fileName);
      out.writeInt(baseBlockSize);
      return new VariableIntBlockIndexOutput(out, 2*baseBlockSize) {

        int pendingCount;
        final int[] buffer = new int[2+2*baseBlockSize];

        @Override
        protected int add(int value) throws IOException {
          assert value >= 0;
          buffer[pendingCount++] = value;
          // silly variable block length int encoder: if
          // first value <= 3, we write N vints at once;
          // else, 2*N
          final int flushAt = buffer[0] <= 3 ? baseBlockSize : 2*baseBlockSize;

          // intentionally be non-causal here:
          if (pendingCount == flushAt+1) {
            for(int i=0;i<flushAt;i++) {
              out.writeVInt(buffer[i]);
            }
            buffer[0] = buffer[flushAt];
            pendingCount = 1;
            return flushAt;
          } else {
            return 0;
          }
        }
      };
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new SepPostingsWriterImpl(state, new MockIntFactory(baseBlockSize));

    boolean success = false;
    TermsIndexWriterBase indexWriter;
    try {
      indexWriter = new FixedGapTermsIndexWriter(state);
      success = true;
    } finally {
      if (!success) {
        postingsWriter.close();
      }
    }

    success = false;
    try {
      FieldsConsumer ret = new BlockTermsWriter(indexWriter, state, postingsWriter);
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
                                                                      new MockIntFactory(baseBlockSize), state.codecId);

    TermsIndexReaderBase indexReader;
    boolean success = false;
    try {
      indexReader = new FixedGapTermsIndexReader(state.dir,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       state.termsIndexDivisor,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator(),
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
  public void files(Directory dir, SegmentInfo segmentInfo, String codecId, Set<String> files) throws IOException {
    SepPostingsReaderImpl.files(segmentInfo, codecId, files);
    BlockTermsReader.files(dir, segmentInfo, codecId, files);
    FixedGapTermsIndexReader.files(dir, segmentInfo, codecId, files);
  }

  @Override
  public void getExtensions(Set<String> extensions) {
    SepPostingsWriterImpl.getExtensions(extensions);
    BlockTermsReader.getExtensions(extensions);
    FixedGapTermsIndexReader.getIndexExtensions(extensions);
  }
}
