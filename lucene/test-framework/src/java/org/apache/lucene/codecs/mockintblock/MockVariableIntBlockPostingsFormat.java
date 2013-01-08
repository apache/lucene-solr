package org.apache.lucene.codecs.mockintblock;

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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blockterms.BlockTermsReader;
import org.apache.lucene.codecs.blockterms.BlockTermsWriter;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexReader;
import org.apache.lucene.codecs.blockterms.FixedGapTermsIndexWriter;
import org.apache.lucene.codecs.blockterms.TermsIndexReaderBase;
import org.apache.lucene.codecs.blockterms.TermsIndexWriterBase;
import org.apache.lucene.codecs.intblock.VariableIntBlockIndexInput;
import org.apache.lucene.codecs.intblock.VariableIntBlockIndexOutput;
import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.codecs.sep.IntIndexOutput;
import org.apache.lucene.codecs.sep.IntStreamFactory;
import org.apache.lucene.codecs.sep.SepPostingsReader;
import org.apache.lucene.codecs.sep.SepPostingsWriter;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * A silly test codec to verify core support for variable
 * sized int block encoders is working.  The int encoder
 * used here writes baseBlockSize ints at once, if the first
 * int is <= 3, else 2*baseBlockSize.
 */

public final class MockVariableIntBlockPostingsFormat extends PostingsFormat {
  private final int baseBlockSize;
  
  public MockVariableIntBlockPostingsFormat() {
    this(1);
  }

  public MockVariableIntBlockPostingsFormat(int baseBlockSize) {
    super("MockVariableIntBlock");
    this.baseBlockSize = baseBlockSize;
  }

  @Override
  public String toString() {
    return getName() + "(baseBlockSize="+ baseBlockSize + ")";
  }

  /**
   * If the first value is <= 3, writes baseBlockSize vInts at once,
   * otherwise writes 2*baseBlockSize vInts.
   */
  public static class MockIntFactory extends IntStreamFactory {

    private final int baseBlockSize;

    public MockIntFactory(int baseBlockSize) {
      this.baseBlockSize = baseBlockSize;
    }

    @Override
    public IntIndexInput openInput(Directory dir, String fileName, IOContext context) throws IOException {
      final IndexInput in = dir.openInput(fileName, context);
      final int baseBlockSize = in.readInt();
      return new VariableIntBlockIndexInput(in) {

        @Override
        protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) {
          return new BlockReader() {
            @Override
            public void seek(long pos) {}
            @Override
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
    public IntIndexOutput createOutput(Directory dir, String fileName, IOContext context) throws IOException {
      final IndexOutput out = dir.createOutput(fileName, context);
      boolean success = false;
      try {
        out.writeInt(baseBlockSize);
        VariableIntBlockIndexOutput ret = new VariableIntBlockIndexOutput(out, 2*baseBlockSize) {
          int pendingCount;
          final int[] buffer = new int[2+2*baseBlockSize];
          
          @Override
          protected int add(int value) throws IOException {
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
        success = true;
        return ret;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(out);
        }
      }
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new SepPostingsWriter(state, new MockIntFactory(baseBlockSize));

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
    PostingsReaderBase postingsReader = new SepPostingsReader(state.directory,
                                                              state.fieldInfos,
                                                              state.segmentInfo,
                                                              state.context,
                                                              new MockIntFactory(baseBlockSize), state.segmentSuffix);

    TermsIndexReaderBase indexReader;
    boolean success = false;
    try {
      indexReader = new FixedGapTermsIndexReader(state.directory,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       state.termsIndexDivisor,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator(),
                                                       state.segmentSuffix, state.context);
      success = true;
    } finally {
      if (!success) {
        postingsReader.close();
      }
    }

    success = false;
    try {
      FieldsProducer ret = new BlockTermsReader(indexReader,
                                                state.directory,
                                                state.fieldInfos,
                                                state.segmentInfo,
                                                postingsReader,
                                                state.context,
                                                1024,
                                                state.segmentSuffix);
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
}
