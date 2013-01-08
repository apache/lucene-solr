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
import org.apache.lucene.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.codecs.intblock.FixedIntBlockIndexOutput;
import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.codecs.sep.IntIndexOutput;
import org.apache.lucene.codecs.sep.IntStreamFactory;
import org.apache.lucene.codecs.sep.SepPostingsReader;
import org.apache.lucene.codecs.sep.SepPostingsWriter;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * A silly test codec to verify core support for fixed
 * sized int block encoders is working.  The int encoder
 * used here just writes each block as a series of vInt.
 */

public final class MockFixedIntBlockPostingsFormat extends PostingsFormat {

  private final int blockSize;

  public MockFixedIntBlockPostingsFormat() {
    this(1);
  }

  public MockFixedIntBlockPostingsFormat(int blockSize) {
    super("MockFixedIntBlock");
    this.blockSize = blockSize;
  }

  @Override
  public String toString() {
    return getName() + "(blockSize=" + blockSize + ")";
  }

  // only for testing
  public IntStreamFactory getIntFactory() {
    return new MockIntFactory(blockSize);
  }

  /**
   * Encodes blocks as vInts of a fixed block size.
   */
  public static class MockIntFactory extends IntStreamFactory {
    private final int blockSize;

    public MockIntFactory(int blockSize) {
      this.blockSize = blockSize;
    }

    @Override
    public IntIndexInput openInput(Directory dir, String fileName, IOContext context) throws IOException {
      return new FixedIntBlockIndexInput(dir.openInput(fileName, context)) {

        @Override
        protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) {
          return new BlockReader() {
            public void seek(long pos) {}
            @Override
            public void readBlock() throws IOException {
              for(int i=0;i<buffer.length;i++) {
                buffer[i] = in.readVInt();
              }
            }
          };
        }
      };
    }

    @Override
    public IntIndexOutput createOutput(Directory dir, String fileName, IOContext context) throws IOException {
      IndexOutput out = dir.createOutput(fileName, context);
      boolean success = false;
      try {
        FixedIntBlockIndexOutput ret = new FixedIntBlockIndexOutput(out, blockSize) {
          @Override
          protected void flushBlock() throws IOException {
            for(int i=0;i<buffer.length;i++) {
              out.writeVInt(buffer[i]);
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
    PostingsWriterBase postingsWriter = new SepPostingsWriter(state, new MockIntFactory(blockSize));

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
                                                              new MockIntFactory(blockSize), state.segmentSuffix);

    TermsIndexReaderBase indexReader;
    boolean success = false;
    try {
      indexReader = new FixedGapTermsIndexReader(state.directory,
                                                       state.fieldInfos,
                                                       state.segmentInfo.name,
                                                       state.termsIndexDivisor,
                                                       BytesRef.getUTF8SortedAsUnicodeComparator(), state.segmentSuffix,
                                                       IOContext.DEFAULT);
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
