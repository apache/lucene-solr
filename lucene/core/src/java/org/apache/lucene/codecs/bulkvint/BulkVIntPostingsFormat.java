package org.apache.lucene.codecs.bulkvint;

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
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.codecs.BlockTermsReader;
import org.apache.lucene.codecs.BlockTermsWriter;
import org.apache.lucene.codecs.BlockTreeTermsReader;
import org.apache.lucene.codecs.BlockTreeTermsWriter;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermsIndexReaderBase;
import org.apache.lucene.codecs.TermsIndexWriterBase;
import org.apache.lucene.codecs.VariableGapTermsIndexReader;
import org.apache.lucene.codecs.VariableGapTermsIndexWriter;
import org.apache.lucene.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.codecs.intblock.FixedIntBlockIndexOutput;
import org.apache.lucene.codecs.sep.IntStreamFactory;
import org.apache.lucene.codecs.sep.SepPostingsReader;
import org.apache.lucene.codecs.sep.SepPostingsWriter;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;

/**
 * Silly codec that acts like MockFixedIntBlockCodec mostly (uses vint encoding):
 * writes a single vint header (uncompressed size of the block in bytes)
 * writes the block as a list of vints
 */

public final class BulkVIntPostingsFormat extends PostingsFormat {

  private final int blockSize;
  private final int minBlockSize;
  private final int maxBlockSize;
  public final static int DEFAULT_BLOCK_SIZE = 128;

  public BulkVIntPostingsFormat() {
    this(BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE, BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
  }

  // nocommit: can't expose this until we write blockSize
  // into index somewhere (where/how? private file?  pass through to
  // sep somehow?)
  /*
  public BulkVIntPostingsFormat(int minBlockSize, int maxBlockSize, int blockSize) {
    super("BulkVInt");
    this.blockSize = blockSize;
    this.minBlockSize = minBlockSize;
    this.maxBlockSize = maxBlockSize;
  }
  */

  @Override
  public String toString() {
    return getName() + "(blockSize=" + blockSize + ")";
  }

  private class BulkVIntFactory extends IntStreamFactory {

    @Override
    public FixedIntBlockIndexInput openInput(Directory dir, String fileName, IOContext context) throws IOException {
      return new FixedIntBlockIndexInput(dir.openInput(fileName, context)) {

        @Override
        protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
          return new BlockReader() {
            private final byte bytes[] = new byte[blockSize*5]; // header * max(Vint)
            
            @Override
            public void readBlock() throws IOException {
              final int numBytes = in.readVInt(); // read header
              if (numBytes == 0) { // 1's
                Arrays.fill(buffer, 1);
                return;
              }
              in.readBytes(bytes, 0, numBytes); // readBytes
              
              int upto = 0;
              
              // decode bytes
              for(int i=0;i<buffer.length;i++) {
                byte b = bytes[upto++];
                int j = b & 0x7F;
                for (int shift = 7; (b & 0x80) != 0; shift += 7) {
                  b = bytes[upto++];
                  j |= (b & 0x7F) << shift;
                }
                buffer[i] = j;
              }
            }
          };
        }
      };
    }

    @Override
    public FixedIntBlockIndexOutput createOutput(Directory dir, String fileName, IOContext context) throws IOException {
      return new FixedIntBlockIndexOutput(dir.createOutput(fileName, context), blockSize) {
        private final byte bytes[] = new byte[blockSize*5]; // header * max(Vint)
        
        @Override
        protected void flushBlock() throws IOException {
          int upto = 0;
          
          boolean allOnes = true;
          // encode ints
          for(int i=0;i<buffer.length;i++) {
            int j = buffer[i];
            if (j != 1) {
              allOnes = false;
            }
            while ((j & ~0x7F) != 0) {
              bytes[upto++] = (byte)((j & 0x7f) | 0x80);
              j >>>= 7;
            }
            bytes[upto++] = (byte)j;
          }
          
          if (allOnes) {
            // the most common int pattern (all 1's)
            // write a special header (numBytes=0) for this case.
            out.writeVInt(0);
          } else {
            // write header (length in bytes)
            out.writeVInt(upto);
          
            // write block
            out.writeBytes(bytes, 0, upto);
          }
        }
      };
    }
  }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    // TODO: implement a new PostingsWriterBase to improve skip-settings
    PostingsWriterBase postingsWriter = new SepPostingsWriter(state, new BulkVIntFactory()); 
    boolean success = false;
    try {
      FieldsConsumer ret = new BlockTreeTermsWriter(state, 
                                                    postingsWriter,
                                                    minBlockSize, 
                                                    maxBlockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        postingsWriter.close();
      }
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    PostingsReaderBase postingsReader = new SepPostingsReader(state.dir,
                                                              state.fieldInfos,
                                                              state.segmentInfo,
                                                              state.context,
                                                              new BulkVIntFactory(),
                                                              state.segmentSuffix);

    boolean success = false;
    try {
      FieldsProducer ret = new BlockTreeTermsReader(state.dir,
                                                    state.fieldInfos,
                                                    state.segmentInfo.name,
                                                    postingsReader,
                                                    state.context,
                                                    state.segmentSuffix,
                                                    state.termsIndexDivisor);
      success = true;
      return ret;
    } finally {
      if (!success) {
        postingsReader.close();
      }
    }
  }
}
