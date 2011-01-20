package org.apache.lucene.index.codecs.bulkvint;

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
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;
import org.apache.lucene.index.codecs.PostingsWriterBase;
import org.apache.lucene.index.codecs.PostingsReaderBase;
import org.apache.lucene.index.codecs.TermsIndexReaderBase;
import org.apache.lucene.index.codecs.TermsIndexWriterBase;
import org.apache.lucene.index.codecs.BlockTermsWriter;
import org.apache.lucene.index.codecs.BlockTermsReader;
import org.apache.lucene.index.codecs.VariableGapTermsIndexReader;
import org.apache.lucene.index.codecs.VariableGapTermsIndexWriter;
import org.apache.lucene.index.codecs.standard.StandardCodec;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRef;

/**
 * Silly codec that acts like MockFixedIntBlockCodec mostly (uses vint encoding):
 * writes a single vint header (uncompressed size of the block in bytes)
 * writes the block as a list of vints
 */

public class BulkVIntCodec extends Codec {

  private final int blockSize;

  public BulkVIntCodec(int blockSize) {
    this.blockSize = blockSize;
    name = "BulkVInt";
  }

  @Override
  public String toString() {
    return name + "(blockSize=" + blockSize + ")";
  }

  // only for testing
  public IntStreamFactory getIntFactory() {
    return new BulkVIntFactory();
  }

  private class BulkVIntFactory extends IntStreamFactory {

    @Override
    public IntIndexInput openInput(Directory dir, String fileName, int readBufferSize) throws IOException {
      return new FixedIntBlockIndexInput(dir.openInput(fileName, readBufferSize)) {

        @Override
        protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
          return new BlockReader() {
            final byte bytes[] = new byte[blockSize*5]; // header * max(Vint)
            
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
    public IntIndexOutput createOutput(Directory dir, String fileName) throws IOException {
      return new FixedIntBlockIndexOutput(dir.createOutput(fileName), blockSize) {
        final byte bytes[] = new byte[blockSize*5]; // header * max(Vint)
        
        @Override
        protected void flushBlock() throws IOException {
          int upto = 0;
          
          boolean allOnes = true;
          // encode ints
          for(int i=0;i<buffer.length;i++) {
            int j = buffer[i];
            if (j != 1)
              allOnes = false;
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
    PostingsWriterBase postingsWriter = new SepPostingsWriterImpl(state, new BulkVIntFactory());

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
                                                                      new BulkVIntFactory(), state.codecId);

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
