package org.apache.lucene.index.codecs.pfordelta2;

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
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.index.codecs.intblock.FixedIntBlockIndexOutput;
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
import org.apache.lucene.util.pfor2.PForDelta;

/**
 * A codec for fixed sized int block encoders. The int encoder
 * used here writes each block as data encoded by PForDelta.
 */

public class PForDeltaFixedIntBlockCodec extends Codec {

  private final int blockSize;

  public PForDeltaFixedIntBlockCodec(int blockSize) {
    this.blockSize = blockSize;
    name = "PatchedFrameOfRef2";
  }

  @Override
  public String toString() {
    return name + "(blockSize=" + blockSize + ")";
  }

  /**
   * Encode a block of integers using PForDelta and 
   * @param block the input block to be compressed
   * @param elementNum the number of elements in the block to be compressed 
   * @return the compressed size in the number of integers of the compressed data
   * @throws Exception
   */
  int[] encodeOneBlockWithPForDelta(final int[] block, int elementNum) // throws Exception
    {
      assert block != null && block.length > 0;
      /*
      if(block == null || block.length == 0)
      {
        throw new Exception("input block is empty");
      }
      */
      
      final int[] compressedBlock = PForDelta.compressOneBlock(block, elementNum);
      assert compressedBlock != null;

      //if(compressedBlock == null) 
      //{
      //throw new Exception("compressed buffer is null");
      //}
      return compressedBlock;
    }
    
    /**
     * Decode a block of compressed data (using PForDelta) into a block of elementNum uncompressed integers
     * @param block the input block to be decompressed
     * @param elementNum the number of elements in the block to be compressed 
     */
    void decodeOneBlockWithPForDelta(final int[] block, int elementNum, final int[] output)
    {
      int[] decompressedBlock = PForDelta.decompressOneBlock(block, elementNum);
      System.arraycopy(decompressedBlock, 0, output, 0, decompressedBlock.length);
    }
    
    
    public IntStreamFactory getIntFactory() {
      return new PForDeltaIntFactory();
    }

    private class PForDeltaIntFactory extends IntStreamFactory {

      @Override
      public IntIndexInput openInput(Directory dir, String fileName, int readBufferSize) throws IOException {
        return new FixedIntBlockIndexInput(dir.openInput(fileName, readBufferSize)) {

          @Override
          protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
            return new BlockReader() {
              // nocommit fixed size:
              private final int[] compressedData = new int[256];
              public void seek(long pos) {}
              public void readBlock() throws IOException {
                if(buffer != null)
                {
                  // retrieve the compressed size in ints
                  final int compressedSizeInInt = in.readInt();
                  // read the compressed data (compressedSizeInInt ints)
                  for(int i=0;i<compressedSizeInInt;i++) {
                    compressedData[i] = in.readInt();
                  }
                  // decompress the block
                  decodeOneBlockWithPForDelta(compressedData, blockSize, buffer);
                }
              }
            };
          }
        };
      }

      @Override
      public IntIndexOutput createOutput(Directory dir, String fileName) throws IOException {
        return new FixedIntBlockIndexOutput(dir.createOutput(fileName), blockSize) {
          @Override
          protected void flushBlock() throws IOException {
            int compressedSizeInInts = 0; 
            // compress the data
            //try {
            final int[] compressedBuffer = encodeOneBlockWithPForDelta(buffer, blockSize);
            //catch(Exception e) {
            //e.printStackTrace();
            //}
            // write out the compressed size in ints 
            out.writeInt(compressedBuffer.length);
            // write out the compressed data
            for(int i=0;i<compressedBuffer.length;i++) {
              out.writeInt(compressedBuffer[i]);
            }
          }
        };
      }
    }

  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    PostingsWriterBase postingsWriter = new SepPostingsWriterImpl(state, new PForDeltaIntFactory());

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
                                                                      new PForDeltaIntFactory(), state.codecId);

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
