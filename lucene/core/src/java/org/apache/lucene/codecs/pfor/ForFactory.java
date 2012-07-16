package org.apache.lucene.codecs.pfor;
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
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.apache.lucene.util.IOUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.codecs.sep.IntStreamFactory;
import org.apache.lucene.codecs.sep.IntIndexInput;
import org.apache.lucene.codecs.sep.IntIndexOutput;
import org.apache.lucene.codecs.intblock.FixedIntBlockIndexInput;
import org.apache.lucene.codecs.intblock.FixedIntBlockIndexOutput;

/** 
 * Used to plug to PostingsReader/WriterBase.
 * Encoder and decoder in lower layers are called by 
 * flushBlock() and readBlock()
 */

public final class ForFactory extends IntStreamFactory {

  public ForFactory() {
  }

  @Override
  public IntIndexOutput createOutput(Directory dir, String fileName, IOContext context)  throws IOException {
    boolean success = false;
    IndexOutput out = dir.createOutput(fileName, context);
    try {
      IntIndexOutput ret = new ForIndexOutput(out);
      success = true;
      return ret;
    } finally {
      if (!success) {
        // For some cases (e.g. disk full), the IntIndexOutput may not be 
        // properly created. So we should close those opened files. 
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }

  @Override
  public IntIndexInput openInput(Directory dir, String fileName, IOContext context) throws IOException {
    return new ForIndexInput(dir.openInput(fileName, context));
  }

  /**
   * Here we'll hold both input buffer and output buffer for 
   * encoder/decoder.
   */
  private class ForIndexInput extends FixedIntBlockIndexInput {

    ForIndexInput(final IndexInput in) throws IOException {
      super(in);
    }

    class ForBlockReader implements FixedIntBlockIndexInput.BlockReader {
      private final byte[] encoded;
      private final int[] buffer;
      private final IndexInput in;
      private final IntBuffer encodedBuffer;

      ForBlockReader(final IndexInput in, final int[] buffer) {
        // upperbound for encoded value should include(here header is not buffered):
        // blockSize of normal value when numFrameBits=32(4x bytes); 
        this.encoded = new byte[ForPostingsFormat.DEFAULT_BLOCK_SIZE*4]; 
        this.in = in;
        this.buffer = buffer;
        this.encodedBuffer = ByteBuffer.wrap(encoded).asIntBuffer();
      }

      // TODO: implement public void skipBlock() {} ?
      @Override
      public void readBlock() throws IOException {
        final int header = in.readInt();
        final int numBytes = ForUtil.getEncodedSize(header);
        assert numBytes <= ForPostingsFormat.DEFAULT_BLOCK_SIZE*4;
        in.readBytes(encoded,0,numBytes);
        ForUtil.decompress(encodedBuffer,buffer,header);
      }
    }

    @Override
    protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
      return new ForBlockReader(in,buffer);
    }
  }

  private class ForIndexOutput extends FixedIntBlockIndexOutput {
    private final byte[] encoded;
    private final IntBuffer encodedBuffer;

    ForIndexOutput(IndexOutput out) throws IOException {
      super(out,ForPostingsFormat.DEFAULT_BLOCK_SIZE);
      this.encoded = new byte[ForPostingsFormat.DEFAULT_BLOCK_SIZE*4];
      this.encodedBuffer=ByteBuffer.wrap(encoded).asIntBuffer();
    }

    @Override
    protected void flushBlock() throws IOException {
      final int header = ForUtil.compress(buffer,encodedBuffer);
      final int numBytes = ForUtil.getEncodedSize(header);
      // nocommit writeVInt instead?
      out.writeInt(header);
      out.writeBytes(encoded, numBytes);
    }
  }
}
