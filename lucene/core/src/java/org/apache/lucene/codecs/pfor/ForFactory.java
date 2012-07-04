package org.apache.lucene.codecs.pfor;
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
 * Stuff to pass to PostingsReader/WriterBase.
 * Things really make sense are: flushBlock() and readBlock()
 */

public final class ForFactory extends IntStreamFactory {
  private final int blockSize;

  public ForFactory() {
    this.blockSize=ForPostingsFormat.DEFAULT_BLOCK_SIZE;
  }

  @Override
  public IntIndexOutput createOutput(Directory dir, String fileName, IOContext context)  throws IOException {
    IndexOutput out = dir.createOutput(fileName, context);
    boolean success = false;
    try {
      FixedIntBlockIndexOutput ret = new  ForIndexOutput(out, blockSize);
      success = true;
      return ret;
    } finally {
      if (!success) {
        // TODO: why handle exception like this? 
        // and why not use similar codes for read part?
        IOUtils.closeWhileHandlingException(out);
      }
    }
  }
  @Override
  public IntIndexInput openInput(Directory dir, String fileName, IOContext context) throws IOException {
    FixedIntBlockIndexInput ret = new ForIndexInput(dir.openInput(fileName, context));
    return ret;
  }

  // wrap input and output with buffer support
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
        this.encoded = new byte[blockSize*8+4];
        this.in = in;
        this.buffer = buffer;
        this.encodedBuffer = ByteBuffer.wrap(encoded).asIntBuffer();
      }

      // TODO: implement public void skipBlock() {} ?
      @Override
      public void readBlock() throws IOException {
        final int numBytes = in.readInt();
        assert numBytes <= blockSize*8+4;
        in.readBytes(encoded,0,numBytes);
        ForUtil.decompress(encodedBuffer,buffer);
      }
    }

    @Override
    protected BlockReader getBlockReader(final IndexInput in, final int[] buffer) throws IOException {
      return new ForBlockReader(in,buffer);
    }
  }

  private class ForIndexOutput extends FixedIntBlockIndexOutput {
      private byte[] encoded;
      private IntBuffer encodedBuffer;
    ForIndexOutput(IndexOutput out, int blockSize) throws IOException {
      super(out,blockSize);
      this.encoded = new byte[blockSize*8+4];
      this.encodedBuffer=ByteBuffer.wrap(encoded).asIntBuffer();
    }
    @Override
    protected void flushBlock() throws IOException {
      final int numBytes = ForUtil.compress(buffer,buffer.length,encodedBuffer);
      out.writeInt(numBytes);
      out.writeBytes(encoded, numBytes);
    }
  }
}
